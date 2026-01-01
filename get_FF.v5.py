import sys
import requests
import re
import os
import concurrent.futures
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from tqdm import tqdm


# Global User-Agent constant
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
HEADERS = {'User-Agent': USER_AGENT}

# Create a shared session for connection pooling
session = requests.Session()
session.headers.update(HEADERS)


def get_html(url):
    """
    Fetches the HTML content from the specified URL.
    """
    try:
        response = session.get(url)
        response.raise_for_status()  # Check for HTTP errors
        
        return response.text
        
    except requests.exceptions.RequestException as e:
        print(f"\033[91mError fetching URL: {e}\033[0m", file=sys.stderr)
        return None



def get_file_size(url, referer=None):
    """
    Fetches the content length of the file at the URL.
    Returns size in bytes or 0 if fails.
    """
    try:
        headers = {'Referer': referer} if referer else {}
        
        # Try HEAD first
        response = session.head(url, headers=headers, allow_redirects=True)
        size = int(response.headers.get('content-length', 0))
        
        # If HEAD fails to get size (e.g. 405 Method Not Allowed, or just missing header), try GET stream
        if size == 0:
             with session.get(url, headers=headers, stream=True) as response:
                size = int(response.headers.get('content-length', 0))
        
        if size == 0:
            # Debug print (thread-safe enough for simple debugging)
             print(f"\033[31m[DEBUG] Could not get size for {url}. Status: {response.status_code}\033[0m")
             
        return size
    except Exception as e:
        print(f"\033[33m[DEBUG] Error getting size for {url}: {e}\033[0m")
        return 0

def download_file(args):
    """
    Downloads a single file from the URL to the specified folder.
    args is a tuple: (url, folder, filename, pbar, referer)
    """
    url, folder, filename, pbar, referer = args
    try:
        if not url:
            return

        # Use the provided filename if available, otherwise fallback
        if not filename:
             # Attempt to get a filename from the URL
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            
            # Fallback if filename is empty
            if not filename:
                filename = "downloaded_file_" + str(hash(url))

        # Sanitize filename to avoid filesystem issues
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        file_path = os.path.join(folder, filename)
        
        # Update headers with referer
        headers = {'Referer': referer} if referer else {}
        
        with session.get(url, stream=True, headers=headers) as r:
            r.raise_for_status()
            # We don't need individual content-length checking here for the bar, 
            # as we rely on the pre-calculated total for the global bar.
            
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)
                    if pbar:
                        pbar.update(len(chunk))
        
        return True

    except Exception as e:
        # User tqdm.write to safely print while bar is active
        tqdm.write(f"Error downloading {url}: {e}")
        return False

if __name__ == "__main__":
    try:
        import argparse
        parser = argparse.ArgumentParser(description="Scrape and download files concurrently.")
        parser.add_argument("url", nargs="?", help="The URL to fetch")
        parser.add_argument("-o", "--output", help="Local download folder path")
        args = parser.parse_args()

        print()
        target_url = args.url
        if not target_url:
            target_url = input("\033[92mEnter the URL to fetch: \033[0m")

        if not target_url.startswith('http'):
            target_url = 'https://' + target_url

        print(f"Fetching HTML from: {target_url}...")
        html_code = get_html(target_url)

        final_link_list = [] # List of tuples (url, filename, referer)

        if html_code:
            # Save to a file for easier inspection
#            output_file = "output.html"
#            try:
#                with open(output_file, "w", encoding="utf-8") as f:
#                    f.write(html_code)
#                print(f"Successfully saved HTML to '{output_file}'")
#
#            except IOError as e:
#                print(f"Error saving to file: {e}")

            # Parse HTML to find specific links
            soup = BeautifulSoup(html_code, 'html.parser')
            
            # logic: find <li> element with <a> tag with name "Filehoster: FuckingFast"
            # and save all the subsequent href links to a variable.
            print("Searching for 'Filehoster: FuckingFast' links...", end="")
            target_a = soup.find('a', string="Filehoster: FuckingFast")
            
            extracted_links = []
            if target_a:
                parent_li = target_a.find_parent('li')
                if parent_li:
                    # Get all anchors in the parent li
                    all_anchors = parent_li.find_all('a', href=True)
                    
                    # Filter for links that appear after the target_a
                    # (Assuming 'subsequent' means following siblings/elements in the same container)
                    start_collecting = False
                    for anchor in all_anchors:
                        if anchor == target_a:
                            start_collecting = True
                            continue
                        if start_collecting:
                            extracted_links.append(anchor['href'])
                            
            print(f"\033[92mFound {len(extracted_links)} links.\033[0m\n")
            for link in extracted_links:
                print(f"Processing: {link}")
                
                # Extract filename from fragment
                parsed_original = urlparse(link)
                filename_from_fragment = parsed_original.fragment
                if not filename_from_fragment:
                    # Try to guess from the original link path if fragment is missing
                    filename_from_fragment = os.path.basename(parsed_original.path)
                
                # Fetch the content of the sub-page
                sub_html = get_html(link)
                if sub_html:
                    # regex match window.open
                    # looking for: window.open("https://fuckingfast.co/...", "_self") or similar
                    match = re.search(r"window\.open\(['\"]([^'\"]+)['\"]", sub_html)
                    if match:
                        final_url = match.group(1)
                        print(f"  -> Found final link: {final_url}")
                        # Store tuple (url, filename, referer)
                        final_link_list.append((final_url, filename_from_fragment, link))
                    else:
                        print("  -> No window.open link found.")

        # Proceed to download if we have links
        if final_link_list:
            print(f"\n\033[32mCollected {len(final_link_list)} files to download.\033[0m")
            
            if args.output:
                download_folder = args.output
            else:
                download_folder = input("Enter the local download folder path (default: W:/DC Dloads): ").strip() or "W:/DC Dloads"

            # Remove quotes if user pasted path with quotes
            if download_folder.startswith('"') and download_folder.endswith('"'):
                download_folder = download_folder[1:-1]

            try:
                os.makedirs(download_folder, exist_ok=True)
                print(f"Downloading to: {download_folder}")
                
                print("Calculating total download size... ", end="")
                total_size_bytes = 0
                
                # NOTE: For very robust cancellation we might strictly use daemon threads even for size calc,
                # but thread pool is cleaner for simple map. We'll rely on global exit.
                # Just adding referer to get_file_size calls.
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    # Create a map of future -> url
                    futures = {
                        executor.submit(get_file_size, url, referer): url 
                        for (url, _, referer) in final_link_list
                    }
                    for future in concurrent.futures.as_completed(futures):
                        total_size_bytes += future.result()
                
                print(f"{total_size_bytes / (1024*1024):.2f} MB")
                print()
                if total_size_bytes == 0:
                    print("\033[33m[WARNING] Total size is 0 bytes. Server might not be reporting Content-Length, or files are empty.\033[0m")                
                
                num_workers = 5
                print("\033[33mStarting " + str(num_workers) + " concurrent downloads...\033[0m")
                
                # Create a single global progress bar
                # If total size is 0, we can't show a percentage, so we pass None to total
                pbar_total = total_size_bytes if total_size_bytes > 0 else None
                
                with tqdm(
                    total=pbar_total, 
                    unit='B', 
                    unit_scale=True, 
                    unit_divisor=1024, 
                    desc="Total Progress"
                ) as pbar:
                    
                    # Create a queue for download tasks
                    import queue
                    import threading
                    import time
                    
                    task_queue = queue.Queue()
                    
                    # Fill queue
                    # args: (url, folder, filename, pbar, referer)
                    for item in final_link_list:
                        # item is (url, filename, referer)
                        task_args = (item[0], download_folder, item[1], pbar, item[2])
                        task_queue.put(task_args)

                    def worker():
                        while True:
                            try:
                                args = task_queue.get_nowait()
                            except queue.Empty:
                                return
                            
                            download_file(args)
                            task_queue.task_done()

                    # Start daemon threads
                    threads = []
                    for _ in range(num_workers):
                        t = threading.Thread(target=worker, daemon=True)
                        t.start()
                        threads.append(t)

                    # Wait for queue to be empty, but keep main thread alive to catch KeyboardInterrupt
                    # queue.join() blocks and may ignore signals on Windows.
                    while any(t.is_alive() for t in threads):
                         # If queue is empty, threads will exit eventually
                        time.sleep(0.5)
                        if task_queue.unfinished_tasks == 0:
                            break
            
                print("\n\033[92mAll downloads completed (or attempted).\033[0m")

            except OSError as e:
                print(f"\033[91mError creating directory or downloading: {e}\033[0m")
        else:
            print("\033[93mNo final links were found to download.\033[0m")
            
    except KeyboardInterrupt:
        print("\n\033[93m[!] Cancelled by user. Exiting immediately...\033[0m")
        # Force kill all daemon threads immediately
        os._exit(0)
