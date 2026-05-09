#!/usr/bin/env python3
"""
get_FF.py – Async scraper / downloader for Fuckingfast links on fitgirl-repacks.site.
uses Python3's asyncio and aiohttp for concurrent downloads.
Author: ChatGPT (adapted by user)
Date: 2024-06-10

Usage: python get_FF.py URL [-o OUTPUT_FOLDER] [--insecure-ssl] [-c CONCURRENCY]
   Arguments:
    - URL: Target page URL containing FuckingFast links: e.g. https://fitgirl-repacks.site/somepage
   Options:
    - OUTPUT_FOLDER: Local folder to save downloads (default: {DEF_DLOAD_FOLDER})
    - CONCURRENCY: Max concurrent download connections (default: {MAX_CONCURRENT_DOWNLOADS})
    - --insecure-ssl: Disable SSL certificate verification (insecure)

Dependencies:
    pip install aiohttp tqdm beautifulsoup4 requests re
"""

import asyncio
import os
import re
import sys
import argparse
import aiohttp

from pathlib import Path
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from tqdm import tqdm


# ---------- Configuration ----------
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/91.0.4472.124 Safari/537.36"
)

# Max concurrent *download* connections
# **************************************************************************************************
# **************************************************************************************************
MAX_CONCURRENT_DOWNLOADS = 2
DEF_DLOAD_FOLDER = r"W:\FG_Dloads"
INFO_FILE = "fitgirl.nfo"
# **************************************************************************************************
# **************************************************************************************************

# Timeout for any single request (connect + read)
# Increased slightly for stability on slower connections
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=90, connect=30)

# Connection settings
HEADERS = {"User-Agent": USER_AGENT}


# ---------- Helpers ----------
async def fetch_text(session: aiohttp.ClientSession, url: str, retries: int = 3) -> str | None:
    """Return the page body as text or None on failure, with retries."""
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                return await resp.text()
        except Exception as exc:
            if attempt == retries:
                print(f"\033[91mError fetching {url} after {retries} attempts: {exc}\033[0m", file=sys.stderr)
                return None
            # Exponential backoff: 1s, 2s, 4s...
            await asyncio.sleep(1 * (2 ** (attempt - 1)))
    return None



async def get_file_info(session: aiohttp.ClientSession, url: str) -> tuple[int, str | None]:
    """
    Try a HEAD request first; if it fails or returns no Content‑Length,
    fall back to a streamed GET. Returns (size_in_bytes, filename_from_header).
    """
    filename = None
    size = 0
    try:
        async with session.head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True) as resp:
            size = int(resp.headers.get("content-length") or 0)
            if "Content-Disposition" in resp.headers:
                cd = resp.headers["Content-Disposition"]
                # simple regex for filename="xyz"
                fname_match = re.search(r'filename="?([^";]+)"?', cd)
                if fname_match:
                    filename = fname_match.group(1)

        if size == 0:  # maybe the server refused HEAD or didn't send length
            async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True) as resp:
                size = int(resp.headers.get("content-length") or 0)
                if not filename and "Content-Disposition" in resp.headers:
                    cd = resp.headers["Content-Disposition"]
                    fname_match = re.search(r'filename="?([^";]+)"?', cd)
                    if fname_match:
                        filename = fname_match.group(1)
        return size, filename
    except Exception as exc:
        print(f"\033[33mCould not get info for {url}: {exc}\033[0m")
        return 0, None


def select_optional_files(final_links: list[tuple]) -> list[tuple]:
    """Interactively select optional files to download."""
    optional_indices = [
        i
        for i, (url, filename, _) in enumerate(final_links)
        if "optional" in (filename or "").lower()
        or "optional" in os.path.basename(url).lower()
        or "selective" in (filename or "").lower()
        or "selective" in os.path.basename(url).lower()
    ]

    if not optional_indices:
        return final_links

    print("\n\033[92mFound following optional files:\033[0m")
    for display_i, orig_i in enumerate(optional_indices, start=1):
        url, filename, _ = final_links[orig_i]
        display_name = filename or os.path.basename(url)
        print(f"  [{display_i:2d}] {display_name}")

    sel = input(
        "\n\033[92mSelect optional files to download (e.g: '1,2' or '2-4'; 'all' for all; or press Enter to skip all): \033[0m"
    ).strip()

    # Determine which optional indices to include. Default (empty input) => skip all optional files.
    if not sel:
        selected_optional = set()
    elif sel.lower() == "all":
        selected_optional = set(optional_indices)
    else:
        selected_optional = set()
        parts = [p.strip() for p in sel.split(",") if p.strip()]
        for p in parts:
            if "-" in p:
                # Range syntax a-b
                try:
                    a_str, b_str = p.split("-", 1)
                    a = int(a_str)
                    b = int(b_str)
                except Exception:
                    print(f"Ignoring invalid range: {p}")
                    continue
                if a > b:
                    a, b = b, a
                for n in range(a, b + 1):
                    if 1 <= n <= len(optional_indices):
                        selected_optional.add(optional_indices[n - 1])
                    else:
                        print(f"Index out of range in range: {n}")
            else:
                if not p.isdigit():
                    print(f"Ignoring invalid index: {p}")
                    continue
                n = int(p)
                if 1 <= n <= len(optional_indices):
                    selected_optional.add(optional_indices[n - 1])
                else:
                    print(f"Index out of range: {n}")

    # Rebuild final_links
    new_links: list[tuple] = []
    for idx, item in enumerate(final_links):
        if idx in optional_indices:
            if idx in selected_optional:
                new_links.append(item)
        else:
            new_links.append(item)
    return new_links


async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    folder: Path,
    filename: str | None,
    referer: str | None,
    semaphore: asyncio.Semaphore,
    pbar: tqdm,
    expected_size: int,
    retries: int = 3,
    results: list = None
):
    """Download one file, skipping it if already present and correct size. Includes retry logic."""
    async with semaphore:
        if not url:
            if results is not None: results.append(("ignored", "No URL"))
            return

        # Resolve filename
        if not filename:
            parsed = urlparse(url)
            filename = os.path.basename(parsed.path) or "downloaded_file"
        # Sanitize: replace only the most dangerous characters
        filename = re.sub(r'[<>:"/\\|?*]', "_", filename)

        file_path = folder / filename

        # --- Skip logic -----------------------------------------
        if file_path.exists():
            local_size = file_path.stat().st_size
            if expected_size and local_size == expected_size:
                tqdm.write(f"Skipping (already downloaded): {file_path.name}")
                pbar.update(local_size or expected_size)
                if results is not None: results.append(("skipped", filename))
                return True
            else:
                tqdm.write(
                    f"Re-downloading (size mismatch) – {file_path.name}: "
                    f"{local_size} bytes vs {expected_size} bytes"
                )
        # ---------------------------------------------------------

        headers = {"Referer": referer} if referer else {}

        # Retry loop for download
        for attempt in range(1, retries + 1):
            try:
                async with session.get(url, timeout=REQUEST_TIMEOUT, headers=headers) as resp:
                    resp.raise_for_status()
                    
                    with open(file_path, "wb") as f:
                        # Increased chunk size to 64KB for better IO performance
                        async for chunk in resp.content.iter_chunked(65536):
                            f.write(chunk)
                            pbar.update(len(chunk)) 
                if results is not None: results.append(("downloaded", filename))
                return True

            except Exception as exc:
                msg = f"\033[91mError downloading {filename} (Attempt {attempt}/{retries}): {exc}\033[0m"
                tqdm.write(msg)
                if attempt < retries:
                    await asyncio.sleep(2)
                else:
                    if results is not None: results.append(("failed", filename))
                    return False
        return False


# ---------- Scraping logic ----------
async def extract_links(session: aiohttp.ClientSession, main_url: str) -> tuple[list[tuple], BeautifulSoup | None]:
    """
    Return a tuple: (list of tuples (final_url, filename_from_fragment, original_link), soup)
    by following the “Filehoster: FuckingFast” pattern.
    """
    html = await fetch_text(session, main_url)
    if not html:
        return [], None

    soup = BeautifulSoup(html, "html.parser")

    target_a = soup.find("a", string="Filehoster: FuckingFast")
    if not target_a:
        print("\n\033[93mNo 'Filehoster: FuckingFast' found.\033[0m")
        return [], soup

    parent_li = target_a.find_parent("li")
    if not parent_li:
        print("\033[93mAnchor is not inside an <li> element.\033[0m")
        return [], soup

    extracted_links = []
    start_collecting = False
    for a in parent_li.find_all("a", href=True):
        if a == target_a:
            start_collecting = True
            continue
        if start_collecting:
            extracted_links.append(a["href"])

    print(f"\n\033[92mFound {len(extracted_links)} candidate links, scraping URLs....\033[0m")

    final_link_list: list[tuple] = []

    # Process links concurrently to speed up resolution
    async def resolve_link(link):
        parsed_orig = urlparse(link)
        filename_from_fragment = parsed_orig.fragment
        if not filename_from_fragment:
            filename_from_fragment = os.path.basename(parsed_orig.path)

        sub_html = await fetch_text(session, link)
        if not sub_html:
            return None

        # <--  REGEX  to find final URL -----------------------------------
        match = re.search(r"window\.open\(['\"]([^'\"]+)['\"]", sub_html)
        # -----------------------------------------------------------------

        if match:
            final_url = match.group(1)
            return (final_url, filename_from_fragment, link)
        return None

    # Resolve all candidate links in parallel
    results = await asyncio.gather(*[resolve_link(link) for link in extracted_links])
    
    for res in results:
        if res:
            final_link_list.append(res)
            print(f"  -> Found final link: {res[0]}")
        else:
            # We don't print "No window.open" for every failure to keep noise down, 
            # or we could print it if needed.
            pass

    return final_link_list, soup


# ---------- NFO file scraping ----------
def save_nfo(page_soup: BeautifulSoup, download_folder: Path):
    if not page_soup:
        print("\033[33mNo page source available to extract NFO.\033[0m")
        return

    # Find the entry-content div
    entry_content = page_soup.find('div', class_='entry-content')
    
    if not entry_content:
        print("\033[33mWarning: Could not find text for NFO.\033[0m")
        return
    
    # Extract title (first h3 in entry-content)
    title_tag = entry_content.find(['h3'])
    title = title_tag.get_text() if title_tag else "Title not found"
    
    # Check for "Requires Windows 10+" 
    requires_win10plus = page_soup.find('strong', style='color: red')
    if requires_win10plus:
        requires_win10plus = requires_win10plus.get_text()
    else:
        requires_win10plus = ""
        
    # Extract Repack Features section
    repack_features = ""
    features_heading = page_soup.find('h3', string=re.compile('Repack Features', re.IGNORECASE))
    if features_heading:
        # Get all content until the next h3
        current = features_heading.find_next()
        while current and current.name not in ('h3', 'div') :
            if current.name in ['p', 'ul', 'ol']:
                repack_features += current.get_text() + "\n"
            current = current.find_next()
        repack_features = repack_features.strip()
    
    # Extract Game Description section
    game_description = ""
    description_heading = page_soup.find(string=re.compile('Game Description', re.IGNORECASE))
    if description_heading:
        # first simple scan:
        tag = page_soup.find_all('div', class_='su-spoiler-content su-u-clearfix su-u-trim')
        # Get all content until the next h3
        current = description_heading.find_next()
        #print(f"\n\n{current}")
        while current: 
            if current.name == 'div': 
                #and "su-spoiler-content.su-u-clearfix.su-u-trim" in current.get('class', []):
                game_description = current.get_text(strip=False, separator="")
                break
            else:
                print("\n!!! NO 'Game Description' FOUND !!!\n")
                return
            current = current.find_next()
        game_description = game_description.strip()

    # Format output
    output = f"""
{title}

{requires_win10plus}

Repack Features:
-----------------------------
{repack_features}

Game Description:
-----------------------------
    {game_description}
    """
 
    # Save NFO to file
    output_file = download_folder / INFO_FILE
    print(f"Creating NFO file... ", end="")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(output)
        print(f"\033[92mNFO saved to {output_file}.\033[0m (please verify!)")
    except (Exception, FileNotFoundError, PermissionError, IOError) as e:
        print(f"\033[91mError saving NFO: {e}\033[0m")
        
    print(f"\nTitle: \033[33m{title[:60]}...\033[0m")
    print(f"Repack Features extracted: {len(repack_features)} characters")
    print(f"Game Description extracted: {len(game_description)} characters\n\n")
 


# ---------- Main driver ----------
async def main():
    global DEF_DLOAD_FOLDER
   
    # --------- Argument parsing (minimal) ----------
    parser = argparse.ArgumentParser(description="Async scrape & download fuckingfast files from fitgirl-repacks.site")
    parser.add_argument("url", nargs="?", help="Target URL")
    parser.add_argument("-o", "--output", help="Download folder (default: {DEF_DLOAD_FOLDER})")
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=MAX_CONCURRENT_DOWNLOADS,
        help="Maximum concurrent download connections (default: {MAX_CONCURRENT_DOWNLOADS})",
    )
    parser.add_argument(
        "--insecure-ssl",
        action="store_true",
        help="Disable SSL certificate verification (insecure)",
    )
    args = parser.parse_args()

    target_url = args.url
    if not target_url:
        target_url = input("\033[92m\nEnter the URL to fetch: \033[0m").strip()
    if not target_url.startswith(("http://", "https://")):
        target_url = "https://" + target_url

    # --------- Prepare session ----------
    # Respect the --insecure-ssl flag: pass ssl=False only when disabling verification,
    # otherwise leave as None to use the default SSL context.
    ssl_option = False if args.insecure_ssl else None
    conn = aiohttp.TCPConnector(limit=args.concurrency, ssl=ssl_option)
    
    async with aiohttp.ClientSession(headers=HEADERS, connector=conn) as session:

        # ---------- Scrape for final links ----------
        final_links, soup = await extract_links(session, target_url)

        if not final_links:
            print("\n\033[93mNo downloadable links were found. Download manually or use magnet links.\033[0m")
            if soup:
                out_folder = args.output
                if not out_folder:
                    out_folder_input = input(f"\n\033[92mEnter local download folder for NFO (default: {DEF_DLOAD_FOLDER}): \033[0m").strip()
                    print("")
                    out_folder = out_folder_input if out_folder_input else DEF_DLOAD_FOLDER
                out_folder = Path(out_folder.strip('"'))  # strip quotes and convert to Path
                os.makedirs(out_folder, exist_ok=True)
                save_nfo(soup, out_folder)
            return

        # ---------- Handle optional files selection ----------
        final_links = select_optional_files(final_links)

        # ---------- Determine download folder ----------
        out_folder = args.output
        if not out_folder:
            out_folder_input = input(
                f"\n\033[92mEnter local download folder path (default: {DEF_DLOAD_FOLDER}): \033[0m"
            ).strip()
            out_folder = out_folder_input if out_folder_input else DEF_DLOAD_FOLDER
        
        out_folder = Path(out_folder.strip('"'))  # strip quotes and convert to Path
        os.makedirs(out_folder, exist_ok=True)
        print(f"\nDownloading to: {out_folder}\n")
        
        # ---------- Save NFO ----------
        # Now that we have a folder and the soup, save the NFO
        if soup:
             save_nfo(soup, out_folder)

        # ---------- Calculate total size & Resolve Filenames ----------
        print("Calculating total download size and resolving filenames…")
        # get_file_info returns (size, filename_from_header)
        infos = await asyncio.gather(
            *[get_file_info(session, url) for (url, _, _) in final_links]
        )
        
        total_bytes = sum(size for size, _ in infos)
        print(f"Total size: {total_bytes / 1_048_576:.2f} MiB")

        # Update final_links with the better filename if available
        # final_links structure: (url, filename_fragment, referer)
        # We want to use the header filename if present, else fragment, else basename
        
        ready_tasks = []
        for i, (size, header_fname) in enumerate(infos):
            url, fragment_fname, referer = final_links[i]
            
            # Priority: Header > Fragment > URL basename
            best_filename = header_fname or fragment_fname
            
            ready_tasks.append({
                "url": url,
                "filename": best_filename,
                "referer": referer,
                "size": size
            })

        # ---------- Download with progress bar ----------
        semaphore = asyncio.Semaphore(args.concurrency)
        results = [] # List to store (status, filename) tuples

        with tqdm(
            total=total_bytes,
            unit="B",
            unit_scale=True,
            desc="Progress",
            disable=(total_bytes == 0),
            dynamic_ncols=True,
            #bar_format='[{elapsed}<{remaining}] {n_fmt}/{total_fmt} | {l_bar}{bar} {rate_fmt:5}{postfix}', 
            bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} [{rate_fmt:>8}]",
            colour='green', 
            leave=False,
        ) as pbar:
            tasks = [
                download_file(
                    session,
                    t["url"],
                    out_folder,
                    t["filename"],
                    t["referer"],
                    semaphore,
                    pbar,
                    expected_size=t["size"],
                    results=results
                )
                for t in ready_tasks
            ]
            # Run all downloads concurrently
            await asyncio.gather(*tasks)

        print("\n\033[92mAll downloads finished.\n\033[0m")
        
        # ---------- Summary Report ----------
        print("\n" + "="*40)
        print("          DOWNLOAD SUMMARY")
        print("="*40)
        
        downloaded = [f for s, f in results if s == "downloaded"]
        skipped = [f for s, f in results if s == "skipped"]
        failed = [f for s, f in results if s == "failed"]
        
        if downloaded:
            print(f"\n\033[92mDownloaded ({len(downloaded)}):\033[0m")
            for f in downloaded: print(f"\033[92m  +\033[0m {f}\033[0m")
            
        if skipped:
            print(f"\n\033[36mSkipped ({len(skipped)}):\033[0m")
            for f in skipped: print(f"\033[36m  ~\033[0m {f}\033[0m")
            
        if failed:
            print(f"\n\033[91mFailed ({len(failed)}):\033[0m")
            for f in failed: print(f"\033[91m  !\033[0m {f}\033[0m")
            
        print("\n" + "="*45 + "\n")
        print(f"\033[92m   Downloaded: {len(downloaded)}  \033[36mSkipped: {len(skipped)}  \033[91mFailed: {len(failed)}\033[0m")
        print("\n" + "="*45 + "\n")


# ---------- Entry point ----------
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\033[93m[!] Cancelled by user. Exiting…\033[0m")
    except Exception as e:
        print(f"\n\033[91m[!] Unexpected error: {e}\033[0m")