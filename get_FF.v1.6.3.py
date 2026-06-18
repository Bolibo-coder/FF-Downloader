#!/usr/bin/env python3
"""
get_FF – Python Async scraper / downloader for Fuckingfast links on fitgirl-repacks.site.
uses Python3's asyncio and aiohttp for concurrent downloads.
Author: ChatGPT, Antigravity 2 (adapted by user)
UPDATE: handle single-file pages (e.g. duck-side-of-the-moon)

Usage: python get_FF.py URL [-o OUTPUT_FOLDER] [--insecure-ssl] [-c CONCURRENCY]
   Arguments:
    - URL: Target page URL containing FuckingFast links: e.g. https://fitgirl-repacks.site/somepage
   Options:
    - OUTPUT_FOLDER: Local folder to save downloads (default: <DEF_DLOAD_FOLDER>)
    - CONCURRENCY: Max concurrent download connections (default: <MAX_CONCURRENT_DOWNLOADS>)
    - --insecure-ssl: Disable SSL certificate verification (insecure)

Dependencies:
    pip install bs4 
	( also needs aiohttp asyncio os re sys argparse pathlib urllib tqdm - should be already installed with python distro)
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
    "Chrome/125.0.0.0 Safari/537.36"
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

# Timeout for large file downloads (no total timeout, but 30s socket timeout)
DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(total=0, connect=30, sock_read=30)

# Connection settings
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1"
}


# ---------- Helpers ----------
class AsyncSpinner:
    """A simple async context manager for showing a terminal spinner."""
    def __init__(self, message: str, done_message: str = ".... Done!"):
        self.message = message
        self.done_message = done_message
        self.chars = [".  ", ".. ", "...", "...."]
        self.idx = 0
        self.stop_event = asyncio.Event()
        self.task = None

    async def _spin(self):
        while not self.stop_event.is_set():
            # Using \r to return to start of line, then write message and dot sequence
            dots = self.chars[self.idx % len(self.chars)]
            # We append \033[0m at the very end to ensure color doesn't bleed
            sys.stdout.write(f"\r{self.message}{dots}\033[0m ")
            sys.stdout.flush()
            self.idx += 1
            await asyncio.sleep(0.3) # Slower for dots

    async def __aenter__(self):
        sys.stdout.write("\033[?25l") # Hide cursor
        sys.stdout.flush()
        self.task = asyncio.create_task(self._spin())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.stop_event.set()
        if self.task:
            await self.task
        # Clear the spinner dots, show cursor, and print completion message
        sys.stdout.write("\033[?25h") # Show cursor
        sys.stdout.write(f"\r{self.message} {self.done_message}\033[0m     \n")
        sys.stdout.flush()


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


async def get_file_info(session: aiohttp.ClientSession, url: str, retries: int = 3) -> tuple[int, str | None]:
    """
    Try a HEAD request first; if it fails or returns no Content‑Length,
    fall back to a streamed GET. Returns (size_in_bytes, filename_from_header).
    Uses retries for stability.
    """
    for attempt in range(1, retries + 1):
        try:
            filename = None
            size = 0
            async with session.head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True) as resp:
                # Some servers might not like HEAD, we check for status but don't strictly require 200 here
                # because we fall back to GET anyway if size is 0.
                if resp.status == 200:
                    size = int(resp.headers.get("content-length") or 0)
                    if "Content-Disposition" in resp.headers:
                        cd = resp.headers["Content-Disposition"]
                        fname_match = re.search(r'filename="?([^";]+)"?', cd)
                        if fname_match:
                            filename = fname_match.group(1)

            if size == 0:  # maybe the server refused HEAD or didn't send length
                async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True) as resp:
                    resp.raise_for_status()
                    size = int(resp.headers.get("content-length") or 0)
                    if not filename and "Content-Disposition" in resp.headers:
                        cd = resp.headers["Content-Disposition"]
                        fname_match = re.search(r'filename="?([^";]+)"?', cd)
                        if fname_match:
                            filename = fname_match.group(1)
            return size, filename
        except Exception as exc:
            if attempt == retries:
                print(f"\033[33mCould not get info for {url} after {retries} attempts: {exc}\033[0m")
                return 0, None
            # Exponential backoff: 1s, 2s, 4s...
            await asyncio.sleep(1 * (2 ** (attempt - 1)))
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
                    print(f"{p} : Ignoring invalid index.")
                    continue
                n = int(p)
                if 1 <= n <= len(optional_indices):
                    selected_optional.add(optional_indices[n - 1])
                else:
                    print(f"{n} : Index out of range.")

    # Rebuild final_links
    new_links: list[tuple] = []
    for idx, item in enumerate(final_links):
        if idx in optional_indices:
            if idx in selected_optional:
                #new_links.append(item)
                new_links = [item] + new_links
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
        # Note: bytes already on disk are pre-accounted via pbar's initial=
        # so we do NOT call pbar.update() for them here.
        if file_path.exists():
            local_size = file_path.stat().st_size
            if expected_size and local_size == expected_size:
                tqdm.write(f"Skipping (already downloaded): {file_path.name}")
                if results is not None: results.append(("skipped", filename))
                return True
            else:
                tqdm.write(
                    f"Re-downloading (size mismatch) – {file_path.name}: "
                    f"{local_size} bytes vs {expected_size} bytes"
                )
        # ---------------------------------------------------------

        headers = {"Referer": referer} if referer else {}

        # Track how many NEW (network) bytes this file has contributed to pbar.
        # Bytes already on disk are handled via pbar's initial= and are NOT
        # added through pbar.update(), so only actual downloads count here.
        net_contributed = 0

        # Retry loop for download
        for attempt in range(1, retries + 1):
            try:
                # Check current file size to see if we can resume a partial download
                local_size = file_path.stat().st_size if file_path.exists() else 0
                if expected_size > 0 and local_size >= expected_size:
                    # File completed (perhaps by a previous attempt in this loop)
                    # The remaining network bytes for this file = expected - local_at_start - net_contributed
                    # But since local >= expected, all bytes are accounted for.
                    if results is not None: results.append(("downloaded", filename))
                    return True

                current_headers = headers.copy()
                if 0 < local_size < expected_size:
                    current_headers["Range"] = f"bytes={local_size}-"

                async with session.get(url, timeout=DOWNLOAD_TIMEOUT, headers=current_headers) as resp:
                    # A 206 status indicates the server supports resuming and is sending partial content
                    is_partial = (resp.status == 206)
                    if not is_partial:
                        # Fallback: server doesn't support Range. We must start from scratch.
                        # Undo the initial= credit for the partial bytes that were on disk,
                        # because we're about to overwrite the file from byte 0.
                        if local_size > 0:
                            pbar.update(-local_size)
                        local_size = 0

                    resp.raise_for_status()

                    # Revert any network bytes from a previous failed attempt
                    # so we don't double-count
                    if net_contributed > 0:
                        pbar.update(-net_contributed)
                        net_contributed = 0

                    mode = "ab" if is_partial else "wb"
                    with open(file_path, mode) as f:
                        # Chunk size of 64KB allows smooth, real-time progress and speed updates
                        async for chunk in resp.content.iter_chunked(64 * 1024):
                            # Offload disk write to thread pool if supported, else just write
                            if sys.version_info >= (3, 9):
                                await asyncio.to_thread(f.write, chunk)
                            else:
                                f.write(chunk)
                            pbar.update(len(chunk))
                            net_contributed += len(chunk)
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
async def extract_links(session: aiohttp.ClientSession, main_url: str, soup: BeautifulSoup = None) -> tuple[list[tuple], BeautifulSoup | None]:
    """
    Return a tuple: (list of tuples (final_url, filename_from_fragment, original_link), soup)
    by following the "Filehoster: FuckingFast" pattern.

    Two layouts exist on fitgirl-repacks.site:

    LAYOUT A – Multi-file repack (most pages):
        <li>
          <a href="https://fuckingfast.co/...">Filehoster: FuckingFast</a>
          <a href="https://fuckingfast.co/...#part1.rar">part 1</a>
          <a href="https://fuckingfast.co/...#part2.rar">part 2</a>
          ...
        </li>
      The label anchor itself is NOT a file – the files are the siblings that follow it.

    LAYOUT B – Single-file repack (e.g. duck-side-of-the-moon):
        <li>
          <a href="https://fuckingfast.co/...#Game_Name.rar">Filehoster: FuckingFast</a>
        </li>
      The label anchor IS the only download link (no sibling file links).

    We detect Layout B when the label anchor's own href points to fuckingfast.co
    and no sibling fuckingfast links follow it inside the same <li>.
    """
    if not soup:
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

    # Collect sibling links that come AFTER the label anchor (Layout A)
    # IMPORTANT: the fitgirl page uses unclosed <li> tags, so BS4 may absorb
    # the next sibling <li> (MultiUpload, JDownloader, etc.) as children of
    # this <li>. We therefore ONLY keep links that point to fuckingfast.co.
    extracted_links = []
    start_collecting = False
    for a in parent_li.find_all("a", href=True):
        if a == target_a:
            start_collecting = True
            continue
        if start_collecting:
            href = a["href"]
            if "fuckingfast.co" in href:
                extracted_links.append(href)

    # ── Layout B detection ──────────────────────────────────────────────────
    # If there are no fuckingfast sibling links AND the label anchor's href
    # itself points to fuckingfast.co, this is a single-file page (Layout B).
    label_href = target_a.get("href", "")
    is_single_file = (not extracted_links) and ("fuckingfast.co" in label_href)

    if is_single_file:
        print("\n\033[96m[Layout B] Single-file FuckingFast page detected – using label anchor as the download link.\033[0m\n")
        extracted_links = [label_href]
    else:
        print("\n")

    if not extracted_links:
        print("\033[93mNo FuckingFast download links found in the <li>.\033[0m")
        return [], soup

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

    # Resolve all candidate links in parallel with a semaphore
    resolve_semaphore = asyncio.Semaphore(5)  # Default resolution concurrency
    
    async def sem_resolve(link):
        async with resolve_semaphore:
            return await resolve_link(link)
    async with AsyncSpinner(f"\033[92mFound {len(extracted_links)} candidate link(s), scraping URLs now"):
        results = await asyncio.gather(*[sem_resolve(link) for link in extracted_links])
    
    for res in results:
        if res:
            final_link_list.append(res)
            # print(f"  -> Found final link: {res[0]}")
        else:
            # We don't print "No window.open" for every failure to keep noise down, 
            # or we could print it if needed.
            pass
            
    return final_link_list, soup

# ---------- NFO text extractor ----------
def extract_text(html_content, start_marker, end_marker):
    """
    Extracts text between two markers in html_content and saves it to a file.
    """
    start_idx = html_content.find(start_marker)
    if start_idx == -1:
        print(f"\n\033[31mError:\033[0m Could not find start marker '{start_marker}' in the page.")
        print("\033[31m       Game Description secion will be empty.\033[0m")
        return

    # Adjust start position to be after the marker
    start_pos = start_idx + len(start_marker)

    # Find the end marker starting from the start position
    end_idx = html_content.find(end_marker, start_pos)
    if end_idx == -1:
        print(f"\n\033[31mError:\033[0m Could not find the closing marker '{end_marker}' after the start marker.")
        return

    # Extract the block of HTML/text between the markers
    extracted_block = html_content[start_pos:end_idx].strip()

    # Parse with BeautifulSoup to extract clean text
    soup = BeautifulSoup(extracted_block, "html.parser")
    
    # Add a newline for each <p> found to improve formatting, ignoring empty ones
    for p in soup.find_all('p'):
        if p.get_text(strip=True):
            p.append('\n')
        else:
            p.decompose()

    # Add a bullet character before each <li> element
    for li in soup.find_all('li'):
        li.insert(0, "\u2022 ")

    # Remove extra newlines for <ul> elements
    for ul in soup.find_all('ul'):
        for content in ul.contents:
            if isinstance(content, str) and content.isspace():
                content.extract()

    # Get the text and collapse excessive newlines (3 or more -> 2)
    clean_text = soup.get_text().strip()
    clean_text = re.sub(r'\n{3,}', '\n\n', clean_text)

    # Return found text
    if not clean_text:
        clean_text = f"??? NO '{start_marker}' TEXT FOUND ???"
    return clean_text

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
    
    # Convert soup to string for marker-based search
    page_html = str(page_soup)

    
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
    repack_features = extract_text(
                                    html_content=page_html, 
                                    start_marker="Repack Features", 
                                    end_marker="<div class=", 
                                )
    
    
    # Extract Game Description section
    game_description = extract_text(
                                    html_content=page_html, 
                                    start_marker="Game Description", 
                                    end_marker="<!-- .entry-content -->", 
                                )
    if not game_description:
        game_description = "No description."
    
    if "Backwards Compatibility" in game_description:
        game_description = game_description.split("Backwards Compatibility")[0].strip()


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
    print("\nCreating NFO file... \n")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(output)
        print(f"\033[92mNFO saved to {output_file}.\033[0m \033[33m(please verify - may be incomplete!)\033[0m")
    except (Exception, FileNotFoundError, PermissionError, IOError) as e:
        print(f"\033[91mError saving NFO: {e}\033[0m")
        
    print(f"\n    Title: \033[35m{title}\033[0m")
    print(f"    Repack Features extracted: {len(repack_features)} characters")
    print(f"    Game Description extracted: {len(game_description)} characters\n\n")
 


# ---------- Main driver ----------
async def main():
    global DEF_DLOAD_FOLDER
   
    # --------- Argument parsing (minimal) ----------implement all
    parser = argparse.ArgumentParser(description="Async scrape & download fuckingfast files from fitgirl-repacks.site")
    parser.add_argument("url", nargs="?", help="Target URL")
    parser.add_argument("-o", "--output", help=f"Download folder (default: {DEF_DLOAD_FOLDER})")
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=MAX_CONCURRENT_DOWNLOADS,
        help=f"Maximum concurrent download connections (default: {MAX_CONCURRENT_DOWNLOADS})",
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
    
    # Increase the TCP connector limit beyond just download concurrency, 
    # so that link resolution (which uses its own semaphore) isn't bottlenecked
    conn_limit = max(10, args.concurrency * 3)
    conn = aiohttp.TCPConnector(limit=conn_limit, ssl=ssl_option)
    
    async with aiohttp.ClientSession(headers=HEADERS, connector=conn) as session:

        # ---------- Initial page fetch ----------
        html = await fetch_text(session, target_url)
        if not html:
            print("\033[91mError fetching target URL.\033[0m")
            return
        soup = BeautifulSoup(html, "html.parser")

        # ---------- Determine download folder & Save NFO first ----------
        out_folder = args.output
        if not out_folder:
            out_folder_input = input(
                f"\n\033[92mEnter local download folder path (default: {DEF_DLOAD_FOLDER}): \033[0m"
            ).strip()
            out_folder = out_folder_input if out_folder_input else DEF_DLOAD_FOLDER
        
        out_folder = Path(out_folder.strip('"'))  # strip quotes and convert to Path
        out_folder.mkdir(parents=True, exist_ok=True)
        
        # Save NFO immediately after folder is determined
        save_nfo(soup, out_folder)

        # ---------- Scrape for final links ----------
        final_links, _ = await extract_links(session, target_url, soup=soup)

        if not final_links:
            print("\n\033[93mNo downloadable links were found. Download manually or use magnet/torrent links.\033[0m")
            return

        # ---------- Handle optional files selection ----------
        final_links = select_optional_files(final_links)
        
        print(f"\nNow downloading to: {out_folder}\n")

        # ---------- Calculate total size & Resolve Filenames ----------
        async with AsyncSpinner("\033[94mCalculating total download size and resolving filenames"):
            # Use a semaphore to limit concurrent resolution tasks
            resolve_semaphore = asyncio.Semaphore(args.concurrency)
            
            async def sem_get_info(url):
                async with resolve_semaphore:
                    return await get_file_info(session, url)

            # get_file_info returns (size, filename_from_header)
            infos = await asyncio.gather(
                *[sem_get_info(url) for (url, _, _) in final_links]
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

        # ---------- Calculate bytes already on disk ----------
        # Pre-account for bytes that are already downloaded (complete or partial)
        # so tqdm's rate/speed calculation only reflects actual network I/O.
        initial_bytes = 0
        for t in ready_tasks:
            fname = t["filename"]
            if not fname:
                parsed = urlparse(t["url"])
                fname = os.path.basename(parsed.path) or "downloaded_file"
            fname = re.sub(r'[<>:"/\\|?*]', "_", fname)
            fpath = out_folder / fname
            if fpath.exists():
                local = fpath.stat().st_size
                # Cap at expected_size so we never exceed the total
                if t["size"] > 0:
                    initial_bytes += min(local, t["size"])
                else:
                    initial_bytes += local

        # ---------- Download with progress bar ----------
        semaphore = asyncio.Semaphore(args.concurrency)
        results = [] # List to store (status, filename) tuples

        with tqdm(
            total=total_bytes if total_bytes > 0 else None,
            initial=initial_bytes,
            unit="B",
            unit_scale=True,
            desc="Progress",
            disable=False,
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