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
    - OUTPUT_FOLDER: Local folder to save downloads (default: C:/FG_Dloads)
    - CONCURRENCY: Max concurrent download connections (default: 5)
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
MAX_CONCURRENT_DOWNLOADS = 5
DEF_DLOAD_FOLDER = r"X:\FFast"

# Timeout for any single request (connect + read)
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=60)

# Connection settings
HEADERS = {"User-Agent": USER_AGENT}
PAGE_SOUP = None


# ---------- Helpers ----------
async def fetch_text(session: aiohttp.ClientSession, url: str) -> str | None:
    """Return the page body as text or None on failure."""
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            resp.raise_for_status()
            return await resp.text()
    except Exception as exc:
        print(f"\033[91mError fetching {url}: {exc}\033[0m", file=sys.stderr)
        return None


async def get_file_size(session: aiohttp.ClientSession, url: str) -> int:
    """
    Try a HEAD request first; if it fails or returns no Content‑Length,
    fall back to a streamed GET.  Returns size in bytes (or 0 on failure).
    """
    try:
        async with session.head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True) as resp:
            size = int(resp.headers.get("content-length") or 0)
        if size == 0:  # maybe the server refused HEAD
            async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True) as resp:
                size = int(resp.headers.get("content-length") or 0)
        return size
    except Exception as exc:
        print(f"\033[33mCould not get size for {url}: {exc}\033[0m")
        return 0


async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    folder: Path,
    filename: str | None,
    referer: str | None,
    semaphore: asyncio.Semaphore,
    pbar: tqdm,
    expected_size: int,
):
    """Download one file, skipping it if already present and correct size."""
    async with semaphore:
        if not url:
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
                tqdm.write(f"Skipping (already downloaded): {file_path}")
                pbar.update(local_size or expected_size)
                return True
            else:
                tqdm.write(
                    f"Re-downloading (size mismatch) – "
                    f"{local_size} bytes vs {expected_size} bytes"
                )
        # ---------------------------------------------------------

        headers = {"Referer": referer} if referer else {}

        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT, headers=headers) as resp:
                resp.raise_for_status()
                with open(file_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(8192):
                        f.write(chunk)
                        pbar.update(len(chunk))
            return True

        except Exception as exc:
            tqdm.write(f"\033[91mError downloading {url}: {exc}\033[0m")
            return False


# ---------- Scraping logic ----------
async def extract_links(session: aiohttp.ClientSession, main_url: str) -> list[tuple]:
    global PAGE_SOUP
    """
    Return a list of tuples (final_url, filename_from_fragment, original_link)
    by following the “Filehoster: FuckingFast” pattern.
    """
    html = await fetch_text(session, main_url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    PAGE_SOUP = soup

    target_a = soup.find("a", string="Filehoster: FuckingFast")
    if not target_a:
        print("\033[93mNo anchor with text 'Filehoster: FuckingFast' found.\033[0m")
        return []

    parent_li = target_a.find_parent("li")
    if not parent_li:
        print("\033[93mAnchor is not inside an <li> element.\033[0m")
        return []

    extracted_links = []
    start_collecting = False
    for a in parent_li.find_all("a", href=True):
        if a == target_a:
            start_collecting = True
            continue
        if start_collecting:
            extracted_links.append(a["href"])

    print(f"\n\033[92mFound {len(extracted_links)} candidate links:\033[0m")

    final_link_list: list[tuple] = []

    for link in extracted_links:
        # Resolve filename from fragment or path
        parsed_orig = urlparse(link)
        filename_from_fragment = parsed_orig.fragment
        if not filename_from_fragment:
            filename_from_fragment = os.path.basename(parsed_orig.path)

        sub_html = await fetch_text(session, link)
        if not sub_html:
            continue

        # <--  REGEX  to find final URL -----------------------------------
        match = re.search(r"window\.open\(['\"]([^'\"]+)['\"]", sub_html)
        # -----------------------------------------------------------------

        if match:
            final_url = match.group(1)
            print(f"  -> Found final link: {final_url}")
            final_link_list.append((final_url, filename_from_fragment, link))
        else:
            print("  -> No window.open() found.")

    return final_link_list

# ---------- NFO file scraping ----------
def save_nfo(page_soup, dowload_folder):
    # Find the entry-content div
    entry_content = page_soup.find('div', class_='entry-content')
    
    if not entry_content:
        print("Error: Could not find entry-content div")
        exit(1)
        
    # Extract title (first h3 in entry-content)
    title_tag = entry_content.find(['h3'])
    title = title_tag.get_text(strip=True) if title_tag else "Title not found"
    
    # Extract Repack Features section
    repack_features = ""
    features_heading = entry_content.find('h3', string=re.compile('Repack Features', re.IGNORECASE))
    if features_heading:
        # Get all content until the next h3
        current = features_heading.find_next_sibling()
        while current and current.name != 'h3':
            if current.name in ['p', 'ul', 'ol']:
                repack_features += current.get_text() + "\n"
            current = current.find_next_sibling()
        repack_features = repack_features.strip()
    # Extract Game Description section
    game_description = ""
    description_heading = entry_content.find('div', string=re.compile('Game Description', re.IGNORECASE))
    #print(f"!!!!checking ...{description_heading}")
    if description_heading:
        # Get all content until the next h3
        current = description_heading.find_next_sibling()
        print("!!!!!!!!current")
        while current and current.name not in ['h2', 'h3']:
            if current.name in ['p', 'ul', 'ol', 'li']:
                text = current.get_text()
                if text.strip():
                    game_description += text + "\n"
            # Also check for su-spoiler-content div and extract its content
            elif current.name == 'div' and 'su-spoiler-content' in current.get('class', []):
                spoiler_text = current.get_text()
                if spoiler_text.strip():
                    game_description += spoiler_text + "\n"
            current = current.find_next_sibling()
        
        game_description = game_description.strip()

    # If Game Description section not found, try to find it by text content
    if not game_description:
        content_text = entry_content.get_text()
    
        # Find "Game Description" in the text
        desc_match = re.search(r'Game Description\s*(.+?)(?=Included DLCs|Backwards Compatibility|$)', 
                            content_text, re.DOTALL | re.IGNORECASE)
        if desc_match:
            game_description = desc_match.group(1).strip()
            
    # Extract Included DLCs section
    included_dlcs = ""
    content_text = entry_content.get_text()
    dlc_match = re.search(r'Included DLCs\s*(.+?)(?=Backwards Compatibility|Selective Download|$)', 
                          content_text, re.DOTALL | re.IGNORECASE)
    if dlc_match:
        included_dlcs = dlc_match.group(1).strip()

    # Format output
    output = f"""
{title}


Repack Features:
-----------------------------
    {repack_features}

Game Description:
-----------------------------
    {game_description}
    """
    if included_dlcs:
        output += f"""
Included DLCs:
    {included_dlcs}
    """
    # Save to file
    output_file = os.path.join(dowload_folder, "fitgirl.nfo")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(output)

    print(f"\033[92mNFO Successfully saved to {output_file}.\033[0m")
    print(f"\nTitle: \033[33m{title[:60]}.\033[0m")
    print(f"Repack Features extracted: {len(repack_features)} characters")
    print(f"Game Description extracted: {len(game_description)} characters")
    #print(f"\n\n\n\n\n{output}")


# ---------- Main driver ----------
async def main():
    global DEF_DLOAD_FOLDER
   
    # --------- Argument parsing (minimal) ----------
    parser = argparse.ArgumentParser(description="Async scrape & download fuckingfast files from fitgirl-repacks.site")
    parser.add_argument("url", nargs="?", help="Target URL")
    parser.add_argument("-o", "--output", help="Download folder (default: C:/FG_Dloads)")
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=MAX_CONCURRENT_DOWNLOADS,
        help="Maximum concurrent download connections (default: 5)",
    )
    parser.add_argument(
        "--insecure-ssl",
        action="store_true",
        help="Disable SSL certificate verification (insecure)",
    )
    args = parser.parse_args()

    target_url = args.url
    if not target_url:
        target_url = input("\033[92mEnter the URL to fetch: \033[0m").strip()
    if not target_url.startswith(("http://", "https://")):
        target_url = "https://" + target_url

    # --------- Prepare session ----------
    # Respect the --insecure-ssl flag: pass ssl=False only when disabling verification,
    # otherwise leave as None to use the default SSL context.
    ssl_option = False if args.insecure_ssl else None
    conn = aiohttp.TCPConnector(limit=args.concurrency, ssl=ssl_option)
    async with aiohttp.ClientSession(headers=HEADERS, connector=conn) as session:

        # ---------- Scrape for final links ----------
        final_links = await extract_links(session, target_url)

        if not final_links:
            print("\n\033[93mNo downloadable links were found.\033[0m")
            return

        # ---------- Handle optional files selection ----------
        optional_indices = [
            i
            for i, (url, filename, _) in enumerate(final_links)
            if "optional" in (filename or "").lower()
            or "optional" in os.path.basename(url).lower()
        ]

        if optional_indices:
            print("\n\033[92mFound files with 'optional' in the filename:\033[0m")
            for display_i, orig_i in enumerate(optional_indices, start=1):
                url, filename, _ = final_links[orig_i]
                display_name = filename or os.path.basename(url)
                print(f"  [{display_i}] {display_name} — {url}")

            sel = input(
                "\n\033[92mSelect optional files to download (e.g. 1,2 or 1-2), 'all' for all, or press Enter to skip: \033[0m"
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

            # Rebuild final_links to include only selected optional files (other files always kept)
            new_links: list[tuple] = []
            for idx, item in enumerate(final_links):
                if idx in optional_indices:
                    if idx in selected_optional:
                        new_links.append(item)
                else:
                    new_links.append(item)
            final_links = new_links

        # ---------- Determine download folder ----------
        out_folder = args.output
        if not out_folder:
            out_folder = input(
                f"\n\033[92mEnter local download folder path (default: {DEF_DLOAD_FOLDER}): \033[0m"
            ).strip() or DEF_DLOAD_FOLDER
        out_folder = Path(out_folder.strip('"'))  # strip quotes and convert to Path
        DEF_DLOAD_FOLDER = out_folder

        os.makedirs(out_folder, exist_ok=True)
        print(f"\n\033[92mDownloading to: {out_folder}\033[0m\n")

        # ---------- Calculate total size ----------
        print("Calculating total download size…")
        sizes = await asyncio.gather(
            *[get_file_size(session, url) for (url, _, _) in final_links]
        )
        total_bytes = sum(sizes)
        print(f"Total size: {total_bytes / 1_048_576:.2f} MiB")

        # Map URL -> expected size so we can skip later
        url_to_size = {url: sz for (url, _, _), sz in zip(final_links, sizes)}

        # ---------- Download with progress bar ----------
        semaphore = asyncio.Semaphore(args.concurrency)

        with tqdm(
            total=total_bytes,
            unit="B",
            unit_scale=True,
            desc="Total Progress",
            disable=(total_bytes == 0),
        ) as pbar:
            tasks = [
                download_file(
                    session,
                    url,
                    out_folder,
                    filename,
                    referer,
                    semaphore,
                    pbar,
                    expected_size=url_to_size.get(url, 0),
                )
                for (url, filename, referer) in final_links
            ]
            # Run all downloads concurrently
            await asyncio.gather(*tasks)

        print("\n\033[92mAll files downloaded.\n\033[0m")


# ---------- Entry point ----------
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\033[93m[!] Cancelled by user. Exiting…\033[0m")
    
    save_nfo(PAGE_SOUP, DEF_DLOAD_FOLDER)
   