import pandas as pd
import re
import random
import asyncio
from playwright.async_api import async_playwright

try:
    from playwright_stealth import stealth_async
except ImportError:
    stealth_async = None

NUM_WORKERS = 2


def convert_follower_count(raw_text):
    if not raw_text or raw_text in ["Not Found", "N/A"]:
        return "N/A"
    clean = str(raw_text).replace('+', '').replace(',', '').lower().strip()
    multiplier = 1
    if 'k' in clean:
        multiplier = 1000
        clean = clean.replace('k', '')
    elif 'm' in clean:
        multiplier = 1000000
        clean = clean.replace('m', '')
    try:
        val = float(clean)
        return int(val * multiplier)
    except ValueError:
        return raw_text


async def get_followers_logic(page, url):
    try:
        await page.goto(f"https://www.google.com/search?q={url}", wait_until="domcontentloaded")

        if "google.com/sorry" in page.url:
            print(f"\n[!] CAPTCHA DETECTED on a tab. Please solve manually.")
            await page.wait_for_selector("div#search", timeout=600000)

        cite_xpath = "//cite[contains(., 'followers')]"
        try:
            await page.wait_for_selector(cite_xpath, timeout=5000)
            raw_text = await page.locator(cite_xpath).first.text_content()
            match = re.search(r'([\d\.\+kKmM,]+)\s*followers', raw_text, re.IGNORECASE)
            if match:
                return convert_follower_count(match.group(1))
            return "Not Found"
        except Exception:
            return "Not Found"
    except Exception:
        return "N/A"


async def process_ig_row(page, idx, row, total_rows):
    ig_url = str(row['Instagram URL']).strip()
    website = row.get('Website', '')
    print(f"[{idx+1}/{total_rows}] Processing: {website} ({ig_url})")
    result = await get_followers_logic(page, ig_url)
    print(f"   -> Found: {result}")
    await asyncio.sleep(random.uniform(2, 4))
    return idx, result


async def launch_worker(playwright):
    browser = await playwright.chromium.launch(
        headless=False,
        args=["--start-maximized", "--disable-blink-features=AutomationControlled"]
    )
    context = await browser.new_context(viewport={"width": 1280, "height": 720})
    page = await context.new_page()
    if stealth_async:
        try:
            await stealth_async(page)
        except Exception:
            print("Warning: Stealth skipped on a window")
    return browser, page


async def run_safe_batch():
    input_file = 'list New.csv'

    print("--- 1. Loading Data ---")
    try:
        df = pd.read_csv(input_file, encoding='latin-1', dtype=str)
    except Exception as e:
        print(f"CRITICAL: Could not read {input_file}. Error: {e}")
        return

    # Build work list
    work = []
    total_rows = len(df)
    for idx, row in df.iterrows():
        ig_url = str(row['Instagram URL']).strip()
        if ig_url in ['N/A', 'nan', ''] or not ig_url:
            continue
        existing = str(row['Instagram Followers']).strip()
        if existing not in ['N/A', 'nan', 'Not Found', '']:
            print(f"[{idx+1}/{total_rows}] Already has followers: {row['Website']}")
            continue
        work.append((idx, row))

    if not work:
        print("No rows to process.")
        return

    print(f"--- {len(work)} rows to process with {NUM_WORKERS} windows ---")
    print("--- 2. Launching Browsers ---")

    async with async_playwright() as p:
        browsers = []
        pages = []
        try:
            for i in range(NUM_WORKERS):
                b, pg = await launch_worker(p)
                browsers.append(b)
                pages.append(pg)
                print(f"   Window {i+1} ready.")

            print(f"--- 3. {NUM_WORKERS} Browser Windows Launched ---\n")

            for i in range(0, len(work), NUM_WORKERS):
                batch = work[i:i+NUM_WORKERS]
                tasks = [
                    process_ig_row(pages[j], batch[j][0], batch[j][1], total_rows)
                    for j in range(len(batch))
                ]
                results = await asyncio.gather(*tasks)

                for row_idx, result in results:
                    # Compare before converting to string
                    if isinstance(result, int) and result > 1000:
                        df.at[row_idx, 'Facebook Social Presence?'] = 'YES'
                        df.at[row_idx, 'Instagram Social Presence?'] = 'YES'
                    else:
                        df.at[row_idx, 'Facebook Social Presence?'] = 'NO'
                        df.at[row_idx, 'Instagram Social Presence?'] = 'NO'
                    # Store as string
                    df.at[row_idx, 'Instagram Followers'] = str(result)

                # Safe save after each batch
                while True:
                    try:
                        df.to_csv(input_file, index=False)
                        break
                    except PermissionError:
                        print(f"[!] Close {input_file}! Retrying in 3s...")
                        await asyncio.sleep(3)

            for b in browsers:
                await b.close()
            print("\n✅ Level 2 Complete.")

        except Exception as e:
            print(f"\n[X] BROWSER CRASHED: {e}")
            for b in browsers:
                try:
                    await b.close()
                except Exception:
                    pass


if __name__ == "__main__":
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    asyncio.run(run_safe_batch())