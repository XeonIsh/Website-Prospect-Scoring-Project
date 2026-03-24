import pandas as pd
import re
import random
import asyncio
from playwright.async_api import async_playwright

try:
    from playwright_stealth import stealth_async
except ImportError:
    stealth_async = None

NUM_WORKERS = 4


def compose_transparency_url(website):
    domain = str(website).lower().strip()
    domain = domain.replace('https://', '').replace('http://', '')
    if domain.startswith('www.'):
        domain = domain[4:]
    clean_domain = domain.split('/')[0]
    region = "GB" if clean_domain.endswith(".co.uk") else "US"
    url = f"https://adstransparency.google.com/?region={region}&domain={clean_domain}&preset-date=Last+30+days"
    return url, clean_domain


async def check_ads_status(page, transparency_url):
    try:
        await page.goto(transparency_url, wait_until="networkidle", timeout=60000)

        ad_count_xpath = "//div[contains(@class, 'ads-count') and contains(@class, 'ads-count-searchable')]"
        try:
            await page.wait_for_selector(ad_count_xpath, timeout=12000)
            raw_text = await page.locator(ad_count_xpath).first.text_content()
        except Exception:
            return "NO"

        numbers = re.findall(r'\d+', raw_text)
        if numbers and int(numbers[0]) > 0:
            return "YES"
        return "NO"

    except Exception:
        return "NO"


async def process_transparency_row(page, idx, row, total_rows):
    t_url, domain = compose_transparency_url(row['Website'])
    print(f"[{idx+1}/{total_rows}] {domain} -> Checking Ads...")
    status = await check_ads_status(page, t_url)
    print(f"   -> Result: {status}")
    await asyncio.sleep(random.uniform(1, 2))
    return idx, status


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
            pass
    return browser, page


async def run_full_crawl():
    file_name = 'list New.csv'

    print(f"--- Processing File: {file_name} ---")
    try:
        df = pd.read_csv(file_name, encoding='latin-1', dtype=str)
    except Exception as e:
        print(f"Error loading {file_name}: {e}")
        return

    # Build work list
    work = []
    total_rows = len(df)
    for idx, row in df.iterrows():
        google_shopping = str(row['Has Google Shopping?']).strip()
        if google_shopping not in ['N/A', 'nan', '']:
            print(f"[{idx+1}/{total_rows}] Already processed: {row['Website']}")
            continue
        if str(row['Website']).strip() in ['nan', '']:
            continue
        work.append((idx, row))

    if not work:
        print("No rows to process.")
        return

    print(f"--- {len(work)} rows to process with {NUM_WORKERS} windows ---")

    async with async_playwright() as p:
        browsers = []
        pages = []
        try:
            for i in range(NUM_WORKERS):
                b, pg = await launch_worker(p)
                browsers.append(b)
                pages.append(pg)
                print(f"   Window {i+1} ready.")

            for i in range(0, len(work), NUM_WORKERS):
                batch = work[i:i+NUM_WORKERS]
                tasks = [
                    process_transparency_row(pages[j], batch[j][0], batch[j][1], total_rows)
                    for j in range(len(batch))
                ]
                results = await asyncio.gather(*tasks)

                for row_idx, status in results:
                    df.at[row_idx, 'Has Google Shopping?'] = status
                    df.at[row_idx, 'Has Google Search?'] = status

                # Safe save
                while True:
                    try:
                        df.to_csv(file_name, index=False)
                        break
                    except PermissionError:
                        print(f"[!] Close {file_name}! Retrying in 3s...")
                        await asyncio.sleep(3)

            for b in browsers:
                await b.close()
            print(f"\n✅ Level 4 Complete. Results saved to {file_name}")

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
    asyncio.run(run_full_crawl())