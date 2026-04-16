import pandas as pd
import random
import asyncio
from playwright.async_api import async_playwright

try:
    from playwright_stealth import stealth_async
except ImportError:
    stealth_async = None

NUM_WORKERS = 4


def get_handle_from_ig(ig_url):
    if pd.isna(ig_url) or str(ig_url) in ['nan', 'N/A', '']:
        return None
    handle = str(ig_url).lower().replace('https://', '').replace('http://', '')
    handle = handle.replace('www.instagram.com/', '').replace('instagram.com/', '')
    return handle.split('?')[0].strip('/')


async def check_meta_ads(page, handle, website_url):
    region = "GB" if isinstance(website_url, str) and ".co.uk" in website_url else "US"
    url = (
        f"https://www.facebook.com/ads/library/"
        f"?active_status=active&ad_type=all&country={region}"
        f"&is_targeted_country=false&media_type=all&q={handle}"
        f"&search_type=keyword_unordered"
    )

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        print("   [i] Page loaded. Waiting 3s for hydration...")
        await page.wait_for_timeout(3000)

        if "facebook.com/login" in page.url or "facebook.com/checkpoint" in page.url:
            print("   [!] Redirected to login. Stopping.")
            return "NO"

        search_input = page.get_by_placeholder("Search by keyword or advertiser")

        try:
            await search_input.wait_for(state="visible", timeout=10000)
            await search_input.click()
            print("   [i] Search bar clicked. Waiting for dropdown...")
        except Exception:
            print("   [!] Search bar not visible.")
            return "NO"

        style_selector = "//div[contains(@style, '-webkit-line-clamp: 1')]"
        try:
            await page.wait_for_selector(style_selector, timeout=10000)
        except Exception:
            print("   [!] Dropdown didn't appear.")
            return "NO"

        first = page.locator(style_selector).first
        text_content = (await first.text_content()).strip()
        print(f"   [+] Selecting first result: '{text_content}'")
        await first.click()

        await page.reload()
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(3000)

        print("   [i] Checking for ads...")

        ad_indicator = page.locator("//div[contains(text(), 'See ad details') or contains(text(), 'See Ad Details')]")
        if await ad_indicator.count() > 0:
            return "YES"

        zero_ads = page.locator("//div[contains(text(), 'No ads match')]")
        if await zero_ads.count() > 0:
            return "NO"

        return "NO"

    except Exception as e:
        print(f"   Error: {e}")
        return "NO"


async def process_meta_row(page, idx, row, total_rows):
    ig_url = str(row['Instagram URL']).strip()
    handle = get_handle_from_ig(ig_url)
    if not handle:
        handle = str(row['Website']).replace('https://', '').replace('www.', '').split('.')[0]

    print(f"[{idx+1}/{total_rows}] Processing: {handle}")
    status = await check_meta_ads(page, handle, row['Website'])
    print(f"   -> Result: {status}\n")
    await asyncio.sleep(random.uniform(1, 2))
    return idx, status


async def launch_worker(playwright):
    browser = await playwright.chromium.launch(headless=False)
    context = await browser.new_context(
        viewport={"width": 1280, "height": 720},
        locale="en-US",
        timezone_id="America/New_York"
    )
    page = await context.new_page()
    if stealth_async:
        try:
            await stealth_async(page)
        except Exception:
            pass
    return browser, page


async def run_test_crawl():
    input_file = 'list New.csv'
    output_file = 'MasterList_Score.xlsx'

    try:
        df = pd.read_csv(input_file, encoding='latin-1', dtype=str)
    except Exception:
        print("Input file not found.")
        return

    work = []
    total_rows = len(df)
    for idx, row in df.iterrows():
        ig_url = str(row['Instagram URL']).strip()
        if ig_url in ['N/A', 'nan', ''] or not ig_url:
            continue
        meta_ads = str(row['Meta Sponsored Activity']).strip()
        if meta_ads not in ['N/A', 'nan', 'Verify', '']:
            print(f"[{idx+1}/{total_rows}] Already processed: {row['Website']}")
            continue
        work.append((idx, row))

    if not work:
        print("No rows to process.")
    else:
        print(f"--- {len(work)} rows to process with {NUM_WORKERS} windows ---\n")

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
                        process_meta_row(pages[j], batch[j][0], batch[j][1], total_rows)
                        for j in range(len(batch))
                    ]
                    results = await asyncio.gather(*tasks)

                    for row_idx, status in results:
                        df.at[row_idx, 'Meta Sponsored Activity'] = status

                    while True:
                        try:
                            df.to_csv(input_file, index=False)
                            break
                        except PermissionError:
                            print(f"[!] Close {input_file}! Retrying in 3s...")
                            await asyncio.sleep(3)

                for b in browsers:
                    await b.close()

            except Exception as e:
                print(f"\n[X] BROWSER CRASHED: {e}")
                for b in browsers:
                    try:
                        await b.close()
                    except Exception:
                        pass

    print("\n✅ Level 5.1 Test Complete.")

    print(f"\n--- Converting to Excel ---")
    try:
        df_final = pd.read_csv(input_file, encoding='latin-1', dtype=str)

        no_cols = [
            'Meta Sponsored Activity', 'Has Google Search?', 'Has Google Shopping?',
            'Facebook Social Presence?', 'Instagram Social Presence?', 'TikTok Social Presence?',
        ]
        for col in no_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].replace({'': 'NO', 'nan': 'NO', 'N/A': 'NO', 'Verify': 'NO'})
                df_final[col] = df_final[col].fillna('NO')

        df_final.to_excel(output_file, index=False, engine='openpyxl')
        print(f"✅ Successfully created: {output_file}")
    except Exception as e:
        print(f"[!] Error converting to Excel: {e}")


if __name__ == "__main__":
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    asyncio.run(run_test_crawl())
