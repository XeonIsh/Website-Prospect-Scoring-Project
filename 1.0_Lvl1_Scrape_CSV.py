import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

def clear_stale_data(df):
    """Pre-clean: wipe stale/invalid values so all scripts re-process them."""
    import numpy as np
    cleared = 0

    # Convert all target columns to string first to avoid dtype issues
    all_target_cols = [
        'Facebook Account URL', 'Instagram URL', 'TikTok URL',
        'E-Commerce Platform:',
        'Instagram Followers', 'TikTok Followers',
        'Facebook Social Presence?', 'Instagram Social Presence?', 'TikTok Social Presence?',
        'Has Google Search?', 'Has Google Shopping?', 'Meta Sponsored Activity',
    ]
    for c in all_target_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).replace('nan', '')

    url_cols = ['Facebook Account URL', 'Instagram URL', 'TikTok URL']
    url_bad = ['verify', 'no', 'other', 'n/a', 'nan', '']

    ecom_cols = ['E-Commerce Platform:']
    ecom_bad = ['verify', 'other', 'n/a', 'nan', '']

    follower_cols = ['Instagram Followers', 'TikTok Followers']
    follower_bad = ['0', '0.0', '-', 'not found', 'n/a', 'nan', '']

    presence_cols = ['Facebook Social Presence?', 'Instagram Social Presence?', 'TikTok Social Presence?']

    ad_cols = ['Has Google Search?', 'Has Google Shopping?', 'Meta Sponsored Activity']
    ad_bad = ['no', 'n/a', 'nan', '']

    for col_list, bad_values in [
        (url_cols, url_bad),
        (ecom_cols, ecom_bad),
        (follower_cols, follower_bad),
        (ad_cols, ad_bad),
    ]:
        for c in col_list:
            if c in df.columns:
                mask = df[c].str.strip().str.lower().isin(bad_values)
                cleared += mask.sum()
                df.loc[mask, c] = ''

    for c in presence_cols:
        if c in df.columns:
            non_empty = df[c].str.strip() != ''
            cleared += non_empty.sum()
            df[c] = ''

    print(f"Pre-clean: Cleared {cleared} stale cells.\n")
    return df

BLOCKED_TITLES = {
    'just a moment', 'attention required', 'access denied',
    '403 forbidden', '403 - forbidden', '404 not found',
    'error', 'cloudflare', 'one moment, please',
}

def get_brand_name(soup):
    """Extract brand name from title or h1, ignoring bot-block pages."""
    try:
        title_tag = soup.find('title')
        if title_tag:
            brand_name = title_tag.get_text().strip().split('|')[0].strip()
            if brand_name and brand_name.lower() not in BLOCKED_TITLES:
                return brand_name
    except:
        pass
    try:
        h1_tag = soup.find('h1')
        if h1_tag:
            h1_text = h1_tag.get_text().strip()
            if h1_text.lower() not in BLOCKED_TITLES:
                return h1_text
    except:
        pass
    return "Unknown"

def find_social_urls(soup):
    """Extract Facebook, Instagram and TikTok URLs from page"""
    facebook_url = ""
    instagram_url = ""
    tiktok_url = ""
    try:
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            original_href = link['href']
            if 'facebook.com' in href and not facebook_url:
                facebook_url = original_href if original_href.startswith('http') else 'https://' + original_href.lstrip('/')
            if 'instagram.com' in href and not instagram_url:
                instagram_url = original_href if original_href.startswith('http') else 'https://' + original_href.lstrip('/')
            if 'tiktok.com' in href and not tiktok_url:
                tiktok_url = original_href if original_href.startswith('http') else 'https://' + original_href.lstrip('/')
    except:
        pass
    return facebook_url, instagram_url, tiktok_url

def detect_ecommerce(soup, html_text):
    """Detect e-commerce platform from HTML fingerprints."""
    html_lower = html_text.lower()

    shopify_signals = [
        'cdn.shopify.com',
        'myshopify',
        'shopify.com/s/files/',
        'window.shopify',
        'shopifyanalytics',
        'shopify-section',
    ]
    if any(s in html_lower for s in shopify_signals):
        return "Shopify"

    woo_signals = [
        'woocommerce',
        '/wp-content/plugins/woo',
        'wc_add_to_cart_params',
        '/wp-json/wc/',
        'wc-cart',
        'wc-checkout',
    ]
    if any(s in html_lower for s in woo_signals):
        return "WooCommerce"

    bigcommerce_signals = [
        'bigcommerce',
        'cdn.bigcommerce.com',
        'cdn11.bigcommerce.com',
        'bcdata',
        'stencil-utils',
        '/api/storefront/',
        'bigcommerce-checkout',
    ]
    if any(s in html_lower for s in bigcommerce_signals):
        return "BigCommerce"

    if 'prestashop' in html_lower:
        return "PrestaShop"

    volusion_signals = [
        'volusion',
        'a.volusion.com',
        '/v/vspfiles/',
        'vspfiles',
    ]
    if any(s in html_lower for s in volusion_signals):
        return "Volusion"

    if 'xcart' in html_lower or 'x-cart' in html_lower:
        return "XCART"

    magento_signals = ['magento', 'mage.cookies', 'varien/']
    if any(s in html_lower for s in magento_signals):
        return "Magento"

    others_signals = [
        'opencart',
        'oscommerce',
        'zencart',
        'squarespace-cdn.com',
    ]
    if any(s in html_lower for s in others_signals):
        return "OTHERS"

    return "NO"

def process_website(website):
    if pd.isna(website) or str(website).strip() == "":
        return None

    clean_url = str(website).strip()
    if not clean_url.startswith('http'):
        clean_url = 'https://' + clean_url

    row_data = {
        "Brand Name": "Unknown",
        "Facebook Account URL": "",
        "Instagram URL": "",
        "TikTok URL": "",
        "E-Commerce Platform:": "NO",
    }

    try:
        response = requests.get(clean_url, timeout=10, headers=HEADERS)
        encoding = response.encoding or 'utf-8'
        soup = BeautifulSoup(response.content, 'html.parser', from_encoding=encoding)
        html_text = response.text

        brand_name = get_brand_name(soup)
        try:
            brand_name = brand_name.encode('latin-1').decode('utf-8')
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
        row_data["Brand Name"] = brand_name
        fb, ig, tt = find_social_urls(soup)
        row_data["Facebook Account URL"] = fb
        row_data["Instagram URL"] = ig
        row_data["TikTok URL"] = tt
        row_data["E-Commerce Platform:"] = detect_ecommerce(soup, html_text)

        print(f"  -> Brand: {row_data['Brand Name']}")
    except Exception as e:
        print(f"  -> Error: {e}")

    return row_data

def run_scraper(input_filename):
    print("Loading data...")
    df_input = pd.read_csv(input_filename, encoding='latin-1', dtype=str)

    # === PRE-CLEAN: Wipe stale data so all scripts re-process them ===
    df_input = clear_stale_data(df_input)
    df_input.to_csv(input_filename, index=False)

    total_rows = len(df_input)
    print(f"--- Starting SCRAPE on {total_rows} websites (3 workers) ---\n")

    save_lock = threading.Lock()
    completed = [0]

    def process_row(idx, row):
        website = str(row.get('Website', '')).strip()

        if not website or website == 'nan':
            print(f"[{idx+1}/{total_rows}] Skipping empty row")
            return

        if 'amazon.' in website.lower():
            print(f"[{idx+1}/{total_rows}] Skipping Amazon: {website}")
            return

        fb = str(row.get('Facebook Account URL', '')).strip()
        ig = str(row.get('Instagram URL', '')).strip()
        tt = str(row.get('TikTok URL', '')).strip()
        ecom = str(row.get('E-Commerce Platform:', '')).strip()
        if fb not in ['', 'nan'] and ig not in ['', 'nan'] and tt not in ['', 'nan'] and ecom not in ['', 'nan']:
            print(f"[{idx+1}/{total_rows}] Already complete: {website}")
            return

        print(f"[{idx+1}/{total_rows}] Processing: {website}")
        result = process_website(website)

        if result:
            with save_lock:
                for key in result:
                    if not result[key]:
                        continue
                    if key == 'Brand Name':
                        existing = str(df_input.at[idx, 'Brand Name']).strip()
                        if existing and existing.lower() not in ('', 'nan', 'unknown'):
                            continue
                    df_input.at[idx, key] = result[key]
                completed[0] += 1
                if completed[0] % 5 == 0:
                    df_input.to_csv(input_filename, index=False)

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(process_row, idx, row): idx for idx, row in df_input.iterrows()}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"  -> Worker error: {e}")

    df_input.to_csv(input_filename, index=False)
    print(f"\n--- ALL DONE! ---")
    print(f"Data saved to {input_filename}")

if __name__ == "__main__":
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, 'list New.csv')
    run_scraper(csv_path)