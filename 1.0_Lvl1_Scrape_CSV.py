import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

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

def get_brand_name(soup):
    """Extract brand name from title or h1"""
    try:
        title_tag = soup.find('title')
        if title_tag:
            brand_name = title_tag.get_text().strip().split('|')[0].strip()
            if brand_name:
                return brand_name
    except:
        pass
    try:
        h1_tag = soup.find('h1')
        if h1_tag:
            return h1_tag.get_text().strip()
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
    """Detect Shopify, WooCommerce, BigCommerce"""
    html_lower = html_text.lower()
    if 'cdn.shopify.com' in html_lower or 'myshopify' in html_lower:
        return "Shopify"
    if 'woocommerce' in html_lower or '/wp-content/plugins/woocommerce' in html_lower:
        return "WooCommerce"
    if 'bigcommerce' in html_lower or 'cdn.bigcommerce.com' in html_lower:
        return "BigCommerce"
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
        soup = BeautifulSoup(response.content, 'html.parser')
        html_text = response.text

        row_data["Brand Name"] = get_brand_name(soup)
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
    print(f"--- Starting SCRAPE on {total_rows} websites ---\n")

    for idx, row in df_input.iterrows():
        website = row['Website']

        if pd.isna(website) or str(website).strip() == "":
            print(f"[{idx+1}/{total_rows}] Skipping empty row")
            continue

        # Skip if all scrape-target fields already have data
        fb = str(row.get('Facebook Account URL', '')).strip()
        ig = str(row.get('Instagram URL', '')).strip()
        tt = str(row.get('TikTok URL', '')).strip()
        ecom = str(row.get('E-Commerce Platform:', '')).strip()
        if fb not in ['', 'nan'] and ig not in ['', 'nan'] and tt not in ['', 'nan'] and ecom not in ['', 'nan']:
            print(f"[{idx+1}/{total_rows}] Already complete: {website}")
            continue

        print(f"[{idx+1}/{total_rows}] Processing: {website}")
        result = process_website(website)

        if result:
            for key in result:
                if result[key]:
                    df_input.at[idx, key] = result[key]
            df_input.to_csv(input_filename, index=False)

        time.sleep(1)

    print(f"\n--- ALL DONE! ---")
    print(f"Data saved to {input_filename}")

if __name__ == "__main__":
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, 'list New.csv')
    run_scraper(csv_path)