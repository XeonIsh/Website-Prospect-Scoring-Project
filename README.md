# Scoring Pipeline

A 5-step automated pipeline that enriches a CSV of brand/account data with social media presence, follower counts, and ad activity information.

## Pipeline Overview

| Script | Purpose |
|--------|---------|
| **1.0_Lvl1_Scrape_CSV.py** | Pre-cleans stale data, then scrapes each brand's website to extract social media URLs (Facebook, Instagram, TikTok), brand name, and e-commerce platform. Uses requests + BeautifulSoup. |
| **2.0_Lvl2_META_CSV.py** | Searches Google for Instagram follower counts, then derives Facebook & Instagram social presence (YES if >1,000 followers). Uses async Playwright with 2 browser windows. |
| **3.0_Lvl3_TikTok_CSV.py** | Searches Google for TikTok follower counts and derives TikTok social presence (YES if >1,000 followers). Uses async Playwright with 2 browser windows. |
| **4.0_Lvl4_Transparency_CSV.py** | Checks Google Ads Transparency Center to determine if a brand runs Google Search or Shopping ads. Uses async Playwright with 4 browser windows. |
| **5.0_Lvl5_MetaAds_CSV.py** | Checks the Meta Ad Library for sponsored ad activity and exports the final enriched data to `MasterList_Score.xlsx`. Uses async Playwright with 4 browser windows. |

Scripts are designed to be run sequentially (1.0 → 2.0 → 3.0 → 4.0 → 5.0). Each script reads from and writes back to the same CSV, only processing rows that haven't been filled in yet.

## Setup

```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
playwright install chromium
```

## Usage

Place your input data in `list New.csv`, then run each script in order:

```bash
python 1.0_Lvl1_Scrape_CSV.py
python 2.0_Lvl2_META_CSV.py
python 3.0_Lvl3_TikTok_CSV.py
python 4.0_Lvl4_Transparency_CSV.py
python 5.0_Lvl5_MetaAds_CSV.py
```

The final output is saved as `MasterList_Score.xlsx`.

## CSV Columns

`Member ID`, `Account Name`, `Website`, `Brand Name`, `E-Commerce Platform:`, `Has Google Search?`, `Has Google Shopping?`, `Facebook Account URL`, `Instagram URL`, `Meta Sponsored Activity`, `Facebook Social Presence?`, `Instagram Social Presence?`, `TikTok URL`, `TikTok Social Presence?`, `Instagram Followers`, `TikTok Followers`
