# Bangalore Vendor Data Collector

Automatically collects business vendor data from Google Maps for event-related
categories in Bangalore, validates phone numbers, and exports to Excel.

---

## Project Structure

```
bangalore_vendor_project/
├── bangalore_vendor_scraper.py   # Main script
├── requirements.txt              # Python dependencies
├── README.md                     # This file
└── output/                       # Excel file saves here after running
```

---

## Setup (First Time Only)

### 1. Get your free SerpAPI key
- Sign up at https://serpapi.com (no credit card needed)
- Copy your key from https://serpapi.com/manage-api-key

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Add your API key to the script
Open `bangalore_vendor_scraper.py` and replace:
```python
SERPAPI_KEY = "YOUR_SERPAPI_KEY_HERE"
```
with your actual key.

---

## Running the Script

```bash
python bangalore_vendor_scraper.py
```

Output file: `Bangalore_Vendors_Master_List.xlsx` (created in the same folder)

---

## What It Does

- Searches 10 vendor categories within 50km of Bangalore
- Fetches up to 60 results per category (3 pages x 20)
- Validates Indian phone numbers into E.164 format (+91XXXXXXXXXX)
- Filters out permanently closed businesses
- Exports to a professional Excel file with 2 sheets:
  - **All Vendors** — full data with filters
  - **Summary** — totals per category

---

## Customizing Categories

Edit the `VENDOR_CATEGORIES` list in the script:
```python
VENDOR_CATEGORIES = [
    "Event Caterers Bangalore",
    "Tent House Bangalore",
    # Add or remove categories here
]
```

---

## Free Tier Limits

| Plan       | Searches/month | Cost      |
|------------|---------------|-----------|
| Free       | 100           | $0        |
| Hobby      | 5,000         | $50/month |

With 10 categories x 3 pages = **30 searches per run**.
Free tier allows ~3 full runs per month.

---

## Tech Stack

- **SerpAPI** — Google Maps data (free tier)
- **phonenumbers** — Indian number validation
- **pandas** — data structuring
- **openpyxl** — Excel formatting

---

## Built For

Event Lux — Presidency University  
Vendor outreach & database building
