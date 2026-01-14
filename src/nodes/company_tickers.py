"""Ingest and transform SEC company tickers.

Fetches the company tickers index from SEC and transforms it into edgar_companies dataset.
"""

import os

# SEC requires email in User-Agent per their fair access policy
if 'HTTP_USER_AGENT' not in os.environ:
    os.environ['HTTP_USER_AGENT'] = os.getenv('SEC_USER_AGENT', 'DataIntegrations/1.0 (admin@dataintegrations.io)')

import pyarrow as pa
from subsets_utils import save_raw_json, load_raw_json, upload_data
from utils import rate_limited_get
from _transforms.company_tickers.test import test

SEC_BASE_URL = "https://www.sec.gov"
COMPANY_TICKERS_URL = f"{SEC_BASE_URL}/files/company_tickers.json"

DATASET_ID = "edgar_companies"

METADATA = {
    "title": "SEC EDGAR Companies",
    "description": "SEC-registered companies with their CIK numbers, tickers, and metadata. Includes SIC codes, state of incorporation, fiscal year end, and exchange listings.",
    "column_descriptions": {
        "cik": "Central Index Key (10-digit, zero-padded)",
        "ticker": "Stock ticker symbol",
        "name": "Company name",
        "sic_code": "Standard Industrial Classification code",
        "sic_description": "SIC industry description",
        "state_of_incorporation": "State/country of incorporation",
        "fiscal_year_end": "Fiscal year end (MMDD format)",
        "entity_type": "Entity type (e.g., operating, shell company)",
        "ein": "Employer Identification Number",
        "exchanges": "Stock exchanges where listed",
    }
}

SCHEMA = pa.schema([
    pa.field("cik", pa.string(), nullable=False),
    pa.field("ticker", pa.string()),
    pa.field("name", pa.string(), nullable=False),
    pa.field("sic_code", pa.string()),
    pa.field("sic_description", pa.string()),
    pa.field("state_of_incorporation", pa.string()),
    pa.field("fiscal_year_end", pa.string()),
    pa.field("entity_type", pa.string()),
    pa.field("ein", pa.string()),
    pa.field("exchanges", pa.list_(pa.string())),
])


def ingest():
    """Fetch the company tickers index from SEC."""
    print("  Fetching company tickers index...")
    response = rate_limited_get(COMPANY_TICKERS_URL, host="www.sec.gov")
    response.raise_for_status()
    data = response.json()

    # Convert from {0: {...}, 1: {...}} to list format
    companies = list(data.values())

    save_raw_json(companies, "company_tickers")
    print(f"  Fetched {len(companies):,} company tickers")


def transform():
    """Transform company data from submissions into a companies dataset."""
    # Load ticker index for the basic info
    tickers = load_raw_json("company_tickers")

    # Build map of CIK to first ticker seen (companies can have multiple tickers)
    ticker_map = {}
    for t in tickers:
        cik = str(t["cik_str"]).zfill(10)
        if cik not in ticker_map:
            ticker_map[cik] = t

    unique_ciks = list(ticker_map.keys())
    print(f"  Processing {len(unique_ciks):,} unique companies ({len(tickers):,} ticker entries)...")

    records = []
    errors = 0
    for cik in unique_ciks:
        try:
            data = load_raw_json(f"submissions/{cik}")
        except FileNotFoundError:
            errors += 1
            continue

        # Skip error entries
        if data.get("error"):
            continue

        # Get basic info from ticker map or submission
        ticker_info = ticker_map.get(cik, {})

        # Extract exchanges as a list
        exchanges = data.get("exchanges", [])
        if not exchanges:
            exchanges = None

        records.append({
            "cik": cik,
            "ticker": ticker_info.get("ticker") or data.get("tickers", [None])[0] if data.get("tickers") else None,
            "name": data.get("name") or ticker_info.get("title", ""),
            "sic_code": data.get("sic"),
            "sic_description": data.get("sicDescription"),
            "state_of_incorporation": data.get("stateOfIncorporation"),
            "fiscal_year_end": data.get("fiscalYearEnd"),
            "entity_type": data.get("entityType"),
            "ein": data.get("ein"),
            "exchanges": exchanges,
        })

    if errors > 0:
        print(f"  Skipped {errors} companies with missing submissions")
    print(f"  Transformed {len(records):,} companies")

    table = pa.Table.from_pylist(records, schema=SCHEMA)

    test(table)

    upload_data(table, DATASET_ID, mode="overwrite")
def run():
    """Ingest and transform company tickers."""
    print("\n--- Company Tickers ---")
    ingest()
    transform()


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
