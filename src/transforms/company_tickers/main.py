"""Transform SEC company tickers with metadata from submissions."""

import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "edgar_companies"

METADATA = {
    "id": DATASET_ID,
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


def run():
    """Transform company data from submissions into a companies dataset."""
    # Load ticker index for the basic info
    tickers = load_raw_json("company_tickers")
    ticker_map = {str(t["cik_str"]).zfill(10): t for t in tickers}

    # Iterate over all companies from ticker index (works in both local and cloud mode)
    print(f"  Processing {len(tickers):,} company submissions...")

    records = []
    errors = 0
    for ticker_entry in tickers:
        cik = str(ticker_entry["cik_str"]).zfill(10)

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
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
