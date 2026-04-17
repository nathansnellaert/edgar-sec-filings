"""Download and transform SEC EDGAR filings.

Fetches company submissions (metadata + filings) from SEC and produces:
- edgar_filings: all filing records with form types, dates, accession numbers
- edgar_companies: company master data enriched from submission metadata
"""

import pyarrow as pa
from tqdm import tqdm
from subsets_utils import (
    save_raw_json, load_raw_json,
    load_state, save_state,
    merge, publish, validate,
)
from subsets_utils.testing import assert_valid_date, assert_length
from connector_utils import rate_limited_get

SEC_BASE_URL = "https://data.sec.gov"

# -- Filings dataset ----------------------------------------------------------

FILINGS_DATASET_ID = "edgar_filings"

FILINGS_METADATA = {
    "id": FILINGS_DATASET_ID,
    "title": "SEC EDGAR Filings",
    "description": "SEC EDGAR filings including 10-K, 10-Q, 8-K, and other forms. Contains filing dates, form types, and accession numbers for all SEC-registered companies.",
    "license": "Public Domain (SEC EDGAR data is public and freely available)",
    "column_descriptions": {
        "cik": "Central Index Key (10-digit)",
        "company_name": "Company name at time of filing",
        "form_type": "SEC form type (10-K, 10-Q, 8-K, etc.)",
        "filing_date": "Date filed with SEC (YYYY-MM-DD)",
        "accession_number": "SEC accession number (unique filing identifier)",
        "file_number": "SEC file number",
        "report_date": "Report period end date",
        "is_xbrl": "Whether filing includes XBRL data",
        "is_inline_xbrl": "Whether filing uses inline XBRL",
        "primary_document": "Primary document filename",
    }
}

FILINGS_SCHEMA = pa.schema([
    pa.field("cik", pa.string(), nullable=False),
    pa.field("company_name", pa.string(), nullable=False),
    pa.field("form_type", pa.string(), nullable=False),
    pa.field("filing_date", pa.string(), nullable=False),
    pa.field("accession_number", pa.string(), nullable=False),
    pa.field("file_number", pa.string()),
    pa.field("report_date", pa.string()),
    pa.field("is_xbrl", pa.bool_(), nullable=False),
    pa.field("is_inline_xbrl", pa.bool_(), nullable=False),
    pa.field("primary_document", pa.string()),
])

# -- Companies dataset ---------------------------------------------------------

COMPANIES_DATASET_ID = "edgar_companies"

COMPANIES_METADATA = {
    "id": COMPANIES_DATASET_ID,
    "title": "SEC EDGAR Companies",
    "description": "SEC-registered companies with their CIK numbers, tickers, and metadata. Includes SIC codes, state of incorporation, fiscal year end, and exchange listings.",
    "license": "Public Domain (SEC EDGAR data is public and freely available)",
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

COMPANIES_SCHEMA = pa.schema([
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


# =============================================================================
# Download
# =============================================================================

def download():
    """Fetch submissions for all companies with incremental caching."""
    companies = load_raw_json("company_tickers")
    print(f"  Loaded {len(companies):,} companies from ticker index")

    state = load_state("submissions")
    completed = set(state.get("completed", []))

    pending = [c for c in companies if c["cik_str"] not in completed]

    if not pending:
        print("  All company submissions already fetched")
        return

    print(f"  Fetching submissions for {len(pending):,} companies ({len(completed):,} cached)...")

    errors = 0
    with tqdm(pending, desc="Submissions", unit="co", ncols=100) as pbar:
        for company in pbar:
            cik = str(company["cik_str"]).zfill(10)
            ticker = company.get("ticker", "")

            pbar.set_postfix_str(f"{ticker or cik[:8]}", refresh=False)

            url = f"{SEC_BASE_URL}/submissions/CIK{cik}.json"

            try:
                response = rate_limited_get(url)
                response.raise_for_status()
                data = response.json()
                save_raw_json(data, f"submissions/{cik}")
            except Exception as e:
                errors += 1
                save_raw_json({"error": str(e), "cik": cik}, f"submissions/{cik}")

            completed.add(company["cik_str"])
            save_state("submissions", {"completed": sorted(completed)})

    if errors > 0:
        print(f"  Encountered {errors} errors during fetch")


# =============================================================================
# Transform: filings
# =============================================================================

def transform_filings():
    """Transform filings data from submissions into edgar_filings dataset."""
    tickers = load_raw_json("company_tickers")
    print(f"  Processing filings from {len(tickers):,} companies...")

    all_records = []
    companies_processed = 0
    errors = 0

    for ticker_entry in tickers:
        cik = str(ticker_entry["cik_str"]).zfill(10)

        try:
            data = load_raw_json(f"submissions/{cik}")
        except FileNotFoundError:
            errors += 1
            continue

        if data.get("error"):
            continue

        company_name = data.get("name", "")
        recent_filings = data.get("filings", {}).get("recent", {})

        if not recent_filings:
            continue

        forms = recent_filings.get("form", [])
        filing_dates = recent_filings.get("filingDate", [])
        accession_numbers = recent_filings.get("accessionNumber", [])
        file_numbers = recent_filings.get("fileNumber", [])
        report_dates = recent_filings.get("reportDate", [])
        is_xbrl_list = recent_filings.get("isXBRL", [])
        is_inline_xbrl_list = recent_filings.get("isInlineXBRL", [])
        primary_documents = recent_filings.get("primaryDocument", [])

        for i, form_type in enumerate(forms):
            if i >= len(filing_dates) or i >= len(accession_numbers):
                break

            all_records.append({
                "cik": cik,
                "company_name": company_name,
                "form_type": form_type,
                "filing_date": filing_dates[i],
                "accession_number": accession_numbers[i],
                "file_number": file_numbers[i] if i < len(file_numbers) else None,
                "report_date": report_dates[i] if i < len(report_dates) and report_dates[i] else None,
                "is_xbrl": bool(is_xbrl_list[i]) if i < len(is_xbrl_list) else False,
                "is_inline_xbrl": bool(is_inline_xbrl_list[i]) if i < len(is_inline_xbrl_list) else False,
                "primary_document": primary_documents[i] if i < len(primary_documents) else None,
            })

        companies_processed += 1
        if companies_processed % 1000 == 0:
            print(f"    Processed {companies_processed:,} companies, {len(all_records):,} filings...")

    if errors > 0:
        print(f"  Skipped {errors} companies with missing submissions")
    print(f"  Transformed {len(all_records):,} filings from {companies_processed:,} companies")

    table = pa.Table.from_pylist(all_records, schema=FILINGS_SCHEMA)

    validate(table, {
        "columns": {"cik": "string", "form_type": "string", "filing_date": "string", "accession_number": "string"},
        "not_null": ["cik", "company_name", "form_type", "filing_date", "accession_number"],
        "unique": ["accession_number"],
        "min_rows": 100000,
    })
    assert_length(table, "cik", 10)
    assert_valid_date(table, "filing_date")

    merge(table, FILINGS_DATASET_ID, key="accession_number")
    publish(FILINGS_DATASET_ID, FILINGS_METADATA)


# =============================================================================
# Transform: companies
# =============================================================================

def transform_companies():
    """Transform company metadata from submissions into edgar_companies dataset."""
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

        if data.get("error"):
            continue

        ticker_info = ticker_map.get(cik, {})

        exchanges = data.get("exchanges", [])
        if not exchanges:
            exchanges = None

        records.append({
            "cik": cik,
            "ticker": ticker_info.get("ticker") or (data.get("tickers", [None])[0] if data.get("tickers") else None),
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

    table = pa.Table.from_pylist(records, schema=COMPANIES_SCHEMA)

    validate(table, {
        "columns": {"cik": "string", "name": "string"},
        "not_null": ["cik", "name"],
        "unique": ["cik"],
        "min_rows": 10000,
    })
    assert_length(table, "cik", 10)

    merge(table, COMPANIES_DATASET_ID, key="cik")
    publish(COMPANIES_DATASET_ID, COMPANIES_METADATA)


# =============================================================================
# DAG
# =============================================================================

from nodes.company_tickers import download as download_tickers

NODES = {
    download: [download_tickers],
    transform_filings: [download],
    transform_companies: [download],
}


if __name__ == "__main__":
    download()
    transform_filings()
    transform_companies()
