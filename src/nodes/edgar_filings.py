"""Ingest and transform SEC EDGAR filings.

Fetches company submissions (metadata + filings) from SEC and transforms into edgar_filings dataset.
"""

import os

# SEC requires email in User-Agent per their fair access policy
if 'HTTP_USER_AGENT' not in os.environ:
    os.environ['HTTP_USER_AGENT'] = os.getenv('SEC_USER_AGENT', 'DataIntegrations/1.0 (admin@dataintegrations.io)')

import pyarrow as pa
from tqdm import tqdm
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, upload_data
from utils import rate_limited_get
from _transforms.edgar_filings.test import test

SEC_BASE_URL = "https://data.sec.gov"

DATASET_ID = "edgar_filings"

METADATA = {
    "title": "SEC EDGAR Filings",
    "description": "SEC EDGAR filings including 10-K, 10-Q, 8-K, and other forms. Contains filing dates, form types, and accession numbers for all SEC-registered companies.",
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

SCHEMA = pa.schema([
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


def ingest():
    """Fetch submissions for all companies with incremental caching."""
    # Load the ticker index
    companies = load_raw_json("company_tickers")
    print(f"  Loaded {len(companies):,} companies from ticker index")

    # Track which companies we've already fetched
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

                # Save per-company (includes metadata + filings)
                save_raw_json(data, f"submissions/{cik}")

            except Exception as e:
                errors += 1
                # Save error marker to avoid retrying
                save_raw_json({"error": str(e), "cik": cik}, f"submissions/{cik}")

            # Update state after each save
            completed.add(company["cik_str"])
            save_state("submissions", {"completed": sorted(completed)})

    if errors > 0:
        print(f"  Encountered {errors} errors during fetch")


def transform():
    """Transform filings data from submissions into a filings dataset."""
    # Load ticker index to get list of all companies
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

        # Skip error entries
        if data.get("error"):
            continue

        company_name = data.get("name", "")
        recent_filings = data.get("filings", {}).get("recent", {})

        if not recent_filings:
            continue

        # Extract filing arrays
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

    table = pa.Table.from_pylist(all_records, schema=SCHEMA)

    test(table)

    upload_data(table, DATASET_ID, mode="overwrite")
def run():
    """Ingest and transform edgar filings."""
    print("\n--- EDGAR Filings ---")
    ingest()
    transform()


from nodes.company_tickers import run as company_tickers_run

NODES = {
    run: [company_tickers_run],
}


if __name__ == "__main__":
    run()
