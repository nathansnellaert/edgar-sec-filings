"""Transform SEC EDGAR filings from submissions data."""

import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "edgar_filings"

METADATA = {
    "id": DATASET_ID,
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


def run():
    """Transform filings data from submissions into a filings dataset."""
    # Load ticker index to get list of all companies (works in both local and cloud mode)
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
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
