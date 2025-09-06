#!/usr/bin/env python3
"""Fetch and process SEC EDGAR filings index data - ALL form types."""

import os
import logging
from datetime import datetime
import pyarrow as pa
from ratelimit import limits, sleep_and_retry
from utils.http_client import get
from utils.io import load_state, save_state

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SEC API configuration
SEC_BASE_URL = "https://data.sec.gov"

def get_user_agent():
    """Get user agent from environment variable."""
    return os.environ["SEC_USER_AGENT"]


@sleep_and_retry
@limits(calls=10, period=1)  # SEC rate limit: 10 requests per second
def fetch_company_submissions(cik):
    """Fetch company submissions from SEC for a given CIK."""
    cik_str = str(cik).zfill(10)
    url = f"{SEC_BASE_URL}/submissions/CIK{cik_str}.json"
    
    headers = {
        "User-Agent": get_user_agent(),
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
        "Connection": "keep-alive"
    }
    
    try:
        response = get(url, headers=headers, timeout=60.0)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.warning(f"Failed to fetch submissions for CIK {cik_str}: {e}")
        return None


def process_filings(tickers_data):
    """Process SEC EDGAR filings for all companies - ALL form types."""
    logger.info("Processing EDGAR filings (all form types)...")
    
    # Define schema
    schema = pa.schema([
        pa.field("cik", pa.string(), nullable=False),
        pa.field("ticker", pa.string(), nullable=True),
        pa.field("company_name", pa.string(), nullable=False),
        pa.field("form_type", pa.string(), nullable=False),
        pa.field("filing_date", pa.date32(), nullable=False),
        pa.field("accession_number", pa.string(), nullable=False),
        pa.field("file_number", pa.string(), nullable=True),
        pa.field("film_number", pa.string(), nullable=True),
        pa.field("acceptance_datetime", pa.string(), nullable=True),
        pa.field("report_date", pa.date32(), nullable=True),
        pa.field("is_xbrl", pa.bool_(), nullable=False),
        pa.field("is_inline_xbrl", pa.bool_(), nullable=False),
        pa.field("primary_document", pa.string(), nullable=True),
        pa.field("primary_doc_description", pa.string(), nullable=True),
        pa.field("filing_year", pa.int32(), nullable=False),
        pa.field("filing_quarter", pa.string(), nullable=False),
        pa.field("filing_month", pa.string(), nullable=False)
    ])
    
    # If no tickers data, return empty table
    if tickers_data.num_rows == 0:
        logger.info("No tickers data provided, returning empty filings table")
        return pa.Table.from_pylist([], schema=schema)
    
    # Load state to track last update per company
    state = load_state("edgar_filings")
    last_updates = state.get("last_updates", {})
    
    all_filings = []
    processed_count = 0
    new_last_updates = {}
    
    # Process each company
    for batch in tickers_data.to_batches():
        for row in batch.to_pylist():
            cik = row["cik_str"]
            ticker = row["ticker"]
            company_name = row["title"]
            
            # Get last update date for this company
            last_update = last_updates.get(cik)
            if last_update:
                last_update_dt = datetime.fromisoformat(last_update)
            else:
                last_update_dt = datetime(1900, 1, 1)  # Process all if no previous state
            
            logger.info(f"Processing filings for {ticker} (CIK: {cik})")
            
            # Fetch submissions data
            submissions_data = fetch_company_submissions(cik)
            if not submissions_data:
                continue
            
            # Track the most recent filing date for this company
            most_recent_filing = last_update_dt
            
            # Process recent filings
            recent_filings = submissions_data.get("filings", {}).get("recent", {})
            if not recent_filings:
                continue
            
            # Extract filing data arrays
            forms = recent_filings.get("form", [])
            filing_dates = recent_filings.get("filingDate", [])
            accession_numbers = recent_filings.get("accessionNumber", [])
            file_numbers = recent_filings.get("fileNumber", [])
            film_numbers = recent_filings.get("filmNumber", [])
            acceptance_datetimes = recent_filings.get("acceptanceDateTime", [])
            report_dates = recent_filings.get("reportDate", [])
            is_xbrl_list = recent_filings.get("isXBRL", [])
            is_inline_xbrl_list = recent_filings.get("isInlineXBRL", [])
            primary_documents = recent_filings.get("primaryDocument", [])
            primary_doc_descriptions = recent_filings.get("primaryDocDescription", [])
            
            # Process ALL form types (no filtering)
            for i, form_type in enumerate(forms):
                if i >= len(filing_dates) or i >= len(accession_numbers):
                    break
                
                filing_date = filing_dates[i]
                filing_dt = datetime.strptime(filing_date, "%Y-%m-%d")
                
                # Skip if this filing is older than our last update
                if filing_dt <= last_update_dt:
                    continue
                
                # Update most recent filing date
                if filing_dt > most_recent_filing:
                    most_recent_filing = filing_dt
                
                # Parse report date if available
                report_date = None
                if i < len(report_dates) and report_dates[i]:
                    try:
                        report_date = datetime.strptime(report_dates[i], "%Y-%m-%d").date()
                    except:
                        pass
                
                filing_record = {
                    "cik": cik,
                    "ticker": ticker,
                    "company_name": company_name,
                    "form_type": form_type,
                    "filing_date": filing_dt.date(),
                    "accession_number": accession_numbers[i] if i < len(accession_numbers) else None,
                    "file_number": file_numbers[i] if i < len(file_numbers) else None,
                    "film_number": film_numbers[i] if i < len(film_numbers) else None,
                    "acceptance_datetime": acceptance_datetimes[i] if i < len(acceptance_datetimes) else None,
                    "report_date": report_date,
                    "is_xbrl": is_xbrl_list[i] if i < len(is_xbrl_list) else False,
                    "is_inline_xbrl": is_inline_xbrl_list[i] if i < len(is_inline_xbrl_list) else False,
                    "primary_document": primary_documents[i] if i < len(primary_documents) else None,
                    "primary_doc_description": primary_doc_descriptions[i] if i < len(primary_doc_descriptions) else None,
                    "filing_year": filing_dt.year,
                    "filing_quarter": f"{filing_dt.year}-Q{(filing_dt.month - 1) // 3 + 1}",
                    "filing_month": f"{filing_dt.year}-{filing_dt.month:02d}"
                }
                
                all_filings.append(filing_record)
            
            # Update last update date for this company
            if most_recent_filing > last_update_dt:
                new_last_updates[cik] = most_recent_filing.isoformat()
            else:
                new_last_updates[cik] = last_updates.get(cik, datetime(1900, 1, 1).isoformat())
            
            processed_count += 1
            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count} companies, found {len(all_filings)} new filings")
    
    # Save updated state
    save_state("edgar_filings", {
        "last_updates": new_last_updates,
        "last_run": datetime.utcnow().isoformat()
    })
    
    logger.info(f"Processed {len(all_filings)} new filings across all form types")
    
    # Create PyArrow table
    table = pa.Table.from_pylist(all_filings, schema=schema)
    return table