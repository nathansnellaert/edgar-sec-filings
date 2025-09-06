#!/usr/bin/env python3
"""Fetch and process SEC company tickers data with enhanced metadata."""

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
SEC_BASE_URL = "https://www.sec.gov"
COMPANY_TICKERS_URL = f"{SEC_BASE_URL}/files/company_tickers.json"

def get_user_agent():
    """Get user agent for SEC API requests.
    
    SEC requires a User-Agent header with contact information.
    Format: CompanyName/Version (Contact Email)
    """
    return os.environ.get(
        "SEC_USER_AGENT", 
        "DataIntegrations/1.0 (admin@dataintegrations.io)"
    )


@sleep_and_retry
@limits(calls=10, period=1)  # SEC rate limit: 10 requests per second
def fetch_company_tickers():
    """Fetch company tickers from SEC."""
    headers = {
        "User-Agent": get_user_agent(),
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
        "Connection": "keep-alive"
    }
    
    logger.info(f"Fetching from: {COMPANY_TICKERS_URL}")
    
    response = get(COMPANY_TICKERS_URL, headers=headers, timeout=60.0)
    response.raise_for_status()
    return response.json()


@sleep_and_retry
@limits(calls=10, period=1)  # SEC rate limit: 10 requests per second
def fetch_company_metadata(cik):
    """Fetch detailed company metadata from SEC submissions endpoint."""
    cik_str = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_str}.json"
    
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
        logger.warning(f"Failed to fetch metadata for CIK {cik_str}: {e}")
        return None


def process_tickers():
    """Process SEC company tickers with enhanced metadata and return as PyArrow table."""
    logger.info("Fetching company tickers from SEC...")
    
    # Define enhanced schema with metadata fields
    schema = pa.schema([
        pa.field("cik_str", pa.string(), nullable=False),
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("sic_code", pa.string(), nullable=True),
        pa.field("sic_description", pa.string(), nullable=True),
        pa.field("state_of_incorporation", pa.string(), nullable=True),
        pa.field("fiscal_year_end", pa.string(), nullable=True),
        pa.field("business_address_state", pa.string(), nullable=True),
        pa.field("mailing_address_state", pa.string(), nullable=True),
        pa.field("ein", pa.string(), nullable=True),
        pa.field("entity_type", pa.string(), nullable=True),
        pa.field("exchanges", pa.string(), nullable=True),
        pa.field("former_names", pa.string(), nullable=True)
    ])
    
    # Load previous state
    state = load_state("company_tickers")
    previous_tickers = state.get("tickers", {})
    
    # Fetch current data
    tickers_data = fetch_company_tickers()
    
    # Create current tickers dict for comparison (keyed by CIK)
    current_tickers = {}
    for key, ticker_info in tickers_data.items():
        cik = str(ticker_info["cik_str"]).zfill(10)
        current_tickers[cik] = {
            "ticker": ticker_info["ticker"],
            "title": ticker_info["title"]
        }
    
    # Check if there are any changes in base ticker data
    base_tickers = {k: {"ticker": v["ticker"], "title": v["title"]} 
                   for k, v in previous_tickers.items() if "ticker" in v}
    if current_tickers == base_tickers and len(previous_tickers) > 0:
        logger.info("No changes in company tickers")
        # Return empty table to indicate no updates needed
        return pa.Table.from_pylist([], schema=schema)
    
    # Convert to list of records with enhanced metadata
    records = []
    processed_count = 0
    
    for cik, info in current_tickers.items():
        record = {
            "cik_str": cik,
            "ticker": info["ticker"],
            "title": info["title"],
            "sic_code": None,
            "sic_description": None,
            "state_of_incorporation": None,
            "fiscal_year_end": None,
            "business_address_state": None,
            "mailing_address_state": None,
            "ein": None,
            "entity_type": None,
            "exchanges": None,
            "former_names": None
        }
        
        # Fetch additional metadata for each company
        metadata = fetch_company_metadata(cik)
        if metadata:
            record["sic_code"] = metadata.get("sic")
            record["sic_description"] = metadata.get("sicDescription")
            record["state_of_incorporation"] = metadata.get("stateOfIncorporation")
            record["fiscal_year_end"] = metadata.get("fiscalYearEnd")
            record["ein"] = metadata.get("ein")
            record["entity_type"] = metadata.get("entityType")
            
            # Extract addresses
            if "addresses" in metadata:
                business_addr = metadata["addresses"].get("business", {})
                mailing_addr = metadata["addresses"].get("mailing", {})
                record["business_address_state"] = business_addr.get("stateOrCountry")
                record["mailing_address_state"] = mailing_addr.get("stateOrCountry")
            
            # Extract exchanges (list to comma-separated string)
            exchanges = metadata.get("exchanges", [])
            if exchanges:
                record["exchanges"] = ",".join(exchanges)
            
            # Extract former names (list to JSON string for complex structure)
            former_names = metadata.get("formerNames", [])
            if former_names:
                # Just get the names, not the dates
                names_list = [fn.get("name", "") for fn in former_names if fn.get("name")]
                if names_list:
                    record["former_names"] = "|".join(names_list[:5])  # Limit to 5 most recent
        
        records.append(record)
        
        processed_count += 1
        if processed_count % 100 == 0:
            logger.info(f"Processed metadata for {processed_count}/{len(current_tickers)} companies")
    
    # Save enhanced state with all metadata
    save_state("company_tickers", {
        "tickers": {r["cik_str"]: r for r in records},
        "last_update": datetime.utcnow().isoformat()
    })
    
    # Create PyArrow table
    table = pa.Table.from_pylist(records, schema=schema)
    logger.info(f"Processed {len(records)} company tickers with metadata (changes detected)")
    
    return table