
"""Ingest SEC company tickers list."""

from subsets_utils import save_raw_json
from utils import rate_limited_get

SEC_BASE_URL = "https://www.sec.gov"
COMPANY_TICKERS_URL = f"{SEC_BASE_URL}/files/company_tickers.json"


def run():
    """Fetch the company tickers index from SEC.

    This is a single static file that lists all SEC-registered companies
    with their CIK, ticker, and name. Used as the master list for other
    ingest modules.
    """
    print("  Fetching company tickers index...")
    response = rate_limited_get(COMPANY_TICKERS_URL, host="www.sec.gov")
    response.raise_for_status()
    data = response.json()

    # Convert from {0: {...}, 1: {...}} to list format
    companies = list(data.values())

    save_raw_json(companies, "company_tickers")
    print(f"  Fetched {len(companies):,} company tickers")
