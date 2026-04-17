"""Download the SEC company tickers index.

This is the entry-point node -- other nodes depend on it because it provides the
list of CIK numbers used to fetch submissions and XBRL facts.
"""

from subsets_utils import save_raw_json
from connector_utils import rate_limited_get

SEC_BASE_URL = "https://www.sec.gov"
COMPANY_TICKERS_URL = f"{SEC_BASE_URL}/files/company_tickers.json"


def download():
    """Fetch the company tickers index from SEC."""
    print("  Fetching company tickers index...")
    response = rate_limited_get(COMPANY_TICKERS_URL, host="www.sec.gov")
    response.raise_for_status()
    data = response.json()

    # Convert from {0: {...}, 1: {...}} to list format
    companies = list(data.values())

    save_raw_json(companies, "company_tickers")
    print(f"  Fetched {len(companies):,} company tickers")


NODES = {
    download: [],
}


if __name__ == "__main__":
    download()
