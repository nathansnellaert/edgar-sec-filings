
"""Ingest SEC EDGAR company submissions (metadata + filings).

The submissions endpoint returns both company metadata and filing history
in one call, so this module handles both.
"""

from tqdm import tqdm
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state
from sec_client import rate_limited_get

SEC_BASE_URL = "https://data.sec.gov"


def run():
    """Fetch submissions for all companies with incremental caching.

    Saves each company's data separately to enable resumption and
    incremental updates.
    """
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
            save_state("submissions", {"completed": list(completed)})

    print(f"  Fetched submissions for {len(completed):,} companies ({errors} errors)")
