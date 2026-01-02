
"""Ingest SEC XBRL Company Facts data.

XBRL facts are structured financial data extracted from filings.
This is separate from submissions because not all companies have XBRL data.
"""

from tqdm import tqdm
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state
from sec_client import rate_limited_get

SEC_BASE_URL = "https://data.sec.gov"


def run():
    """Fetch XBRL facts for all companies with incremental caching.

    Saves each company's facts separately. Uses compression since
    XBRL data can be large.
    """
    # Load the ticker index
    companies = load_raw_json("company_tickers")
    print(f"  Loaded {len(companies):,} companies from ticker index")

    # Track which companies we've already fetched
    state = load_state("xbrl_facts")
    completed = set(state.get("completed", []))

    pending = [c for c in companies if c["cik_str"] not in completed]

    if not pending:
        print("  All XBRL facts already fetched")
        return

    print(f"  Fetching XBRL facts for {len(pending):,} companies ({len(completed):,} cached)...")

    errors = 0
    no_data = 0
    with tqdm(pending, desc="XBRL Facts", unit="co", ncols=100) as pbar:
        for company in pbar:
            cik = str(company["cik_str"]).zfill(10)
            ticker = company.get("ticker", "")

            pbar.set_postfix_str(f"{ticker or cik[:8]}", refresh=False)

            url = f"{SEC_BASE_URL}/api/xbrl/companyfacts/CIK{cik}.json"

            try:
                response = rate_limited_get(url)
                response.raise_for_status()
                data = response.json()

                # Save per-company with compression (XBRL data is large)
                save_raw_json(data, f"xbrl_facts/{cik}", compress=True)

            except Exception as e:
                # Many companies don't have XBRL data (404) - that's expected
                if "404" in str(e):
                    no_data += 1
                else:
                    errors += 1
                # Save empty marker to avoid retrying
                save_raw_json({"no_data": True, "cik": cik}, f"xbrl_facts/{cik}")

            # Update state after each save
            completed.add(company["cik_str"])
            save_state("xbrl_facts", {"completed": list(completed)})

    print(f"  Fetched XBRL facts for {len(completed):,} companies ({no_data} without XBRL, {errors} errors)")
