#!/usr/bin/env python3
"""Main orchestrator for SEC EDGAR filings data integration.

Data streams:
1. company_tickers - Basic ticker list from SEC (fast, ~13k companies)
2. edgar_filings (submissions) - Company metadata + filing history per company
3. xbrl_company_facts - Structured financial data per company

The submissions endpoint provides both company metadata and filings,
so we fetch it once and use it for both transforms.
"""

import argparse
import os

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

# SEC requires email in User-Agent per their fair access policy
if 'HTTP_USER_AGENT' not in os.environ:
    os.environ['HTTP_USER_AGENT'] = os.getenv('SEC_USER_AGENT', 'DataIntegrations/1.0 (admin@dataintegrations.io)')

from subsets_utils import validate_environment

from ingest import company_tickers as ingest_tickers
from ingest import edgar_filings as ingest_submissions
from ingest import xbrl_company_facts as ingest_xbrl

from transforms.company_tickers.main import run as transform_companies
from transforms.edgar_filings.main import run as transform_filings
from transforms.xbrl_company_facts.main import run as transform_xbrl


def main():
    parser = argparse.ArgumentParser(description="SEC EDGAR data integration")
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from APIs")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment(["SEC_USER_AGENT"])

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")

        print("\n--- Fetching company tickers index ---")
        ingest_tickers.run()

        print("\n--- Fetching company submissions (metadata + filings) ---")
        ingest_submissions.run()

        print("\n--- Fetching XBRL company facts ---")
        ingest_xbrl.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")

        print("\n--- Transforming to edgar_companies ---")
        transform_companies()

        print("\n--- Transforming to edgar_filings ---")
        transform_filings()

        print("\n--- Transforming to edgar_xbrl_facts ---")
        transform_xbrl()


if __name__ == "__main__":
    main()
