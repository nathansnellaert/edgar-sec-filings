#!/usr/bin/env python3
"""Main orchestrator for SEC EDGAR filings data integration."""

import os
os.environ['CONNECTOR_NAME'] = 'edgar-sec-filings'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

# Ensure HTTP_USER_AGENT is set for the http_client utility
# SEC requires email in User-Agent per their fair access policy
if 'HTTP_USER_AGENT' not in os.environ:
    os.environ['HTTP_USER_AGENT'] = os.getenv('SEC_USER_AGENT', 'DataIntegrations/1.0 (admin@dataintegrations.io)')

from utils import validate_environment, upload_data
from assets.company_tickers.company_tickers import process_tickers
from assets.edgar_filings.edgar_filings import process_filings
from assets.xbrl_company_facts.xbrl_company_facts import process_xbrl_facts

def main():
    validate_environment(["SEC_USER_AGENT"])
        
    # Process and upload company metadata
    tickers_data = process_tickers()
    upload_data(tickers_data, "edgar_company_tickers")
    
    # Process and upload dependent assets
    filings_data = process_filings(tickers_data)
    if filings_data.num_rows > 0:
        upload_data(filings_data, "edgar_filings")
    
    facts_data = process_xbrl_facts(tickers_data)
    if facts_data.num_rows > 0:
        upload_data(facts_data, "edgar_xbrl_company_facts")


if __name__ == "__main__":
    main()