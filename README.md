# edgar-sec-filings

SEC EDGAR corporate filings, company metadata, and XBRL financial facts.

## Datasets

| Dataset | Description | Key |
|---------|-------------|-----|
| `edgar_companies` | ~15k SEC-registered companies with CIK, ticker, SIC codes, exchange listings | `cik` |
| `edgar_filings` | All recent filings (10-K, 10-Q, 8-K, etc.) with form types and accession numbers | `accession_number` |
| `edgar_xbrl_facts` | Structured financial metrics extracted from XBRL filings (revenue, assets, etc.) | composite |

## Coverage

- **Companies**: All SEC-registered entities with tickers (~15k unique CIKs)
- **Filings**: Recent filings from the submissions API (typically last ~1000 per company)
- **XBRL facts**: All available XBRL data per company; many smaller companies have no XBRL (404s are expected and skipped)

## Data source

All data comes from SEC EDGAR public APIs:
- `https://www.sec.gov/files/company_tickers.json` - ticker index
- `https://data.sec.gov/submissions/CIK{cik}.json` - submissions per company
- `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json` - XBRL facts per company

SEC rate limit: 10 requests/second. License: public domain.

## Secrets

- `SEC_USER_AGENT`: Required. SEC mandates a User-Agent with contact info.
