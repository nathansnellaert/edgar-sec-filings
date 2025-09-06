import os

# Test the fix with GitHub Actions-like environment
os.environ['CONNECTOR_NAME'] = 'edgar-sec-filings'
os.environ['RUN_ID'] = 'test-final-fix'
os.environ['ENABLE_HTTP_CACHE'] = 'false'
os.environ['STORAGE_BACKEND'] = 'local'
os.environ['DATA_DIR'] = 'data'

# Test with just SEC_USER_AGENT set (like in GitHub Actions)
os.environ['SEC_USER_AGENT'] = 'DataIntegrations/1.0 (admin@dataintegrations.io)'

print("Testing final fix with corrected User-Agent...")
print(f"SEC_USER_AGENT = {os.environ.get('SEC_USER_AGENT')}")
print(f"HTTP_USER_AGENT (before main.py) = {os.environ.get('HTTP_USER_AGENT', 'NOT SET')}")

# Import main which should set HTTP_USER_AGENT
import main

print(f"HTTP_USER_AGENT (after main.py import) = {os.environ.get('HTTP_USER_AGENT')}")

# Test fetching tickers
from assets.company_tickers.company_tickers import fetch_company_tickers

try:
    print("\nFetching company tickers...")
    data = fetch_company_tickers()
    print(f"✓ SUCCESS! Got {len(data)} tickers")
    print(f"✓ Sample ticker: {list(data.items())[0] if data else 'No data'}")
    print("\n✅ The fix works! SEC API accepts our User-Agent.")
except Exception as e:
    print(f"✗ FAILED: {e}")
    print("\n❌ The fix didn't work. Further investigation needed.")