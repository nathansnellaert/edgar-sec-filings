import os
import logging
from ratelimit import limits, sleep_and_retry
from subsets_utils.http_client import get

logger = logging.getLogger(__name__)

def get_user_agent():
    """Get user agent for SEC API requests.

    SEC requires a User-Agent header with contact information.
    Format: CompanyName/Version (Contact Email)
    """
    return os.environ.get(
        "SEC_USER_AGENT",
        "DataIntegrations/1.0 (admin@dataintegrations.io)"
    )


def _get_headers(host="data.sec.gov"):
    """Get headers for SEC API requests."""
    return {
        "User-Agent": get_user_agent(),
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Host": host,
        "Connection": "keep-alive"
    }


# SEC rate limit: 10 requests per second - shared across all modules
@sleep_and_retry
@limits(calls=10, period=1)
def rate_limited_get(url, timeout=60.0, host="data.sec.gov"):
    """Rate-limited GET request to SEC API."""
    headers = _get_headers(host)
    response = get(url, headers=headers, timeout=timeout)
    return response
