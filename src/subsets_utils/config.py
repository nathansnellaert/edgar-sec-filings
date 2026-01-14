"""Configuration and environment utilities.

Single source of truth for all paths, directories, and environment settings.
Both local and cloud (R2) execution modes are handled here.
"""

import os


# =============================================================================
# Environment Detection
# =============================================================================

def is_cloud() -> bool:
    """Check if running in cloud mode (CI environment)."""
    return os.environ.get('CI', '').lower() == 'true'


def get_connector_name() -> str:
    """Get current connector name."""
    return os.environ.get('CONNECTOR_NAME', 'unknown')


def get_run_id() -> str:
    """Get current run ID."""
    return os.environ.get('RUN_ID', 'unknown')


# =============================================================================
# Directory Configuration
# =============================================================================

# Local mode: DATA_DIR env var (e.g., "data" or absolute path)
# Cloud mode: /tmp/subsets_cache for disk staging with LRU eviction

CACHE_DIR = os.environ.get('CACHE_DIR', '/tmp/subsets_cache')
CACHE_MIN_FREE_GB = float(os.environ.get('CACHE_MIN_FREE_GB', '1'))


def get_data_dir() -> str:
    """Get data directory for local mode. Raises in cloud mode."""
    if is_cloud():
        raise RuntimeError("get_data_dir() should not be called in cloud mode. Use R2 paths instead.")
    return os.environ.get('DATA_DIR', 'data')


def get_cache_dir() -> str:
    """Get cache directory for cloud mode disk staging."""
    return CACHE_DIR


# =============================================================================
# Environment Validation
# =============================================================================

def validate_environment(additional_required: list[str] = None):
    """Validate required environment variables based on execution mode.

    Local mode: requires DATA_DIR
    Cloud mode: requires R2 credentials
    """
    if is_cloud():
        required = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"]
    else:
        required = []  # DATA_DIR defaults to "data" in local mode

    if additional_required:
        required.extend(additional_required)

    missing = [var for var in required if var not in os.environ]
    if missing:
        mode = "cloud" if is_cloud() else "local"
        raise ValueError(f"Missing required environment variables for {mode} mode: {missing}")


# =============================================================================
# R2/S3 Storage Options
# =============================================================================

def get_storage_options() -> dict | None:
    """Get storage options for DeltaLake S3 writes. Returns None for local mode."""
    if not is_cloud():
        return None
    return {
        'AWS_ENDPOINT_URL': f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        'AWS_ACCESS_KEY_ID': os.environ['R2_ACCESS_KEY_ID'],
        'AWS_SECRET_ACCESS_KEY': os.environ['R2_SECRET_ACCESS_KEY'],
        'AWS_REGION': 'auto',
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    }


def get_bucket_name() -> str:
    """Get R2 bucket name."""
    return os.environ['R2_BUCKET_NAME']


# =============================================================================
# Path Builders
# =============================================================================

def get_r2_base() -> str:
    """Get R2 base path for current connector: <connector>/data"""
    return f"{get_connector_name()}/data"


def raw_key(asset_id: str, ext: str = "parquet") -> str:
    """Get R2 key for a raw asset."""
    return f"{get_r2_base()}/raw/{asset_id}.{ext}"


def raw_uri(asset_id: str, ext: str = "parquet") -> str:
    """Get full URI for a raw asset (S3 URI in cloud, local path otherwise).

    Use this for DuckDB queries that need full paths.
    """
    if is_cloud():
        return f"s3://{get_bucket_name()}/{raw_key(asset_id, ext)}"
    return raw_path(asset_id, ext)


def state_key(asset: str) -> str:
    """Get R2 key for a state file."""
    return f"{get_r2_base()}/state/{asset}.json"


def subsets_uri(dataset_name: str) -> str:
    """Get URI for a subsets Delta table (S3 or local path)."""
    if is_cloud():
        return f"s3://{get_bucket_name()}/{get_r2_base()}/subsets/{dataset_name}"
    return str(os.path.join(os.environ['DATA_DIR'], "subsets", dataset_name))


def raw_path(asset_id: str, ext: str = "parquet") -> str:
    """Get local path for a raw asset (local mode only)."""
    from pathlib import Path
    path = Path(os.environ['DATA_DIR']) / "raw" / f"{asset_id}.{ext}"
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def state_path(asset: str) -> str:
    """Get local path for a state file (local mode only)."""
    from pathlib import Path
    path = Path(os.environ['DATA_DIR']) / "state" / f"{asset}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def cache_path(key: str) -> str:
    """Get local cache path for an R2 key (cloud mode disk staging)."""
    from pathlib import Path
    path = Path(CACHE_DIR) / key
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)
