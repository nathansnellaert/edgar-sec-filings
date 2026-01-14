"""Filesystem abstraction for local and cloud storage.

Provides unified paths and filesystem access for both local development and cloud (R2) execution.
PyArrow and DeltaLake can use these directly without manual if/else branching.
"""

import os
from functools import lru_cache

# Lazy import to avoid boto3 dependency in local mode
_fs_instance = None


def is_cloud() -> bool:
    """Check if running in cloud mode (CI environment)."""
    return os.environ.get('CI', '').lower() == 'true'


def get_connector_name() -> str:
    """Get current connector name."""
    return os.environ.get('CONNECTOR_NAME', 'unknown')


@lru_cache(maxsize=1)
def get_base_uri() -> str:
    """Get base URI for all data storage."""
    if is_cloud():
        bucket = os.environ['R2_BUCKET_NAME']
        connector = get_connector_name()
        return f"s3://{bucket}/{connector}/data"
    return os.environ.get('DATA_DIR', 'data')


def get_filesystem():
    """Get PyArrow filesystem (S3 or local). Cached singleton."""
    global _fs_instance
    if _fs_instance is not None:
        return _fs_instance

    if is_cloud():
        from pyarrow.fs import S3FileSystem
        _fs_instance = S3FileSystem(
            endpoint_override=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            access_key=os.environ['R2_ACCESS_KEY_ID'],
            secret_key=os.environ['R2_SECRET_ACCESS_KEY'],
        )
    else:
        from pyarrow.fs import LocalFileSystem
        _fs_instance = LocalFileSystem()

    return _fs_instance


def get_storage_options() -> dict | None:
    """Get storage options for DeltaLake. Returns None for local mode."""
    if not is_cloud():
        return None
    return {
        'AWS_ENDPOINT_URL': f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        'AWS_ACCESS_KEY_ID': os.environ['R2_ACCESS_KEY_ID'],
        'AWS_SECRET_ACCESS_KEY': os.environ['R2_SECRET_ACCESS_KEY'],
        'AWS_REGION': 'auto',
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    }


# --- Path helpers ---

def raw_uri(asset_id: str, ext: str = "parquet") -> str:
    """Get URI for a raw data asset."""
    return f"{get_base_uri()}/raw/{asset_id}.{ext}"


def subsets_uri(dataset_name: str) -> str:
    """Get URI for a subsets Delta table."""
    return f"{get_base_uri()}/subsets/{dataset_name}"


def state_uri(asset: str) -> str:
    """Get URI for a state file."""
    return f"{get_base_uri()}/state/{asset}.json"


def fs_path(uri: str, ensure_parent: bool = False) -> str:
    """Convert URI to filesystem path. Optionally ensure parent dir exists (local only)."""
    from pathlib import Path
    path = uri.replace("s3://", "") if uri.startswith("s3://") else uri
    if ensure_parent and not is_cloud():
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    return path
