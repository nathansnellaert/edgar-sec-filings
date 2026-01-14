import json
from deltalake import DeltaTable
from .config import subsets_uri, get_storage_options


def publish(dataset_name: str, metadata: dict):
    """Publish metadata to a Delta table."""
    if 'id' not in metadata:
        raise ValueError("Missing required field: 'id'")
    if 'title' not in metadata:
        raise ValueError("Missing required field: 'title'")

    uri = subsets_uri(dataset_name)
    storage_opts = get_storage_options()
    dt = DeltaTable(uri, storage_options=storage_opts) if storage_opts else DeltaTable(uri)

    if 'column_descriptions' in metadata:
        schema = dt.schema().to_pyarrow() if hasattr(dt.schema(), 'to_pyarrow') else dt.schema().to_arrow()
        actual_columns = {field.name for field in schema}
        col_descs = json.loads(metadata['column_descriptions']) if isinstance(
            metadata['column_descriptions'], str
        ) else metadata['column_descriptions']
        invalid = set(col_descs.keys()) - actual_columns
        if invalid:
            raise ValueError(f"Invalid columns in descriptions: {sorted(invalid)}")

    dt.alter.set_table_description(json.dumps(metadata))
    print(f"Published metadata for {dataset_name}")
