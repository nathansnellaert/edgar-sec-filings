"""Transform SEC XBRL Company Facts into structured financial data.

Memory-efficient approach:
1. Process companies in batches of 500
2. Write each batch as parquet to raw/xbrl_processed/ (local mode only)
3. Write directly to Delta table (partitioned by taxonomy)
"""

import shutil
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake, DeltaTable
from subsets_utils import load_raw_json, publish, get_data_dir
from subsets_utils.r2 import is_cloud_mode, get_storage_options, get_delta_table_uri, get_connector_name
from transforms.xbrl_company_facts.test import test

DATASET_ID = "edgar_xbrl_facts"
BATCH_SIZE = 500

METADATA = {
    "id": DATASET_ID,
    "title": "SEC EDGAR XBRL Financial Facts",
    "description": "Structured financial data from SEC XBRL filings. Contains standardized financial metrics (revenue, assets, liabilities, etc.) extracted from company filings using XBRL taxonomies. Partitioned by taxonomy (us-gaap, dei, srt, etc.).",
    "column_descriptions": {
        "cik": "Central Index Key (10-digit)",
        "entity_name": "Company name",
        "taxonomy": "XBRL taxonomy (us-gaap, dei, etc.)",
        "concept": "XBRL concept name (e.g., Assets, Revenue)",
        "label": "Human-readable label for the concept",
        "unit": "Unit of measure (USD, shares, pure)",
        "value": "Reported value",
        "end_date": "Period end date (YYYY-MM-DD)",
        "fiscal_year": "Fiscal year",
        "fiscal_period": "Fiscal period (FY, Q1, Q2, Q3, Q4)",
        "form": "Filing form type (10-K, 10-Q)",
        "filed": "Date filed with SEC",
        "accession": "SEC accession number",
    }
}

SCHEMA = pa.schema([
    pa.field("cik", pa.string(), nullable=False),
    pa.field("entity_name", pa.string(), nullable=False),
    pa.field("taxonomy", pa.string(), nullable=False),
    pa.field("concept", pa.string(), nullable=False),
    pa.field("label", pa.string()),
    pa.field("unit", pa.string(), nullable=False),
    pa.field("value", pa.string()),
    pa.field("end_date", pa.string(), nullable=False),
    pa.field("fiscal_year", pa.int32()),
    pa.field("fiscal_period", pa.string()),
    pa.field("form", pa.string(), nullable=False),
    pa.field("filed", pa.string(), nullable=False),
    pa.field("accession", pa.string(), nullable=False),
])


def extract_company_facts(cik: str) -> list[dict]:
    """Extract all facts from a single company's XBRL data."""
    data = load_raw_json(f"xbrl_facts/{cik}")

    if data.get("no_data") or data.get("error"):
        return []

    entity_name = data.get("entityName", "")
    facts = data.get("facts", {})

    if not facts:
        return []

    records = []
    for taxonomy, concepts in facts.items():
        if not isinstance(concepts, dict):
            continue

        for concept, concept_data in concepts.items():
            if not isinstance(concept_data, dict):
                continue

            label = concept_data.get("label", "")
            units_data = concept_data.get("units", {})

            for unit_type, unit_facts in units_data.items():
                if not isinstance(unit_facts, list):
                    continue

                for fact in unit_facts:
                    if not isinstance(fact, dict):
                        continue

                    filed_str = fact.get("filed")
                    if not filed_str:
                        continue

                    end_date = fact.get("end") or fact.get("instant")
                    if not end_date:
                        continue

                    records.append({
                        "cik": cik,
                        "entity_name": entity_name,
                        "taxonomy": taxonomy,
                        "concept": concept,
                        "label": label,
                        "unit": unit_type,
                        "value": str(fact.get("val")) if fact.get("val") is not None else None,
                        "end_date": end_date,
                        "fiscal_year": fact.get("fy"),
                        "fiscal_period": fact.get("fp"),
                        "form": fact.get("form", ""),
                        "filed": filed_str,
                        "accession": fact.get("accn", ""),
                    })

    return records


def run():
    """Transform XBRL facts with memory-efficient batching.

    Works in both local and cloud mode by:
    1. Loading company list from ticker index (not filesystem glob)
    2. Processing in batches and writing directly to Delta table
    """
    # Load ticker index to get list of all companies (works in both local and cloud mode)
    tickers = load_raw_json("company_tickers")
    print(f"  Processing XBRL facts from {len(tickers):,} companies in batches of {BATCH_SIZE}...")

    batch_records = []
    batch_num = 0
    companies_processed = 0
    companies_with_data = 0
    total_facts = 0
    errors = 0

    # Determine output location
    if is_cloud_mode():
        table_uri = get_delta_table_uri(DATASET_ID)
        storage_options = get_storage_options()
    else:
        data_dir = Path(get_data_dir())
        table_path = data_dir / "subsets" / DATASET_ID
        if table_path.exists():
            shutil.rmtree(table_path)
        table_uri = str(table_path)
        storage_options = None

    first_batch = True

    for ticker_entry in tickers:
        cik = str(ticker_entry["cik_str"]).zfill(10)

        try:
            records = extract_company_facts(cik)
        except FileNotFoundError:
            errors += 1
            companies_processed += 1
            continue

        if records:
            batch_records.extend(records)
            companies_with_data += 1

        companies_processed += 1

        # Write batch when we hit batch size
        if companies_processed % BATCH_SIZE == 0:
            if batch_records:
                table = pa.Table.from_pylist(batch_records, schema=SCHEMA)
                mode = "overwrite" if first_batch else "append"
                if storage_options:
                    write_deltalake(
                        table_uri,
                        table,
                        mode=mode,
                        partition_by=["taxonomy"],
                        schema_mode="merge",
                        storage_options=storage_options,
                    )
                else:
                    write_deltalake(
                        table_uri,
                        table,
                        mode=mode,
                        partition_by=["taxonomy"],
                        schema_mode="merge",
                    )
                total_facts += len(batch_records)
                print(f"    Batch {batch_num}: {len(batch_records):,} facts from {BATCH_SIZE} companies ({companies_processed:,}/{len(tickers):,})")
                batch_num += 1
                batch_records.clear()
                first_batch = False

    # Write final partial batch
    if batch_records:
        table = pa.Table.from_pylist(batch_records, schema=SCHEMA)
        mode = "overwrite" if first_batch else "append"
        if storage_options:
            write_deltalake(
                table_uri,
                table,
                mode=mode,
                partition_by=["taxonomy"],
                schema_mode="merge",
                storage_options=storage_options,
            )
        else:
            write_deltalake(
                table_uri,
                table,
                mode=mode,
                partition_by=["taxonomy"],
                schema_mode="merge",
            )
        total_facts += len(batch_records)
        print(f"    Batch {batch_num}: {len(batch_records):,} facts (final batch)")
        batch_records.clear()

    if errors > 0:
        print(f"  Skipped {errors} companies with missing XBRL data")
    print(f"  Wrote {total_facts:,} total facts from {companies_with_data:,} companies")

    # Validate by reading Delta table metadata (row count) without loading all data
    if storage_options:
        dt = DeltaTable(table_uri, storage_options=storage_options)
    else:
        dt = DeltaTable(table_uri)
    add_actions = dt.get_add_actions()
    # Handle both old API (returns list-like with to_pylist) and new API (returns RecordBatch)
    if hasattr(add_actions, 'to_pydict'):
        # New arro3/RecordBatch API
        total_rows = sum(add_actions.to_pydict()['num_records'])
    else:
        # Old API
        total_rows = sum(f.num_records for f in add_actions.to_pylist())
    print(f"  Delta table has {total_rows:,} rows")

    if total_rows < 100000:
        raise ValueError(f"Expected at least 100,000 rows, got {total_rows:,}")

    publish(DATASET_ID, METADATA)
    print(f"  Published {DATASET_ID} partitioned by taxonomy")


if __name__ == "__main__":
    run()
