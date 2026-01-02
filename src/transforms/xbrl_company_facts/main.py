"""Transform SEC XBRL Company Facts into structured financial data.

Memory-efficient approach:
1. Process companies in batches of 500
2. Write each batch as parquet to raw/xbrl_processed/
3. Read all parquet files and write as partitioned Delta table
"""

import shutil
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake
from subsets_utils import load_raw_json, publish, get_data_dir
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

    1. Process companies in batches, writing parquet files to raw/xbrl_processed/
    2. Read all parquet files (PyArrow is memory-efficient for columnar data)
    3. Validate the full dataset
    4. Write once to partitioned Delta table
    """
    data_dir = Path(get_data_dir())
    xbrl_dir = data_dir / "raw" / "xbrl_facts"
    processed_dir = data_dir / "raw" / "xbrl_processed"

    if not xbrl_dir.exists():
        raise FileNotFoundError(f"No xbrl_facts directory found at {xbrl_dir}")

    # Clean up any previous processed files
    if processed_dir.exists():
        shutil.rmtree(processed_dir)
    processed_dir.mkdir(parents=True)

    xbrl_files = list(xbrl_dir.glob("*.json")) + list(xbrl_dir.glob("*.json.gz"))
    print(f"  Processing XBRL facts from {len(xbrl_files):,} companies in batches of {BATCH_SIZE}...")

    batch_records = []
    batch_num = 0
    companies_processed = 0
    companies_with_data = 0
    total_facts = 0

    for filepath in xbrl_files:
        cik = filepath.stem.replace(".json", "")
        records = extract_company_facts(cik)

        if records:
            batch_records.extend(records)
            companies_with_data += 1

        companies_processed += 1

        # Write batch when we hit batch size
        if companies_processed % BATCH_SIZE == 0:
            if batch_records:
                table = pa.Table.from_pylist(batch_records, schema=SCHEMA)
                batch_path = processed_dir / f"batch_{batch_num:04d}.parquet"
                pq.write_table(table, batch_path, compression="snappy")
                total_facts += len(batch_records)
                print(f"    Batch {batch_num}: {len(batch_records):,} facts from {BATCH_SIZE} companies ({companies_processed:,}/{len(xbrl_files):,})")
                batch_num += 1
                batch_records.clear()

    # Write final partial batch
    if batch_records:
        table = pa.Table.from_pylist(batch_records, schema=SCHEMA)
        batch_path = processed_dir / f"batch_{batch_num:04d}.parquet"
        pq.write_table(table, batch_path, compression="snappy")
        total_facts += len(batch_records)
        print(f"    Batch {batch_num}: {len(batch_records):,} facts (final batch)")
        batch_records.clear()

    print(f"  Wrote {total_facts:,} total facts from {companies_with_data:,} companies")

    # Read parquet batches and write directly to Delta table
    table_path = data_dir / "subsets" / DATASET_ID
    if table_path.exists():
        shutil.rmtree(table_path)

    print(f"  Writing partitioned Delta table from batch files...")
    batch_files = sorted(processed_dir.glob("*.parquet"))
    for i, batch_file in enumerate(batch_files):
        batch_table = pq.read_table(batch_file)
        mode = "overwrite" if i == 0 else "append"
        write_deltalake(
            str(table_path),
            batch_table,
            mode=mode,
            partition_by=["taxonomy"],
            schema_mode="merge",
        )
        print(f"    Written batch {i + 1}/{len(batch_files)}")

    # Validate by reading Delta table metadata (row count) without loading all data
    from deltalake import DeltaTable
    dt = DeltaTable(str(table_path))
    total_rows = sum(f.num_records for f in dt.get_add_actions().to_pylist())
    print(f"  Delta table has {total_rows:,} rows")

    if total_rows < 100000:
        raise ValueError(f"Expected at least 100,000 rows, got {total_rows:,}")

    publish(DATASET_ID, METADATA)
    print(f"  Published {DATASET_ID} partitioned by taxonomy")

    # Clean up intermediate files
    shutil.rmtree(processed_dir)
    print(f"  Cleaned up intermediate batch files")


if __name__ == "__main__":
    run()
