"""Download and transform SEC XBRL Company Facts.

Fetches XBRL facts for all companies and transforms into edgar_xbrl_facts dataset.
Uses batched processing for memory efficiency.
"""

import pyarrow as pa
import pyarrow.compute as pc
from tqdm import tqdm
from subsets_utils import (
    save_raw_json, load_raw_json,
    load_state, save_state,
    merge, publish,
)
from connector_utils import rate_limited_get

SEC_BASE_URL = "https://data.sec.gov"

DATASET_ID = "edgar_xbrl_facts"
BATCH_SIZE = 500

METADATA = {
    "id": DATASET_ID,
    "title": "SEC EDGAR XBRL Financial Facts",
    "description": "Structured financial data from SEC XBRL filings. Contains standardized financial metrics (revenue, assets, liabilities, etc.) extracted from company filings using XBRL taxonomies (us-gaap, dei, srt, etc.).",
    "license": "Public Domain (SEC EDGAR data is public and freely available)",
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


def download():
    """Fetch XBRL facts for all companies with incremental caching."""
    companies = load_raw_json("company_tickers")
    print(f"  Loaded {len(companies):,} companies from ticker index")

    state = load_state("xbrl_facts")
    completed = set(state.get("completed", []))

    pending = [c for c in companies if c["cik_str"] not in completed]

    if not pending:
        print("  All XBRL facts already fetched")
        return

    print(f"  Fetching XBRL facts for {len(pending):,} companies ({len(completed):,} cached)...")

    errors = 0
    no_data = 0
    with tqdm(pending, desc="XBRL Facts", unit="co", ncols=100) as pbar:
        for company in pbar:
            cik = str(company["cik_str"]).zfill(10)
            ticker = company.get("ticker", "")

            pbar.set_postfix_str(f"{ticker or cik[:8]}", refresh=False)

            url = f"{SEC_BASE_URL}/api/xbrl/companyfacts/CIK{cik}.json"

            try:
                response = rate_limited_get(url)
                response.raise_for_status()
                data = response.json()
                save_raw_json(data, f"xbrl_facts/{cik}", compress=True)
            except Exception as e:
                # Many companies don't have XBRL data (404) - that's expected
                if "404" in str(e):
                    no_data += 1
                    save_raw_json({"no_data": True, "cik": cik}, f"xbrl_facts/{cik}")
                else:
                    errors += 1
                    save_raw_json({"error": str(e), "cik": cik}, f"xbrl_facts/{cik}")

            completed.add(company["cik_str"])
            save_state("xbrl_facts", {"completed": sorted(completed)})

    if no_data > 0:
        print(f"  {no_data} companies have no XBRL data (expected)")
    if errors > 0:
        print(f"  Encountered {errors} errors during fetch")


def _extract_company_facts(cik: str) -> list[dict]:
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


def _merge_batch(table: pa.Table, key: list[str]) -> pa.Table:
    """Filter out rows with null merge keys and merge."""
    # fiscal_year/fiscal_period can be null in SEC data (non-fiscal-period facts).
    # Merge keys cannot contain nulls, so drop those rows before merging.
    mask = pc.and_(pc.is_valid(table["fiscal_year"]), pc.is_valid(table["fiscal_period"]))
    filtered = table.filter(mask)
    dropped = len(table) - len(filtered)
    if dropped > 0:
        print(f"      Dropped {dropped:,} rows with null fiscal_year/fiscal_period")
    if len(filtered) > 0:
        merge(filtered, DATASET_ID, key=key)
    return filtered


def transform():
    """Transform XBRL facts with memory-efficient batching."""
    tickers = load_raw_json("company_tickers")
    print(f"  Processing XBRL facts from {len(tickers):,} companies in batches of {BATCH_SIZE}...")

    merge_key = ["cik", "taxonomy", "concept", "unit", "end_date", "fiscal_year", "fiscal_period"]

    batch_records = []
    batch_num = 0
    companies_processed = 0
    companies_with_data = 0
    total_facts = 0
    total_dropped = 0
    errors = 0

    for ticker_entry in tickers:
        cik = str(ticker_entry["cik_str"]).zfill(10)

        try:
            records = _extract_company_facts(cik)
        except FileNotFoundError:
            errors += 1
            companies_processed += 1
            continue

        if records:
            batch_records.extend(records)
            companies_with_data += 1

        companies_processed += 1

        # Write batch when we hit batch size
        if companies_processed % BATCH_SIZE == 0 and batch_records:
            table = pa.Table.from_pylist(batch_records, schema=SCHEMA)
            filtered = _merge_batch(table, merge_key)
            total_facts += len(filtered)
            total_dropped += len(table) - len(filtered)
            print(f"    Batch {batch_num}: {len(filtered):,} facts from {BATCH_SIZE} companies ({companies_processed:,}/{len(tickers):,})")
            batch_num += 1
            batch_records.clear()

    # Write final partial batch
    if batch_records:
        table = pa.Table.from_pylist(batch_records, schema=SCHEMA)
        filtered = _merge_batch(table, merge_key)
        total_facts += len(filtered)
        total_dropped += len(table) - len(filtered)
        print(f"    Batch {batch_num}: {len(filtered):,} facts (final batch)")

    if errors > 0:
        print(f"  Skipped {errors} companies with missing XBRL data")
    if total_dropped > 0:
        print(f"  Dropped {total_dropped:,} facts with null fiscal_year/fiscal_period")
    print(f"  Wrote {total_facts:,} total facts from {companies_with_data:,} companies")

    assert total_facts >= 1000000, f"Expected >= 1,000,000 XBRL facts, got {total_facts:,}"

    publish(DATASET_ID, METADATA)
    print(f"  Published {DATASET_ID}")


from nodes.company_tickers import download as download_tickers

NODES = {
    download: [download_tickers],
    transform: [download],
}


if __name__ == "__main__":
    download()
    transform()
