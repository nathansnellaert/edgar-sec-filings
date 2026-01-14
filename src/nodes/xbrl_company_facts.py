"""Ingest and transform SEC XBRL Company Facts.

Fetches XBRL facts for all companies and transforms into edgar_xbrl_facts dataset.
Memory-efficient batched processing.
"""

import os

# SEC requires email in User-Agent per their fair access policy
if 'HTTP_USER_AGENT' not in os.environ:
    os.environ['HTTP_USER_AGENT'] = os.getenv('SEC_USER_AGENT', 'DataIntegrations/1.0 (admin@dataintegrations.io)')

import pyarrow as pa
from tqdm import tqdm
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state, upload_data
from utils import rate_limited_get
from _transforms.xbrl_company_facts.test import test

SEC_BASE_URL = "https://data.sec.gov"

DATASET_ID = "edgar_xbrl_facts"
BATCH_SIZE = 500

METADATA = {
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


def ingest():
    """Fetch XBRL facts for all companies with incremental caching."""
    # Load the ticker index
    companies = load_raw_json("company_tickers")
    print(f"  Loaded {len(companies):,} companies from ticker index")

    # Track which companies we've already fetched
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

                # Save per-company with compression (XBRL data is large)
                save_raw_json(data, f"xbrl_facts/{cik}", compress=True)

            except Exception as e:
                # Many companies don't have XBRL data (404) - that's expected
                if "404" in str(e):
                    no_data += 1
                    save_raw_json({"no_data": True, "cik": cik}, f"xbrl_facts/{cik}")
                else:
                    errors += 1
                    save_raw_json({"error": str(e), "cik": cik}, f"xbrl_facts/{cik}")

            # Update state after each save
            completed.add(company["cik_str"])
            save_state("xbrl_facts", {"completed": sorted(completed)})

    if no_data > 0:
        print(f"  {no_data} companies have no XBRL data (expected)")
    if errors > 0:
        print(f"  Encountered {errors} errors during fetch")


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


def transform():
    """Transform XBRL facts with memory-efficient batching."""
    # Load ticker index to get list of all companies
    tickers = load_raw_json("company_tickers")
    print(f"  Processing XBRL facts from {len(tickers):,} companies in batches of {BATCH_SIZE}...")

    batch_records = []
    batch_num = 0
    companies_processed = 0
    companies_with_data = 0
    total_facts = 0
    errors = 0
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
                upload_data(table, DATASET_ID, mode=mode)
                total_facts += len(batch_records)
                print(f"    Batch {batch_num}: {len(batch_records):,} facts from {BATCH_SIZE} companies ({companies_processed:,}/{len(tickers):,})")
                batch_num += 1
                batch_records.clear()
                first_batch = False

    # Write final partial batch
    if batch_records:
        table = pa.Table.from_pylist(batch_records, schema=SCHEMA)
        mode = "overwrite" if first_batch else "append"
        upload_data(table, DATASET_ID, mode=mode)
        total_facts += len(batch_records)
        print(f"    Batch {batch_num}: {len(batch_records):,} facts (final batch)")

    if errors > 0:
        print(f"  Skipped {errors} companies with missing XBRL data")
    print(f"  Wrote {total_facts:,} total facts from {companies_with_data:,} companies")

    test(total_facts)
    print(f"  Published {DATASET_ID}")


def run():
    """Ingest and transform XBRL company facts."""
    print("\n--- XBRL Company Facts ---")
    ingest()
    transform()


from nodes.company_tickers import run as company_tickers_run

NODES = {
    run: [company_tickers_run],
}


if __name__ == "__main__":
    run()
