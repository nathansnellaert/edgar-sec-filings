#!/usr/bin/env python3
"""Fetch and process SEC XBRL Company Facts data."""

import os
import logging
from datetime import datetime
import pyarrow as pa
from ratelimit import limits, sleep_and_retry
from utils.http_client import get
from utils.io import load_state, save_state

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SEC API configuration
SEC_BASE_URL = "https://data.sec.gov"

def get_user_agent():
    """Get user agent from environment variable."""
    return os.environ["SEC_USER_AGENT"]


@sleep_and_retry
@limits(calls=10, period=1)  # SEC rate limit: 10 requests per second
def fetch_company_facts(cik):
    """Fetch XBRL company facts from SEC for a given CIK."""
    cik_str = str(cik).zfill(10)
    url = f"{SEC_BASE_URL}/api/xbrl/companyfacts/CIK{cik_str}.json"
    
    headers = {
        "User-Agent": get_user_agent(),
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
        "Connection": "keep-alive"
    }
    
    try:
        response = get(url, headers=headers, timeout=60.0)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.warning(f"Failed to fetch company facts for CIK {cik_str}: {e}")
        return None


def process_xbrl_facts(tickers_data):
    """Process SEC XBRL Company Facts for all companies."""
    logger.info("Processing XBRL Company Facts...")
    
    # Define schema for flattened facts
    schema = pa.schema([
        pa.field("cik", pa.string(), nullable=False),
        pa.field("entity_name", pa.string(), nullable=False),
        pa.field("ticker", pa.string(), nullable=True),
        pa.field("taxonomy", pa.string(), nullable=False),
        pa.field("concept", pa.string(), nullable=False),
        pa.field("label", pa.string(), nullable=True),
        pa.field("description", pa.string(), nullable=True),
        pa.field("unit", pa.string(), nullable=False),
        pa.field("value", pa.float64(), nullable=True),
        pa.field("start_date", pa.date32(), nullable=True),
        pa.field("end_date", pa.date32(), nullable=False),
        pa.field("instant_date", pa.date32(), nullable=True),
        pa.field("period_type", pa.string(), nullable=False),
        pa.field("fiscal_year", pa.int32(), nullable=True),
        pa.field("fiscal_period", pa.string(), nullable=True),
        pa.field("form", pa.string(), nullable=False),
        pa.field("filed", pa.date32(), nullable=False),
        pa.field("accession", pa.string(), nullable=False),
        pa.field("fact_year", pa.int32(), nullable=False),
        pa.field("fact_quarter", pa.string(), nullable=False),
        pa.field("fact_month", pa.string(), nullable=False)
    ])
    
    # If no tickers data, return empty table
    if tickers_data.num_rows == 0:
        logger.info("No tickers data provided, returning empty facts table")
        return pa.Table.from_pylist([], schema=schema)
    
    # Load state to track last update per company
    state = load_state("xbrl_company_facts")
    last_updates = state.get("last_updates", {})
    
    all_facts = []
    processed_count = 0
    new_last_updates = {}
    
    # Process each company
    for batch in tickers_data.to_batches():
        for row in batch.to_pylist():
            cik = row["cik_str"]
            ticker = row["ticker"]
            
            # Get last update date for this company
            last_update = last_updates.get(cik)
            if last_update:
                last_update_dt = datetime.fromisoformat(last_update)
            else:
                last_update_dt = datetime(1900, 1, 1)  # Process all if no previous state
            
            logger.info(f"Processing XBRL facts for {ticker} (CIK: {cik})")
            
            # Fetch company facts data
            facts_data = fetch_company_facts(cik)
            if not facts_data:
                continue
            
            entity_name = facts_data.get("entityName", "")
            facts = facts_data.get("facts", {})
            
            # Track the most recent filing date for this company
            most_recent_filing = last_update_dt
            
            # Process facts from each taxonomy
            for taxonomy, concepts in facts.items():
                if not isinstance(concepts, dict):
                    continue
                
                for concept, concept_data in concepts.items():
                    if not isinstance(concept_data, dict):
                        continue
                    
                    label = concept_data.get("label", "")
                    description = concept_data.get("description", "")
                    units_data = concept_data.get("units", {})
                    
                    # Process each unit type
                    for unit_type, unit_facts in units_data.items():
                        if not isinstance(unit_facts, list):
                            continue
                        
                        for fact in unit_facts:
                            if not isinstance(fact, dict):
                                continue
                            
                            # Parse filing date
                            filed_str = fact.get("filed")
                            if not filed_str:
                                continue
                            
                            try:
                                filed_dt = datetime.strptime(filed_str, "%Y-%m-%d")
                            except:
                                continue
                            
                            # Skip if this fact is older than our last update
                            if filed_dt <= last_update_dt:
                                continue
                            
                            # Update most recent filing date
                            if filed_dt > most_recent_filing:
                                most_recent_filing = filed_dt
                            
                            # Parse dates
                            start_date = None
                            end_date = None
                            instant_date = None
                            period_type = "instant"
                            
                            if "start" in fact and "end" in fact:
                                period_type = "duration"
                                try:
                                    start_date = datetime.strptime(fact["start"], "%Y-%m-%d").date()
                                    end_date = datetime.strptime(fact["end"], "%Y-%m-%d").date()
                                except:
                                    continue
                            elif "instant" in fact:
                                period_type = "instant"
                                try:
                                    instant_date = datetime.strptime(fact["instant"], "%Y-%m-%d").date()
                                    end_date = instant_date  # Use instant as end_date for consistency
                                except:
                                    continue
                            elif "end" in fact:
                                # Some facts only have end date
                                try:
                                    end_date = datetime.strptime(fact["end"], "%Y-%m-%d").date()
                                except:
                                    continue
                            else:
                                continue
                            
                            # Determine fact date for time-based grouping
                            fact_date = end_date if end_date else instant_date
                            if not fact_date:
                                continue
                            
                            fact_record = {
                                "cik": cik,
                                "entity_name": entity_name,
                                "ticker": ticker,
                                "taxonomy": taxonomy,
                                "concept": concept,
                                "label": label,
                                "description": description,
                                "unit": unit_type,
                                "value": fact.get("val"),
                                "start_date": start_date,
                                "end_date": end_date,
                                "instant_date": instant_date,
                                "period_type": period_type,
                                "fiscal_year": fact.get("fy"),
                                "fiscal_period": fact.get("fp"),
                                "form": fact.get("form", ""),
                                "filed": filed_dt.date(),
                                "accession": fact.get("accn", ""),
                                "fact_year": fact_date.year,
                                "fact_quarter": f"{fact_date.year}-Q{(fact_date.month - 1) // 3 + 1}",
                                "fact_month": f"{fact_date.year}-{fact_date.month:02d}"
                            }
                            
                            all_facts.append(fact_record)
            
            # Update last update date for this company
            if most_recent_filing > last_update_dt:
                new_last_updates[cik] = most_recent_filing.isoformat()
            else:
                new_last_updates[cik] = last_updates.get(cik, datetime(1900, 1, 1).isoformat())
            
            processed_count += 1
            if processed_count % 10 == 0:
                logger.info(f"Processed {processed_count} companies, found {len(all_facts)} new facts")
    
    # Save updated state
    save_state("xbrl_company_facts", {
        "last_updates": new_last_updates,
        "last_run": datetime.utcnow().isoformat()
    })
    
    logger.info(f"Processed {len(all_facts)} new XBRL facts")
    
    # Create PyArrow table
    table = pa.Table.from_pylist(all_facts, schema=schema)
    return table