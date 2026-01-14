import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate SEC companies output."""
    validate(table, {
        "columns": {
            "cik": "string",
            "ticker": "string",
            "name": "string",
            "sic_code": "string",
            "sic_description": "string",
            "state_of_incorporation": "string",
            "fiscal_year_end": "string",
            "entity_type": "string",
            "ein": "string",
            "exchanges": "list",
        },
        "not_null": ["cik", "name"],
        "min_rows": 5000,
    })

    # CIKs should be unique
    ciks = table.column("cik").to_pylist()
    assert len(ciks) == len(set(ciks)), "CIKs should be unique"

    # CIKs should be 10-digit zero-padded
    assert all(len(c) == 10 for c in ciks), "CIKs should be 10 digits"

    # Should include some known companies (we may not have all of them in a partial run)
    tickers = set(t for t in table.column("ticker").to_pylist() if t)
    common_tickers = {"AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM"}
    found = tickers & common_tickers
    assert len(found) >= 1, f"Should include at least one major company, found {found}"

    # Should have diverse SIC codes
    sic_codes = set(s for s in table.column("sic_code").to_pylist() if s)
    assert len(sic_codes) >= 100, f"Should have diverse industries, got {len(sic_codes)} SIC codes"

    print(f"  Validated {len(table):,} companies")
