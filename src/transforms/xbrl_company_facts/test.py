import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date


def test(table: pa.Table) -> None:
    """Validate SEC XBRL company facts output."""
    validate(table, {
        "columns": {
            "cik": "string",
            "entity_name": "string",
            "taxonomy": "string",
            "concept": "string",
            "label": "string",
            "unit": "string",
            "value": "string",
            "end_date": "string",
            "fiscal_year": "int",
            "fiscal_period": "string",
            "form": "string",
            "filed": "string",
            "accession": "string",
        },
        "not_null": ["cik", "entity_name", "taxonomy", "concept", "unit", "end_date", "form", "filed", "accession"],
        "min_rows": 100000,
    })

    # End dates and filed dates should be valid
    assert_valid_date(table, "end_date")
    assert_valid_date(table, "filed")

    # Should include common taxonomies
    taxonomies = set(table.column("taxonomy").to_pylist())
    assert "us-gaap" in taxonomies or "dei" in taxonomies, f"Should include us-gaap or dei taxonomy, got {taxonomies}"

    # Should have many concepts
    concepts = set(table.column("concept").to_pylist())
    assert len(concepts) >= 100, f"Should have many concepts, got {len(concepts)}"

    # Should have common units
    units = set(table.column("unit").to_pylist())
    assert "USD" in units, f"Should have USD unit, got {units}"

    # CIKs should be 10-digit
    ciks = set(table.column("cik").to_pylist())
    assert all(len(c) == 10 for c in ciks), "CIKs should be 10 digits"

    print(f"  Validated {len(table):,} XBRL facts")
