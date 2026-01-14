import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date


def test(table: pa.Table) -> None:
    """Validate SEC EDGAR filings output."""
    validate(table, {
        "columns": {
            "cik": "string",
            "company_name": "string",
            "form_type": "string",
            "filing_date": "string",
            "accession_number": "string",
            "file_number": "string",
            "report_date": "string",
            "is_xbrl": "bool",
            "is_inline_xbrl": "bool",
            "primary_document": "string",
        },
        "not_null": ["cik", "company_name", "form_type", "filing_date", "accession_number"],
        "min_rows": 100000,
    })

    # Filing dates should be valid
    assert_valid_date(table, "filing_date")

    # Should have common form types
    form_types = set(table.column("form_type").to_pylist())
    common_forms = {"10-K", "10-Q", "8-K"}
    found = form_types & common_forms
    assert len(found) >= 3, f"Should include common form types, found {found}"

    # Accession numbers + CIK should form a natural key (though duplicates can exist for amendments)
    # Just verify we have many distinct accession numbers
    accessions = set(table.column("accession_number").to_pylist())
    assert len(accessions) >= 100000, f"Should have many distinct accession numbers, got {len(accessions)}"

    # CIKs should be 10-digit
    ciks = set(table.column("cik").to_pylist())
    assert all(len(c) == 10 for c in ciks), "CIKs should be 10 digits"

    print(f"  Validated {len(table):,} filings")
