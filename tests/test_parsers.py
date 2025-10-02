from ingest_app.app.parsers import generation
import pytest


@pytest.fixture
def xml_content():
    with open("tests/test_data/generation-test-curvetype-A01.xml", "rb") as f:
        return f.read()


@pytest.mark.parametrize(
    "input_filename, expected_record_count, expected_start_time, expected_end_time, expected_psr_type_codes, expected_quantities",
    [
        (
            "generation-test-curvetype-A01-30min.xml",
            5,
            "2025-08-20T00:00:00+00:00",
            "2025-08-20T00:30:00+00:00",
            ["B04", "B05", "B06", "B20", "B19"],
            [80.0, 0.0, 0.0, 0.0, 92.0],
        ),
    ],
)
def test_parse_generation_document(
    input_filename,
    expected_record_count,
    expected_start_time,
    expected_end_time,
    expected_psr_type_codes,
    expected_quantities,
):

    file_path = f"tests/test_data/{input_filename}"    
    with open(file_path, "rb") as f:
        xml_content = f.read()
    events = generation.parse_generation_document(xml_content)

    assert len(events) == expected_record_count

    # Start and End Times
    start_times = {e["start_time"] for e in events}
    end_times = {e["end_time"] for e in events}
    assert len(start_times) == 1
    assert len(end_times) == 1
    assert start_times.pop() == expected_start_time
    assert end_times.pop() == expected_end_time

    # PSR-Type Codes
    psr_type_codes = [e["psr_type_code"] for e in events]
    assert psr_type_codes == expected_psr_type_codes

    # Quantities
    quantities = [e["quantity_mw"] for e in events]
    assert quantities == expected_quantities
