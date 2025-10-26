from lxml import etree
import pytest

@pytest.fixture
def xml_period():
    with open("tests/test_data/A03-1day-period.xml") as fb:
        period_tree = etree.parse(fb)
        period = period_tree.xpath("//Period")[0]
    return period

def test_period(xml_period):
    print(xml_period)
    assert xml_period != None