import pytest
import requests
from unittest.mock import MagicMock
from ingest_app.app.api.client import EntsoeClient
from ingest_app.app.exceptions import InvalidIntervalError, NoDataFoundError

@pytest.fixture()
def mock_session():
    return MagicMock(spec=requests.Session)

@pytest.fixture()
def client(mock_session):
    c = EntsoeClient(api_key="api_key")
    c._session = mock_session
    return c

def test_get_data_happy_path(client, mock_session):
    """
    Verifies that parameters are passed correctly and content is returned
    when the API behaves normally.
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"<xml>test_data</xml>"
    mock_response.text = "<xml>test_data</xml>"

    mock_session.get.return_value = mock_response

    params = {"paramA": "a", "paramB": "b"}

    client.get_data(params)

    # The params must be passed correctly to the session's get() method
    request_params = mock_session.get.call_args[1]['params']
    print(request_params)
    assert request_params["securityToken"] == client._api_key
    assert request_params["paramA"] == params["paramA"]
    assert request_params["paramB"] == params["paramB"]

def test_get_data_invalid_interval(client, mock_session):
    """
    Verifies that InvalidIntervalError is raised if API returns the specific error text
    about the time interval being too short.
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "Delivered time interval is not valid for this code"
    mock_session.get.return_value = mock_response

    with pytest.raises(InvalidIntervalError):
        client.get_data({})

def test_get_data_no_matching_data(client, mock_session):
    """
    Verifies that NoDataFoundError is raised if API returns the specific error text
    indicating no data exists for that query.
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "No matching data found for this query"
    mock_session.get.return_value = mock_response

    with pytest.raises(NoDataFoundError):
        client.get_data({})

def test_get_data_http_error(client, mock_session):
    """
    Verifies that the client raises HTTP errors.
    """
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.raise_for_status.side_effect = requests.HTTPError("Unauthorized")
    mock_session.get.return_value = mock_response

    with pytest.raises(requests.HTTPError):
        client.get_data({})