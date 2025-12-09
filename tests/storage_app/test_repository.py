import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from storage_app.app.repository import PostgresRepo

@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = None
    return conn

@pytest.fixture
def repo(mock_conn):
    return PostgresRepo(mock_conn)

@patch("psycopg2.extras.execute_values")
def test_bulk_insert_without_conflicts(mock_execute_values, repo, mock_conn):
    """
    Verifies that bulk_insert correctly formats data and calls execute_values.
    """
    cursor = mock_conn.cursor.return_value
    cursor.rowcount = 2

    table_name = "test_table"
    columns = ["col_a", "col_b"]
    conflict_columns = []
    events = [
        {"col_a": "1", "col_b": 1, "extra_test_field": "this should be excluded from query"},
        {"col_a": "2", "col_b": 2} 
    ]

    count = repo.bulk_insert(table_name, columns, conflict_columns, events)

    # There should be 2 rows returned, and the conn.commit() + execute_values() functions
    #   must've been called
    assert count == 2
    mock_conn.commit.assert_called_once()
    mock_execute_values.assert_called_once()

    # Verify that the query and data tuples within bulk_insert are correct
    sql_query = mock_execute_values.call_args[0][1]
    data_tuples = mock_execute_values.call_args[0][2]
    assert 'INSERT INTO test_table ("col_a", "col_b")' in sql_query
    assert data_tuples == [("1", 1), ("2", 2)]

@patch("psycopg2.extras.execute_values")
def test_bulk_insert_with_conflicts(mock_execute_values, repo, mock_conn):
    """
    Verifies that passing conflict_columns generates the correct ON CONFLICT SQL.
    """
    cursor = mock_conn.cursor.return_value
    cursor.rowcount = 0 

    table_name = "test_table"
    columns = ["col_a", "col_b"]
    conflict_columns = ["col_a", "col_b"]
    events = [{"col_a": "1", "col_b": 1}]

    repo.bulk_insert(table_name, columns, conflict_columns, events)

    # The sql query must include the correct conflict handling
    sql_query = mock_execute_values.call_args[0][1]
    assert 'ON CONFLICT ("col_a", "col_b")' in sql_query
    assert "DO NOTHING" in sql_query

@patch("psycopg2.extras.execute_values")
def test_bulk_insert_rollback_on_error(mock_execute_values, repo, mock_conn):
    """
    Verifies that the transaction is rolled back upon a db error.
    """
    mock_execute_values.side_effect = Exception("Connection lost")

    with pytest.raises(Exception) as error:
        repo.bulk_insert("test_table", ["col"], [], [{"col": 1}])
    
    assert "Connection lost" in str(error.value)

    # Verify that rollback() was called, and that commit() wasn't called
    mock_conn.rollback.assert_called_once()
    mock_conn.commit.assert_not_called()

def test_bulk_insert_empty_list(repo, mock_conn):
    """
    Verifies that an empty list returns 0 without any further execution.
    """
    count = repo.bulk_insert("test_table", ["col"], [], [])

    assert count == 0

    mock_conn.cursor.assert_not_called()
