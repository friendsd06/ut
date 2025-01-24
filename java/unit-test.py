import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession, DataFrame
from warehouse_reader import WarehouseReader, WarehouseError

@pytest.fixture
def mock_spark():
    return Mock(spec=SparkSession)

@pytest.fixture
def mock_token_service():
    service = Mock()
    service.get_token.return_value = "test_token"
    return service

@pytest.fixture
def test_config():
    return {
        "hostname": "test.host",
        "port": "443",
        "ssl": "1",
        "catalog": "test_catalog"
    }

@pytest.fixture
def reader(test_config, mock_token_service):
    return WarehouseReader(test_config, mock_token_service)

class TestWarehouseReader:
    def test_build_jdbc_url(self, reader):
        expected = "jdbc:databricks://test.host:443;ssl=1;catalog=test_catalog"
        assert reader.jdbc_url == expected

    def test_build_jdbc_url_no_params(self):
        config = {"hostname": "test.host", "port": "443"}
        reader = WarehouseReader(config, Mock())
        assert reader.jdbc_url == "jdbc:databricks://test.host:443"

    def test_build_jdbc_url_error(self):
        with pytest.raises(WarehouseError):
            WarehouseReader({}, Mock())

    def test_execute_query(self, reader, mock_spark, mock_token_service):
        mock_reader = Mock()
        mock_spark.read.format.return_value.option.return_value.option.return_value = mock_reader
        mock_reader.load.return_value = Mock(spec=DataFrame)

        result = reader.execute_query(mock_spark, "SELECT 1")

        assert mock_token_service.get_token.called
        assert mock_reader.option.called_with("query", "SELECT 1")
        assert isinstance(result, DataFrame)

    def test_execute_query_with_options(self, reader, mock_spark):
        options = {"fetchsize": "1000"}
        reader.execute_query(mock_spark, "SELECT 1", options)

        mock_spark.read.format.return_value.option.return_value.option.assert_any_call("fetchsize", "1000")

    def test_execute_query_error(self, reader, mock_spark):
        mock_spark.read.format.side_effect = Exception("Query failed")

        with pytest.raises(WarehouseError, match="Query execution failed"):
            reader.execute_query(mock_spark, "SELECT 1")