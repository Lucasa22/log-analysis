import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession

from log_analyzer.core.io_common import IOCommon

def test_io_common_initialization_with_spark_session():
    """
    Tests that IOCommon initializes correctly when a SparkSession is provided.
    """
    mock_spark_session = MagicMock(spec=SparkSession)
    io_instance = IOCommon(spark_session=mock_spark_session)
    assert io_instance.spark is mock_spark_session, "Spark session should be stored."

def test_io_common_initialization_without_spark_session():
    """
    Tests that IOCommon initializes correctly when no SparkSession is provided.
    """
    io_instance = IOCommon()
    assert io_instance.spark is None, "Spark session should default to None."

def test_io_common_read_raises_not_implemented_error():
    """
    Tests that calling the read method on IOCommon raises NotImplementedError.
    """
    io_instance = IOCommon()
    with pytest.raises(NotImplementedError):
        io_instance.read(source="any_source")

def test_io_common_write_raises_not_implemented_error():
    """
    Tests that calling the write method on IOCommon raises NotImplementedError.
    """
    io_instance = IOCommon()
    with pytest.raises(NotImplementedError):
        io_instance.write(data="any_data", destination="any_destination")
