"""
Pytest Configuration and Fixtures for Databricks Medallion Architecture
Capital Markets Post-Trade Settlement

Location 5a-5c: Test validation framework setup
"""

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import tempfile
import shutil
import os

@pytest.fixture(scope="session")
def spark_session():
    """
    Create Spark session for testing (Location 5a).
    
    Returns:
        SparkSession: Test Spark session with Delta Lake support
    """
    spark = SparkSession.builder \
        .appName("test_capital_markets") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()

@pytest.fixture
def sample_trades_data(spark_session):
    """
    Create sample trade data for testing (Location 5a).
    
    Args:
        spark_session (SparkSession): Test Spark session
        
    Returns:
        DataFrame: Sample trades DataFrame
    """
    # Create sample trade data
    trades_data = [
        ("T001", "INST001", "BankA", 1000000.0, "usd", "2023-01-15 09:30:00", "2023-01-17 09:30:00", "SETTLED"),
        ("T002", "INST002", "BankB", 2500000.0, "EUR", "2023-01-15 10:15:00", "2023-01-17 10:15:00", "PENDING"),
        ("T003", "INST001", "BankA", 750000.0, "gbp", "2023-01-15 11:00:00", "2023-01-17 11:00:00", "SETTLED"),
        ("T004", "INST003", "BankC", 5000000.0, "USD", "2023-01-16 09:45:00", "2023-01-18 09:45:00", "PENDING"),
        ("T005", "INST002", "BankB", 1500000.0, "eur", "2023-01-16 14:20:00", "2023-01-18 14:20:00", "SETTLED"),
        # Duplicate record for testing deduplication
        ("T001", "INST001", "BankA", 1000000.0, "USD", "2023-01-15 09:30:00", "2023-01-17 09:30:00", "SETTLED"),
    ]
    
    schema = StructType([
        StructField("TradeID", StringType(), nullable=False),
        StructField("InstrumentID", StringType(), nullable=False),
        StructField("Counterparty", StringType(), nullable=True),
        StructField("Notional", DoubleType(), nullable=True),
        StructField("Currency", StringType(), nullable=False),
        StructField("TradeDate", TimestampType(), nullable=True),
        StructField("SettlementDate", TimestampType(), nullable=True),
        StructField("Status", StringType(), nullable=True)
    ])
    
    df = spark_session.createDataFrame(trades_data, schema)
    return df

@pytest.fixture
def sample_instruments_data(spark_session):
    """
    Create sample instrument data for testing.
    
    Args:
        spark_session (SparkSession): Test Spark session
        
    Returns:
        DataFrame: Sample instruments DataFrame
    """
    instruments_data = [
        ("INST001", "Equity", "Apple Inc.", "AAPL", "NASDAQ"),
        ("INST002", "Bond", "US Treasury 10Y", "US10Y", "UST"),
        ("INST003", "FX Forward", "EUR/USD", "EURUSD", "FX"),
        ("INST004", "Equity", "Microsoft Corp.", "MSFT", "NASDAQ"),
        ("INST005", "Commodity", "Gold Futures", "XAU", "CME"),
    ]
    
    schema = StructType([
        StructField("InstrumentID", StringType(), nullable=False),
        StructField("InstrumentType", StringType(), nullable=True),
        StructField("InstrumentName", StringType(), nullable=True),
        StructField("Ticker", StringType(), nullable=True),
        StructField("Exchange", StringType(), nullable=True)
    ])
    
    df = spark_session.createDataFrame(instruments_data, schema)
    return df

@pytest.fixture
def temp_delta_path():
    """
    Create temporary directory for Delta tables during testing.
    
    Returns:
        str: Temporary directory path
    """
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)

@pytest.fixture
def test_config():
    """
    Create test configuration dictionary.
    
    Returns:
        dict: Test configuration
    """
    return {
        'delta_lake': {
            'bronze_path': '/tmp/test/bronze/',
            'silver_path': '/tmp/test/silver/',
            'gold_path': '/tmp/test/gold/'
        },
        'source_to_target_mapping': {
            'trades': {
                'source_format': 'json',
                'source_path': '/tmp/test/landing/trades/',
                'bronze_table': 'bronze_trades',
                'silver_table': 'silver_trades',
                'gold_table': 'gold_trade_aggregates'
            },
            'instruments': {
                'source_format': 'csv',
                'source_path': '/tmp/test/landing/instruments/',
                'bronze_table': 'bronze_instruments',
                'silver_table': 'silver_instruments'
            }
        },
        'spark': {
            'app_name': 'test_capital_markets',
            'master': 'local[2]',
            'config': {
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
            }
        },
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        }
    }
