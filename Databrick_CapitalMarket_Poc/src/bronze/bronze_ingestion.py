"""
Bronze Layer Data Ingestion for Databricks Medallion Architecture
Capital Markets Post-Trade Settlement

Location 2a: Read Raw Trade Data - Ingest JSON trade data from landing zone
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class BronzeIngestion:
    """Handles raw data ingestion from landing zone to bronze Delta tables."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize Bronze Ingestion with Spark session and configuration.
        
        Args:
            spark (SparkSession): Active Spark session
            config (Dict[str, Any]): Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.delta_config = config.get('delta_lake', {})
        self.source_mapping = config.get('source_to_target_mapping', {})
        
    def ingest_trades_data(self) -> DataFrame:
        """
        Ingest raw trade data from landing zone (Location 2a).
        
        Returns:
            DataFrame: Ingested trades data with metadata
            
        Process:
        1. Read JSON format trade data from landing zone
        2. Validate schema against expected structure
        3. Add processing metadata
        4. Return DataFrame for further processing
        """
        try:
            trades_config = self.source_mapping.get('trades', {})
            source_path = trades_config.get('source_path', '/data/landing/trades/')
            bronze_table = trades_config.get('bronze_table', 'bronze_trades')
            
            logger.info(f"Starting trades ingestion from {source_path}")
            
            # Define expected schema for trade data
            trades_schema = StructType([
                StructField("TradeID", StringType(), nullable=False),
                StructField("InstrumentID", StringType(), nullable=False),
                StructField("Counterparty", StringType(), nullable=True),
                StructField("Notional", DoubleType(), nullable=True),
                StructField("Currency", StringType(), nullable=False),
                StructField("TradeDate", TimestampType(), nullable=True),
                StructField("SettlementDate", TimestampType(), nullable=True),
                StructField("Status", StringType(), nullable=True)
            ])
            
            # Read raw JSON trade data (Location 2a)
            raw_trades_df = self.spark.read.json(
                source_path,
                schema=trades_schema,
                mode="PERMISSIVE"
            )
            
            logger.info(f"Read {raw_trades_df.count()} raw trade records")
            
            # Validate schema and add processing metadata
            validated_trades_df = self._validate_trades_schema(raw_trades_df)
            
            # Add ingestion metadata
            enriched_trades_df = validated_trades_df.withColumn(
                "ingestion_timestamp", 
                current_timestamp()
            ).withColumn(
                "ingestion_source", 
                lit("landing_zone_json")
            ).withColumn(
                "processing_batch_id", 
                lit(f"batch_{current_timestamp().cast('string')}")
            )
            
            logger.info("Trade data validation and enrichment completed")
            return enriched_trades_df
            
        except Exception as e:
            logger.error(f"Error during trades ingestion: {e}")
            raise
    
    def ingest_instruments_data(self) -> DataFrame:
        """
        Ingest instrument reference data from landing zone (Location 2c).
        
        Returns:
            DataFrame: Ingested instruments data with metadata
            
        Process:
        1. Read CSV format instrument data with headers
        2. Apply basic validation
        3. Add processing metadata
        4. Return DataFrame for further processing
        """
        try:
            instruments_config = self.source_mapping.get('instruments', {})
            source_path = instruments_config.get('source_path', '/data/landing/instruments/')
            bronze_table = instruments_config.get('bronze_table', 'bronze_instruments')
            
            logger.info(f"Starting instruments ingestion from {source_path}")
            
            # Read CSV instrument data with headers (Location 2c)
            raw_instruments_df = self.spark.read.csv(
                source_path,
                header=True,
                inferSchema=True,
                mode="PERMISSIVE"
            )
            
            logger.info(f"Read {raw_instruments_df.count()} instrument records")
            
            # Apply basic validation
            validated_instruments_df = self._validate_instruments_data(raw_instruments_df)
            
            # Add ingestion metadata
            enriched_instruments_df = validated_instruments_df.withColumn(
                "ingestion_timestamp", 
                current_timestamp()
            ).withColumn(
                "ingestion_source", 
                lit("landing_zone_csv")
            )
            
            logger.info("Instrument data validation and enrichment completed")
            return enriched_instruments_df
            
        except Exception as e:
            logger.error(f"Error during instruments ingestion: {e}")
            raise
    
    def write_to_bronze_delta(self, df: DataFrame, table_name: str) -> None:
        """
        Write DataFrame to bronze Delta table (Location 2b).
        
        Args:
            df (DataFrame): DataFrame to write
            table_name (str): Target bronze table name
            
        Process:
        1. Write to Delta table in overwrite mode for idempotency
        2. Create Delta table if not exists
        3. Log processing statistics
        """
        try:
            bronze_path = f"{self.delta_config.get('bronze_path', '/delta/bronze/')}{table_name}"
            
            logger.info(f"Writing {df.count()} records to bronze Delta table: {table_name}")
            
            # Write to Bronze Delta Table (Location 2b)
            df.write.format("delta") \
              .mode("overwrite") \
              .option("overwriteSchema", "true") \
              .save(bronze_path)
            
            logger.info(f"Successfully wrote data to bronze Delta table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error writing to bronze Delta table {table_name}: {e}")
            raise
    
    def _validate_trades_schema(self, df: DataFrame) -> DataFrame:
        """
        Validate trades DataFrame schema and data quality.
        
        Args:
            df (DataFrame): Raw trades DataFrame
            
        Returns:
            DataFrame: Validated trades DataFrame
        """
        # Check for required fields
        required_fields = ["TradeID", "InstrumentID", "Currency"]
        for field in required_fields:
            if field not in df.columns:
                raise ValueError(f"Required field '{field}' missing from trades data")
        
        # Filter out records with null required fields
        validated_df = df.filter(
            col("TradeID").isNotNull() &
            col("InstrumentID").isNotNull() &
            col("Currency").isNotNull()
        )
        
        null_count = df.count() - validated_df.count()
        if null_count > 0:
            logger.warning(f"Filtered out {null_count} records with null required fields")
        
        return validated_df
    
    def _validate_instruments_data(self, df: DataFrame) -> DataFrame:
        """
        Validate instruments DataFrame and apply basic validation.
        
        Args:
            df (DataFrame): Raw instruments DataFrame
            
        Returns:
            DataFrame: Validated instruments DataFrame
        """
        # Basic validation - ensure we have instrument identifier
        if "InstrumentID" not in df.columns and "instrument_id" not in df.columns:
            raise ValueError("Instrument identifier field missing from instruments data")
        
        # Standardize column names if needed
        if "instrument_id" in df.columns:
            df = df.withColumnRenamed("instrument_id", "InstrumentID")
        
        # Filter out records with null InstrumentID
        validated_df = df.filter(col("InstrumentID").isNotNull())
        
        null_count = df.count() - validated_df.count()
        if null_count > 0:
            logger.warning(f"Filtered out {null_count} instrument records with null InstrumentID")
        
        return validated_df
    
    def run_bronze_ingestion(self) -> None:
        """
        Run complete bronze layer ingestion process.
        
        Process:
        1. Ingest trades data
        2. Ingest instruments data
        3. Write both to bronze Delta tables
        4. Log processing statistics
        """
        try:
            logger.info("Starting bronze layer ingestion process")
            
            # Ingest trades data
            trades_df = self.ingest_trades_data()
            self.write_to_bronze_delta(trades_df, "bronze_trades")
            
            # Ingest instruments data
            instruments_df = self.ingest_instruments_data()
            self.write_to_bronze_delta(instruments_df, "bronze_instruments")
            
            logger.info("Bronze layer ingestion completed successfully")
            
        except Exception as e:
            logger.error(f"Bronze ingestion process failed: {e}")
            raise
