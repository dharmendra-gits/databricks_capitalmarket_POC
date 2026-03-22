"""
Silver Layer Data Cleansing for Databricks Medallion Architecture
Capital Markets Post-Trade Settlement

Location 3a-3d: Data quality improvements and normalization in silver layer
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col, upper, row_number, current_timestamp, lit,
    to_timestamp, trim, when, regexp_replace
)
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class SilverCleansing:
    """Handles data quality improvements and normalization in silver layer."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize Silver Cleansing with Spark session and configuration.
        
        Args:
            spark (SparkSession): Active Spark session
            config (Dict[str, Any]): Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.delta_config = config.get('delta_lake', {})
        self.source_mapping = config.get('source_to_target_mapping', {})
        
    def cleanse_trades_data(self) -> DataFrame:
        """
        Perform complete silver layer cleansing for trades data.
        
        Returns:
            DataFrame: Cleansed trades data ready for gold layer
            
        Process:
        1. Load Bronze Data (Location 3a)
        2. Apply Currency Normalization (Location 3b)
        3. Remove Duplicate Records (Location 3c)
        4. Format Timestamp Fields (Location 3d)
        5. Add processing metadata
        """
        try:
            logger.info("Starting silver layer data cleansing for trades")
            
            # Load trades from bronze Delta table (Location 3a)
            bronze_trades_df = self._load_bronze_trades()
            
            # Apply currency normalization (Location 3b)
            normalized_df = self._normalize_currency(bronze_trades_df)
            
            # Remove duplicate records (Location 3c)
            deduplicated_df = self._remove_duplicates(normalized_df)
            
            # Format timestamp fields (Location 3d)
            timestamp_formatted_df = self._format_timestamps(deduplicated_df)
            
            # Add silver layer processing metadata
            cleansed_df = timestamp_formatted_df.withColumn(
                "silver_processing_timestamp",
                current_timestamp()
            ).withColumn(
                "data_quality_status",
                lit("CLEANSED")
            )
            
            logger.info(f"Silver cleansing completed. Processed {cleansed_df.count()} records")
            return cleansed_df
            
        except Exception as e:
            logger.error(f"Error during silver data cleansing: {e}")
            raise
    
    def _load_bronze_trades(self) -> DataFrame:
        """
        Load raw trades from bronze Delta table (Location 3a).
        
        Returns:
            DataFrame: Raw trades data from bronze layer
        """
        try:
            bronze_path = f"{self.delta_config.get('bronze_path', '/delta/bronze/')}bronze_trades"
            
            logger.info("Loading trades from bronze Delta table")
            
            # Read trades from bronze Delta table
            bronze_trades_df = self.spark.read.format("delta").load(bronze_path)
            
            record_count = bronze_trades_df.count()
            logger.info(f"Loaded {record_count} records from bronze trades table")
            
            return bronze_trades_df
            
        except Exception as e:
            logger.error(f"Error loading bronze trades data: {e}")
            raise
    
    def _normalize_currency(self, df: DataFrame) -> DataFrame:
        """
        Standardize currency codes to uppercase (Location 3b).
        
        Args:
            df (DataFrame): Input DataFrame with currency column
            
        Returns:
            DataFrame: DataFrame with normalized currency codes
        """
        try:
            logger.info("Applying currency normalization")
            
            # Standardize currency to uppercase and trim whitespace
            normalized_df = df.withColumn(
                "Currency",
                upper(trim(col("Currency")))
            )
            
            # Validate currency codes (should be 3 characters)
            normalized_df = normalized_df.withColumn(
                "Currency",
                when(
                    col("Currency").rlike("^[A-Z]{3}$"),
                    col("Currency")
                ).otherwise(None)
            )
            
            # Count invalid currency codes
            invalid_currency_count = normalized_df.filter(col("Currency").isNull()).count()
            if invalid_currency_count > 0:
                logger.warning(f"Found {invalid_currency_count} records with invalid currency codes")
            
            logger.info("Currency normalization completed")
            return normalized_df
            
        except Exception as e:
            logger.error(f"Error during currency normalization: {e}")
            raise
    
    def _remove_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicate trades based on TradeID (Location 3c).
        
        Args:
            df (DataFrame): Input DataFrame with potential duplicates
            
        Returns:
            DataFrame: DataFrame with duplicates removed
        """
        try:
            logger.info("Removing duplicate records based on TradeID")
            
            # Define window specification for deduplication
            window_spec = Window.partitionBy("TradeID").orderBy(col("ingestion_timestamp").desc())
            
            # Add row number and keep latest record (highest ingestion_timestamp)
            deduplicated_df = df.withColumn(
                "row_num",
                row_number().over(window_spec)
            ).filter(col("row_num") == 1).drop("row_num")
            
            # Count duplicates removed
            original_count = df.count()
            deduplicated_count = deduplicated_df.count()
            duplicates_removed = original_count - deduplicated_count
            
            if duplicates_removed > 0:
                logger.info(f"Removed {duplicates_removed} duplicate records")
            else:
                logger.info("No duplicate records found")
            
            return deduplicated_df
            
        except Exception as e:
            logger.error(f"Error during deduplication: {e}")
            raise
    
    def _format_timestamps(self, df: DataFrame) -> DataFrame:
        """
        Standardize timestamp format for consistency (Location 3d).
        
        Args:
            df (DataFrame): Input DataFrame with timestamp columns
            
        Returns:
            DataFrame: DataFrame with standardized timestamps
        """
        try:
            logger.info("Formatting timestamp fields")
            
            # List of timestamp columns to standardize
            timestamp_columns = ["TradeDate", "SettlementDate", "ingestion_timestamp"]
            
            formatted_df = df
            
            for column in timestamp_columns:
                if column in formatted_df.columns:
                    # Convert to standard timestamp format
                    formatted_df = formatted_df.withColumn(
                        column,
                        to_timestamp(col(column))
                    )
            
            # Add derived business date columns for easier analysis
            formatted_df = formatted_df.withColumn(
                "TradeDateYYYYMMDD",
                col("TradeDate").cast("date")
            ).withColumn(
                "SettlementDateYYYYMMDD", 
                col("SettlementDate").cast("date")
            )
            
            logger.info("Timestamp formatting completed")
            return formatted_df
            
        except Exception as e:
            logger.error(f"Error during timestamp formatting: {e}")
            raise
    
    def write_to_silver_delta(self, df: DataFrame, table_name: str) -> None:
        """
        Write cleansed DataFrame to silver Delta table.
        
        Args:
            df (DataFrame): Cleansed DataFrame to write
            table_name (str): Target silver table name
        """
        try:
            silver_path = f"{self.delta_config.get('silver_path', '/delta/silver/')}{table_name}"
            
            logger.info(f"Writing {df.count()} records to silver Delta table: {table_name}")
            
            # Write to Silver Delta Table
            df.write.format("delta") \
              .mode("overwrite") \
              .option("overwriteSchema", "true") \
              .save(silver_path)
            
            logger.info(f"Successfully wrote cleansed data to silver Delta table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error writing to silver Delta table {table_name}: {e}")
            raise
    
    def run_silver_cleansing(self) -> None:
        """
        Run complete silver layer cleansing process.
        
        Process:
        1. Load bronze data
        2. Apply all cleansing transformations
        3. Write to silver Delta tables
        4. Log processing statistics
        """
        try:
            logger.info("Starting silver layer cleansing process")
            
            # Cleanse trades data
            cleansed_trades_df = self.cleanse_trades_data()
            self.write_to_silver_delta(cleansed_trades_df, "silver_trades")
            
            logger.info("Silver layer cleansing completed successfully")
            
        except Exception as e:
            logger.error(f"Silver cleansing process failed: {e}")
            raise
