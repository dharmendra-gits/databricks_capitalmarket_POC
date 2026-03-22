"""
Gold Layer Business Aggregations for Databricks Medallion Architecture
Capital Markets Post-Trade Settlement

Location 4a-4c: Business-level aggregations and KPI calculations
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, max, min, current_timestamp, lit,
    date_format, year, month, dayofweek, quarter
)
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class GoldAggregations:
    """Handles business-level aggregations and KPI calculations in gold layer."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize Gold Aggregations with Spark session and configuration.
        
        Args:
            spark (SparkSession): Active Spark session
            config (Dict[str, Any]): Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.delta_config = config.get('delta_lake', {})
        self.source_mapping = config.get('source_to_target_mapping', {})
        
    def create_trade_aggregates(self) -> DataFrame:
        """
        Create business-level trade aggregates for gold layer.
        
        Returns:
            DataFrame: Aggregated trade data with business metrics
            
        Process:
        1. Read Silver Trades Data (Location 4a)
        2. Calculate Business Metrics (Location 4b)
        3. Add time-based aggregations
        4. Return gold-ready aggregates
        """
        try:
            logger.info("Starting gold layer trade aggregations")
            
            # Load cleansed trades from silver Delta table (Location 4a)
            silver_trades_df = self._load_silver_trades()
            
            # Calculate business metrics (Location 4b)
            aggregated_df = self._calculate_business_metrics(silver_trades_df)
            
            # Add time-based aggregations
            time_aggregated_df = self._add_time_based_aggregates(aggregated_df)
            
            # Add gold layer processing metadata
            gold_df = time_aggregated_df.withColumn(
                "gold_processing_timestamp",
                current_timestamp()
            ).withColumn(
                "aggregation_level",
                lit("COUNTERPARTY_DATE")
            )
            
            logger.info(f"Gold aggregations completed. Generated {gold_df.count()} aggregate records")
            return gold_df
            
        except Exception as e:
            logger.error(f"Error during gold aggregations: {e}")
            raise
    
    def _load_silver_trades(self) -> DataFrame:
        """
        Load cleansed trades from silver layer (Location 4a).
        
        Returns:
            DataFrame: Cleansed trades data from silver layer
        """
        try:
            silver_path = f"{self.delta_config.get('silver_path', '/delta/silver/')}silver_trades"
            
            logger.info("Loading trades from silver Delta table")
            
            # Read trades from silver Delta table
            silver_trades_df = self.spark.read.format("delta").load(silver_path)
            
            # Filter only valid records
            valid_trades_df = silver_trades_df.filter(
                col("data_quality_status") == "CLEANSED"
            )
            
            record_count = valid_trades_df.count()
            logger.info(f"Loaded {record_count} valid records from silver trades table")
            
            return valid_trades_df
            
        except Exception as e:
            logger.error(f"Error loading silver trades data: {e}")
            raise
    
    def _calculate_business_metrics(self, df: DataFrame) -> DataFrame:
        """
        Calculate daily notional and other business metrics (Location 4b).
        
        Args:
            df (DataFrame): Input cleansed trades DataFrame
            
        Returns:
            DataFrame: DataFrame with business metrics aggregated
        """
        try:
            logger.info("Calculating business metrics - daily notional by counterparty and date")
            
            # Group by Counterparty & TradeDate and Sum Notional Values (Location 4b)
            business_metrics_df = df.groupBy(
                "Counterparty",
                "TradeDateYYYYMMDD"
            ).agg(
                # Primary business metrics
                sum("Notional").alias("DailyNotional"),
                count("TradeID").alias("TradeCount"),
                avg("Notional").alias("AverageTradeSize"),
                max("Notional").alias("LargestTradeSize"),
                min("Notional").alias("SmallestTradeSize"),
                
                # Additional metrics
                countDistinct("InstrumentID").alias("UniqueInstruments"),
                countDistinct("Currency").alias("UniqueCurrencies")
            ).filter(
                col("DailyNotional").isNotNull()
            )
            
            # Add derived metrics
            business_metrics_df = business_metrics_df.withColumn(
                "NotionalUSD",
                when(col("Currency") == "USD", col("DailyNotional"))
                .otherwise(None)  # Would add FX conversion logic here
            ).withColumn(
                "TradeVolumeCategory",
                when(col("DailyNotional") < 1000000, "SMALL")
                .when((col("DailyNotional") >= 1000000) & (col("DailyNotional") < 10000000), "MEDIUM")
                .otherwise("LARGE")
            )
            
            logger.info("Business metrics calculation completed")
            return business_metrics_df
            
        except Exception as e:
            logger.error(f"Error during business metrics calculation: {e}")
            raise
    
    def _add_time_based_aggregates(self, df: DataFrame) -> DataFrame:
        """
        Add time-based aggregations for trend analysis.
        
        Args:
            df (DataFrame): Business metrics DataFrame
            
        Returns:
            DataFrame: DataFrame with time-based aggregations
        """
        try:
            logger.info("Adding time-based aggregations")
            
            # Add time dimension columns
            time_enriched_df = df.withColumn(
                "TradeYear",
                year(col("TradeDateYYYYMMDD"))
            ).withColumn(
                "TradeMonth",
                month(col("TradeDateYYYYMMDD"))
            ).withColumn(
                "TradeQuarter",
                quarter(col("TradeDateYYYYMMDD"))
            ).withColumn(
                "TradeDayOfWeek",
                dayofweek(col("TradeDateYYYYMMDD"))
            ).withColumn(
                "TradeMonthName",
                date_format(col("TradeDateYYYYMMDD"), "MMMM")
            )
            
            # Add running totals and rankings (window functions could be added here)
            # For simplicity, keeping basic aggregations
            
            logger.info("Time-based aggregations completed")
            return time_enriched_df
            
        except Exception as e:
            logger.error(f"Error adding time-based aggregates: {e}")
            raise
    
    def create_counterparty_summary(self) -> DataFrame:
        """
        Create counterparty-level summary aggregations.
        
        Returns:
            DataFrame: Counterparty summary metrics
        """
        try:
            logger.info("Creating counterparty summary aggregations")
            
            # Load silver trades data
            silver_trades_df = self._load_silver_trades()
            
            # Create counterparty-level aggregates
            counterparty_summary_df = silver_trades_df.groupBy("Counterparty").agg(
                count("TradeID").alias("TotalTrades"),
                sum("Notional").alias("TotalNotional"),
                avg("Notional").alias("AverageTradeSize"),
                min("TradeDateYYYYMMDD").alias("FirstTradeDate"),
                max("TradeDateYYYYMMDD").alias("LastTradeDate"),
                countDistinct("InstrumentID").alias("UniqueInstrumentsTraded"),
                countDistinct("Currency").alias("CurrenciesTraded")
            ).orderBy(
                col("TotalNotional").desc()
            )
            
            # Add counterparty ranking
            from pyspark.sql.window import Window
            from pyspark.sql.functions import rank
            
            window_spec = Window.orderBy(col("TotalNotional").desc())
            counterparty_summary_df = counterparty_summary_df.withColumn(
                "NotionalRank",
                rank().over(window_spec)
            )
            
            logger.info("Counterparty summary aggregations completed")
            return counterparty_summary_df
            
        except Exception as e:
            logger.error(f"Error creating counterparty summary: {e}")
            raise
    
    def write_to_gold_delta(self, df: DataFrame, table_name: str, partition_cols: list = None) -> None:
        """
        Write aggregates to gold Delta table (Location 4c).
        
        Args:
            df (DataFrame): Aggregated DataFrame to write
            table_name (str): Target gold table name
            partition_cols (list): Columns to partition by
        """
        try:
            gold_path = f"{self.delta_config.get('gold_path', '/delta/gold/')}{table_name}"
            
            logger.info(f"Writing {df.count()} records to gold Delta table: {table_name}")
            
            # Prepare writer
            writer = df.write.format("delta").mode("overwrite")
            
            # Add partitioning if specified
            if partition_cols:
                writer = writer.partitionBy(partition_cols)
                logger.info(f"Partitioning by: {partition_cols}")
            
            # Write to Gold Delta Table (Location 4c)
            writer.option("overwriteSchema", "true").save(gold_path)
            
            logger.info(f"Successfully wrote aggregates to gold Delta table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error writing to gold Delta table {table_name}: {e}")
            raise
    
    def run_gold_aggregations(self) -> None:
        """
        Run complete gold layer aggregation process.
        
        Process:
        1. Load silver data
        2. Create trade aggregates
        3. Create counterparty summaries
        4. Write to gold Delta tables with partitioning
        5. Log processing statistics
        """
        try:
            logger.info("Starting gold layer aggregation process")
            
            # Create trade aggregates
            trade_aggregates_df = self.create_trade_aggregates()
            self.write_to_gold_delta(
                trade_aggregates_df, 
                "gold_trade_aggregates",
                partition_cols=["TradeYear", "TradeMonth"]
            )
            
            # Create counterparty summaries
            counterparty_summary_df = self.create_counterparty_summary()
            self.write_to_gold_delta(
                counterparty_summary_df,
                "gold_counterparty_summary"
            )
            
            logger.info("Gold layer aggregation completed successfully")
            
        except Exception as e:
            logger.error(f"Gold aggregation process failed: {e}")
            raise
