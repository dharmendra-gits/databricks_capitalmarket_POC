"""
Spark Session Manager for Databricks Medallion Architecture
Capital Markets Post-Trade Settlement
"""

from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

class SparkSessionManager:
    """Manages Spark session creation and configuration for Delta Lake operations."""
    
    def __init__(self, config: dict):
        """
        Initialize Spark Session Manager with configuration.
        
        Args:
            config (dict): Spark configuration dictionary
        """
        self.config = config
        self.spark_session = None
        
    def get_or_create_session(self) -> SparkSession:
        """
        Get existing Spark session or create new one with Delta Lake support.
        
        Returns:
            SparkSession: Configured Spark session with Delta Lake extensions
        """
        if self.spark_session is None:
            self.spark_session = self._create_spark_session()
            logger.info("Created new Spark session with Delta Lake support")
        else:
            logger.info("Using existing Spark session")
            
        return self.spark_session
    
    def _create_spark_session(self) -> SparkSession:
        """
        Create Spark session with Delta Lake configuration.
        
        Returns:
            SparkSession: New Spark session
        """
        builder = SparkSession.builder
        
        # Set basic configuration
        builder.appName(self.config.get('app_name', 'capital_markets_medallion'))
        builder.master(self.config.get('master', 'local[*]'))
        
        # Apply Delta Lake specific configurations
        spark_configs = self.config.get('config', {})
        for key, value in spark_configs.items():
            builder.config(key, value)
        
        # Create session
        session = builder.getOrCreate()
        
        # Set log level
        session.sparkContext.setLogLevel("WARN")
        
        logger.info("Spark session created successfully")
        return session
    
    def stop_session(self):
        """Stop the Spark session."""
        if self.spark_session is not None:
            self.spark_session.stop()
            self.spark_session = None
            logger.info("Spark session stopped")
