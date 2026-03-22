"""
Main Entry Point for Databricks Medallion Architecture
Capital Markets Post-Trade Settlement

Complete Medallion Architecture data flow from raw landing zone 
to business-ready gold aggregates, with configuration-driven 
transformations and validation testing.
"""

import sys
import os
import logging
from typing import Dict, Any

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.utils.spark_session_manager import SparkSessionManager
from src.utils.logger import LoggerSetup
from src.utils.config_loader import ConfigLoader
from src.bronze.bronze_ingestion import BronzeIngestion
from src.silver.silver_cleansing import SilverCleansing
from src.gold.gold_aggregations import GoldAggregations

def main():
    """
    Main execution function for the Capital Markets Medallion Architecture.
    
    Process Flow:
    1. Project Initialization (Location 1a-1c)
    2. Bronze Layer Data Ingestion (Location 2a-2c)
    3. Silver Layer Data Cleansing (Location 3a-3d)
    4. Gold Layer Business Aggregations (Location 4a-4c)
    5. Complete pipeline execution
    """
    
    # Project Initialization
    print("🚀 Starting Capital Markets Medallion Architecture Pipeline")
    
    try:
        # Load configuration from config.yaml (Location 1b)
        config_loader = ConfigLoader()
        config = config_loader.load_config()
        
        # Setup logging (Location 1c)
        logger_config = config.get('logging', {})
        logger = LoggerSetup.setup_logger(
            name='capital_markets',
            level=logger_config.get('level', 'INFO'),
            log_format=logger_config.get('format'),
            log_file=logger_config.get('file')
        )
        
        logger.info("Configuration and logging initialized successfully")
        
        # Initialize Spark Session (Location 1a)
        spark_config = config.get('spark', {})
        spark_manager = SparkSessionManager(spark_config)
        spark = spark_manager.get_or_create_session()
        
        logger.info("Spark session initialized with Delta Lake support")
        
        # Execute Medallion Architecture Pipeline
        
        # Bronze Layer - Raw Data Ingestion (Location 2a-2c)
        logger.info("📍 Starting Bronze Layer Ingestion")
        bronze_ingestion = BronzeIngestion(spark, config)
        bronze_ingestion.run_bronze_ingestion()
        logger.info("✅ Bronze Layer Ingestion Completed")
        
        # Silver Layer - Data Cleansing (Location 3a-3d)
        logger.info("📍 Starting Silver Layer Cleansing")
        silver_cleansing = SilverCleansing(spark, config)
        silver_cleansing.run_silver_cleansing()
        logger.info("✅ Silver Layer Cleansing Completed")
        
        # Gold Layer - Business Aggregations (Location 4a-4c)
        logger.info("📍 Starting Gold Layer Aggregations")
        gold_aggregations = GoldAggregations(spark, config)
        gold_aggregations.run_gold_aggregations()
        logger.info("✅ Gold Layer Aggregations Completed")
        
        # Pipeline Completion
        logger.info("🎉 Capital Markets Medallion Architecture Pipeline Completed Successfully")
        print("🎉 Pipeline execution completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {e}")
        print(f"❌ Pipeline failed: {e}")
        sys.exit(1)
        
    finally:
        # Clean up resources
        try:
            if 'spark_manager' in locals():
                spark_manager.stop_session()
                logger.info("Spark session stopped")
        except Exception as e:
            logger.warning(f"Error stopping Spark session: {e}")

def run_tests():
    """
    Run test suite for validation testing (Location 5a-5c).
    """
    print("🧪 Running Test Suite for Medallion Architecture")
    
    try:
        import subprocess
        import sys
        
        # Run pytest
        result = subprocess.run([
            sys.executable, "-m", "pytest", 
            "tests/", 
            "-v", 
            "--tb=short"
        ], cwd=os.path.dirname(__file__))
        
        if result.returncode == 0:
            print("✅ All tests passed successfully!")
        else:
            print("❌ Some tests failed")
            sys.exit(result.returncode)
            
    except Exception as e:
        print(f"❌ Test execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Capital Markets Medallion Architecture Pipeline")
    parser.add_argument(
        "--test", 
        action="store_true", 
        help="Run test suite instead of pipeline"
    )
    parser.add_argument(
        "--config", 
        default="config/config.yaml",
        help="Path to configuration file"
    )
    
    args = parser.parse_args()
    
    if args.test:
        run_tests()
    else:
        main()
