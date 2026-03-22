#!/usr/bin/env python3
"""
Test script to verify Databricks connection
"""
import os
import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Load environment variables from .env file
load_dotenv()

def load_config():
    """Load configuration from config.yaml"""
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)

def test_databricks_connection():
    """Test connection to Databricks workspace"""
    print("🔍 Testing Databricks Connection...")
    
    try:
        # Load configuration
        config = load_config()
        spark_config = config.get('spark', {})
        
        # Create Spark session with Databricks configuration
        spark = SparkSession.builder \
            .appName("databricks_connection_test") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        print("✅ Spark session created successfully")
        
        # Test basic Spark functionality
        df = spark.range(100)
        print(f"✅ Created test DataFrame with {df.count()} rows")
        
        # Test Delta Lake functionality
        spark.sql("CREATE TABLE IF NOT EXISTS test_delta (id INT, value STRING) USING DELTA")
        print("✅ Delta Lake table created successfully")
        
        # Show connection info
        print(f"📊 Spark version: {spark.version}")
        print(f"📊 App name: {spark.conf.get('spark.app.name')}")
        
        spark.stop()
        print("✅ Connection test completed successfully!")
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False
    
    return True

def show_configuration_status():
    """Show current configuration status"""
    print("\n📋 Configuration Status:")
    print("=" * 50)
    
    try:
        with open('config/config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        
        spark_config = config.get('spark', {}).get('config', {})
        
        # Check Databricks configuration from environment variables
        databricks_address = os.getenv('DATABRICKS_WORKSPACE_URL')
        databricks_token = os.getenv('DATABRICKS_TOKEN')
        cluster_profile = os.getenv('DATABRICKS_CLUSTER_ID')
        
        print(f"🔗 Databricks Address: {'✅ Configured' if databricks_address and 'your-workspace' not in databricks_address else '⚠️ Needs Update'}")
        print(f"🔑 Databricks Token: {'✅ Configured' if databricks_token and 'your-personal' not in databricks_token else '⚠️ Needs Update'}")
        print(f"🖥️  Cluster Profile: {'✅ Configured' if cluster_profile and 'your-cluster' not in cluster_profile else '⚠️ Needs Update'}")
        
        if databricks_address and 'your-workspace' in databricks_address:
            print("\n📝 Next Steps:")
            print("1. Update .env file with your Databricks credentials:")
            print("   - DATABRICKS_WORKSPACE_URL")
            print("   - DATABRICKS_TOKEN")
            print("   - DATABRICKS_CLUSTER_ID")
            print("2. Run this test again")
        
    except Exception as e:
        print(f"❌ Error reading configuration: {e}")

if __name__ == "__main__":
    print("🚀 Databricks Connection Test")
    print("=" * 50)
    
    show_configuration_status()
    
    # Only test connection if environment variables are configured
    try:
        workspace_url = os.getenv('DATABRICKS_WORKSPACE_URL')
        if workspace_url and 'your-workspace' not in workspace_url:
            print("\n🧪 Testing connection...")
            test_databricks_connection()
        else:
            print("\n⚠️ Please update .env file with your Databricks credentials before testing connection")
            
    except Exception as e:
        print(f"❌ Error: {e}")
