# Databricks Medallion Architecture for Capital Markets Post-Trade Settlement

Complete Medallion Architecture data flow from raw landing zone to business-ready gold aggregates, with configuration-driven transformations and validation testing.

## Architecture Overview

This project implements a comprehensive data pipeline following the Databricks Medallion Architecture pattern for capital markets post-trade settlement data.

### Layers

1. **Bronze Layer** - Raw data ingestion from landing zone
2. **Silver Layer** - Data quality improvements and normalization  
3. **Gold Layer** - Business-level aggregations and KPI calculations

### Key Features

- **Configuration-driven** transformations via YAML config
- **Delta Lake** support for ACID transactions and time travel
- **Data quality** validation and cleansing
- **Business aggregations** for capital markets analytics
- **Comprehensive testing** framework with pytest
- **Logging** and monitoring throughout the pipeline

## Project Structure

```
Capital_Market_Poc/
├── config/
│   └── config.yaml              # Configuration file
├── src/
│   ├── utils/
│   │   ├── spark_session_manager.py    # Spark session management
│   │   ├── logger.py                   # Logging setup
│   │   └── config_loader.py            # Configuration loading
│   ├── bronze/
│   │   └── bronze_ingestion.py         # Raw data ingestion
│   ├── silver/
│   │   └── silver_cleansing.py         # Data cleansing
│   └── gold/
│       └── gold_aggregations.py        # Business aggregations
├── tests/
│   ├── conftest.py              # Test fixtures and setup
│   └── test_silver.py           # Silver layer tests
├── main.py                      # Main pipeline entry point
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## Data Flow

### 1. Project Initialization
- **Location 1a**: Spark Session Creation - Initialize Spark session with Delta Lake support
- **Location 1b**: Metadata Configuration - Load source-to-target mappings and schema definitions  
- **Location 1c**: Logger Initialization - Setup centralized logging for all layers

### 2. Bronze Layer Data Ingestion
- **Location 2a**: Read Raw Trade Data - Ingest JSON trade data from landing zone
- **Location 2b**: Write to Bronze Delta - Persist raw trades to bronze Delta table
- **Location 2c**: Read Instrument Data - Ingest CSV instrument reference data

### 3. Silver Layer Data Cleansing
- **Location 3a**: Read Bronze Trades - Load raw trades from bronze Delta table
- **Location 3b**: Currency Normalization - Standardize currency codes to uppercase
- **Location 3c**: Deduplication - Remove duplicate trades based on TradeID
- **Location 3d**: Timestamp Formatting - Standardize timestamp format for consistency

### 4. Gold Layer Business Aggregations
- **Location 4a**: Read Silver Trades - Load cleansed trades from silver layer
- **Location 4b**: Calculate Daily Notional - Aggregate notional values by counterparty and date
- **Location 4c**: Write Gold Aggregates - Persist business aggregates to gold Delta table

### 5. Test Validation Framework
- **Location 5a**: Test Currency Normalization - Validate currency code standardization logic
- **Location 5b**: Validate Uppercase Conversion - Ensure all currency codes are properly uppercase
- **Location 5c**: Test Deduplication Logic - Verify duplicate removal based on TradeID

## Installation and Setup

### Prerequisites

- Python 3.8+
- Apache Spark 3.4+
- Java 8+

### Installation

1. Clone or download the project to your local machine
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure the pipeline by editing `config/config.yaml`:
   - Update source paths for your data
   - Adjust Delta Lake paths as needed
   - Modify Spark configuration for your environment

## Usage

### Running the Pipeline

Execute the complete Medallion Architecture pipeline:

```bash
python main.py
```

### Running Tests

Run the test suite to validate transformations:

```bash
python main.py --test
```

Or directly with pytest:

```bash
pytest tests/ -v
```

### Configuration

The pipeline is driven by `config/config.yaml`. Key sections:

- **source_to_target_mapping**: Define data sources and target tables
- **delta_lake**: Configure Delta Lake paths
- **spark**: Spark session configuration
- **data_quality**: Schema validation rules
- **logging**: Logging configuration

## Data Schema

### Trade Data Schema

| Field | Type | Description |
|-------|------|-------------|
| TradeID | string | Unique trade identifier |
| InstrumentID | string | Reference to instrument |
| Counterparty | string | Trading counterparty |
| Notional | double | Trade notional amount |
| Currency | string | Currency code (ISO 3) |
| TradeDate | timestamp | Trade execution date |
| SettlementDate | timestamp | Settlement date |
| Status | string | Trade status |

### Instrument Data Schema

| Field | Type | Description |
|-------|------|-------------|
| InstrumentID | string | Unique instrument identifier |
| InstrumentType | string | Type of instrument |
| InstrumentName | string | Full instrument name |
| Ticker | string | Trading symbol |
| Exchange | string | Trading exchange |

## Business Metrics

The gold layer generates business-ready aggregates including:

- **Daily Notional by Counterparty**: Total notional values per counterparty per day
- **Trade Counts**: Number of trades by various dimensions
- **Average Trade Sizes**: Statistical analysis of trade sizes
- **Time-based Trends**: Monthly, quarterly, yearly aggregations
- **Counterparty Rankings**: Top counterparties by volume

## Testing

The project includes comprehensive tests:

- **Currency Normalization Tests**: Validate uppercase conversion and format validation
- **Deduplication Tests**: Verify duplicate removal logic
- **Timestamp Formatting Tests**: Ensure consistent timestamp handling
- **End-to-End Pipeline Tests**: Validate complete data flow

Run tests with detailed output:

```bash
pytest tests/ -v -s
```

## Monitoring and Logging

- Centralized logging configuration
- Processing statistics at each layer
- Error handling and recovery
- Data quality metrics

## Performance Considerations

- Delta Lake optimization with Z-ordering
- Partitioned writes in gold layer
- Configurable Spark settings
- Memory management for large datasets

## Extending the Pipeline

The architecture is designed for extensibility:

- Add new data sources in config.yaml
- Implement new transformations in respective layer modules
- Extend business metrics in gold layer
- Add new test cases for validation

## Troubleshooting

### Common Issues

1. **Spark Session Issues**: Check Java installation and SPARK_HOME
2. **Delta Lake Path Errors**: Verify paths in config.yaml exist and are accessible
3. **Memory Issues**: Adjust Spark memory configuration in config
4. **Test Failures**: Ensure test data paths are correct and accessible

### Debug Mode

Enable debug logging by setting log level to DEBUG in config.yaml:

```yaml
logging:
  level: "DEBUG"
```

## Contributing

1. Follow the existing code structure
2. Add tests for new functionality
3. Update documentation
4. Ensure all tests pass before submission

## License

This project is provided as-is for educational and demonstration purposes.
