# UPI Transactions CDC Feed Project

## 🚀 Overview

This project implements a comprehensive **Change Data Capture (CDC)** streaming pipeline for UPI (Unified Payments Interface) transactions using Databricks, Delta Lake, and Apache Spark Structured Streaming. The pipeline processes transaction data in real-time with CDC support, performs merchant aggregations, and provides comprehensive monitoring capabilities for financial transaction analytics.

## 🏗️ Architecture

### Data Pipeline Flow

```
Mock Data Generator → Raw UPI Transactions (CDC) → Streaming Pipeline → Merchant Aggregations
        ↓                        ↓                        ↓                    ↓
   INSERT/UPDATE/DELETE    CDC Feed Enabled        Real-time Processing    Business Analytics
```

### Core Components

1. **Enhanced Data Model** - Comprehensive schema with CDC (Change Data Capture) support
2. **Real-Time Streaming Pipeline** - Processes transactions with CDC awareness
3. **Merchant Aggregations** - Real-time merchant performance metrics
4. **Mock Data Generator** - Realistic test data with CDC operations (INSERT, UPDATE, DELETE)
5. **CDC Processing** - Change Data Capture for real-time data synchronization

### Technology Stack

- **Databricks** - Cloud-native analytics platform
- **Delta Lake** - ACID transactions and CDC support
- **Apache Spark Structured Streaming** - Real-time data processing
- **Unity Catalog** - Data governance and management
- **Python/PySpark** - Data processing logic

## 📊 Data Model

### Tables Created

1. **`raw_upi_transactions_v1`** - Raw transaction data with CDC enabled
2. **`merchant_aggregations`** - Hourly merchant performance metrics

### Key Features

- **CDC Support** - Track all changes to transaction data (INSERT, UPDATE, DELETE)
- **Partitioning** - Optimized by merchant_category for performance
- **Auto-optimization** - Automatic file compaction and optimization
- **Comprehensive Schema** - Transaction details, merchant info, customer data, location data
- **Real-time Processing** - CDC-aware streaming for immediate data synchronization

## 🚀 Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Appropriate permissions to create catalogs, schemas, and tables
- Python 3.8+ runtime
- Delta Lake and Structured Streaming support

### Setup Instructions

1. **Run the Enhanced Data Model Setup**
   ```python
   # Execute: 01_enhanced_data_model.ipynb
   # This creates the catalog, schema, and all required tables with CDC enabled
   ```

2. **Generate Test Data with CDC Operations**
   ```python
   # Execute: 03_enhanced_mock_data_generator.ipynb
   # Generates realistic mock transaction data with INSERT, UPDATE, DELETE operations
   ```

3. **Start the Streaming Pipeline**
   ```python
   # Execute: 02_realtime_streaming_pipeline.ipynb
   # Starts the real-time CDC-aware processing pipeline
   ```

### Execution Order

1. **01_enhanced_data_model.ipynb** - Creates tables with CDC support
2. **03_enhanced_mock_data_generator.ipynb** - Generates test data (run continuously)
3. **02_realtime_streaming_pipeline.ipynb** - Processes CDC changes in real-time

## 🔄 Real-Time Processing

### Streaming Pipeline Features

- **CDC Processing** - Automatically detects and processes INSERT, UPDATE, DELETE operations
- **Batch Processing** - Processes data in 30-second intervals
- **Error Handling** - Comprehensive error logging and recovery
- **Data Validation** - Quality checks on incoming data
- **Merchant Aggregations** - Real-time merchant performance metrics with CDC awareness
- **Idempotent Operations** - Handles duplicate processing gracefully

### Processing Flow

1. **Data Ingestion** - New transactions arrive in `raw_upi_transactions_v1`
2. **CDC Detection** - Streaming pipeline detects changes via CDC feed
3. **Data Validation** - Validates data quality and filters invalid records
4. **CDC-Aware Aggregation** - Performs merchant aggregations considering CDC operations
5. **Storage** - Stores results in merchant aggregations table
6. **Monitoring** - Logs processing status and metrics

### CDC Processing Logic

- **INSERT Operations** - Add new transaction data to aggregations
- **UPDATE Operations** - Modify existing transaction data in aggregations
- **DELETE Operations** - Remove transaction data from aggregations
- **CDC Multiplier** - Uses +1 for INSERT, -1 for DELETE to maintain accurate counts

## 📈 Monitoring & Alerting

### Monitoring Capabilities

- **Pipeline Health** - Real-time streaming query status
- **Transaction Volume** - Volume trends and patterns
- **Error Rates** - Success/failure rates by merchant and status
- **Processing Performance** - Batch processing times and throughput
- **Data Quality** - Validation results and data quality metrics

### Alerting Thresholds

- **Success Rate** - Alert if below 90%
- **Processing Errors** - Alert on any processing failures
- **Pipeline Status** - Alert if streaming stops
- **Data Quality** - Alert on high invalid data rates

### Monitoring Dashboard

The monitoring dashboard provides:

- Real-time pipeline health status
- Transaction volume analysis
- Error rate monitoring
- Processing log analysis
- Merchant performance metrics

## 🔧 Configuration

### Environment Variables

```python
catalog_name = "upi_analytics"
schema_name = "upi_transactions"
streaming_trigger_interval = "30 seconds"
monitoring_hours_back = 24
```

### Cluster Configuration

- **Driver Node**: i3.xlarge
- **Worker Nodes**: i3.xlarge (2-3 nodes)
- **Spark Version**: 13.3.x-scala2.12
- **Auto-scaling**: Enabled
- **Spot Instances**: Enabled with fallback

## 🚀 Deployment

### Databricks Job Workflow

The project includes a complete Databricks job workflow configuration:

```json
{
  "name": "UPI_Transactions_Real_Time_Streaming",
  "description": "Real-time UPI transactions processing pipeline",
  "max_concurrent_runs": 1,
  "timeout_seconds": 0
}
```

### Job Tasks

1. **Setup Data Model** - Creates tables and schema
2. **Start Streaming Pipeline** - Initiates real-time processing
3. **Monitoring Dashboard** - Runs health checks and monitoring

### Deployment Steps

1. **Import Job Configuration**
   ```bash
   # Import: upi_streaming_job_workflow.json
   # Into your Databricks workspace
   ```

2. **Configure Parameters**
   - Set appropriate cluster configurations
   - Update email notifications
   - Configure access controls

3. **Run the Job**
   ```bash
   # Start the job manually or schedule it
   # The streaming pipeline will run continuously
   ```

## 📊 Data Analytics

### Merchant Analytics

- Transaction volume and value
- Success rates by merchant
- Customer acquisition metrics
- Revenue trends and patterns

### Customer Analytics

- Transaction behavior patterns
- Spending analysis by demographics
- Merchant preference analysis
- Customer lifetime value

### Business Intelligence

- Real-time dashboards
- KPI monitoring
- Trend analysis
- Performance benchmarking

## 🔒 Security & Governance

### Data Security

- Unity Catalog integration
- Row-level security support
- Data encryption at rest and in transit
- Access control and audit logging

### Data Governance

- Schema evolution support
- Data lineage tracking
- Quality monitoring
- Compliance reporting

## 🛠️ Troubleshooting

### Common Issues

1. **Streaming Query Stops**
   ```python
   # Check streaming query status
   streaming_query.status
   
   # Restart if needed
   streaming_query.stop()
   streaming_query.start()
   ```

2. **High Error Rates**
   ```python
   # Check data quality
   # Review processing logs
   # Validate input data format
   ```

3. **Performance Issues**
   ```python
   # Optimize cluster size
   # Check partition strategy
   # Review checkpoint locations
   ```

### Monitoring Queries

```sql
-- Check streaming query status
SHOW STREAMS;

-- Monitor processing logs
SELECT * FROM upi_analytics.upi_transactions.processing_log 
WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
ORDER BY start_time DESC;

-- Check transaction volume
SELECT 
  DATE_TRUNC('hour', transaction_timestamp) as hour,
  COUNT(*) as transactions,
  SUM(transaction_amount) as total_amount
FROM upi_analytics.upi_transactions.raw_upi_transactions
WHERE transaction_timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY 1 ORDER BY 1;
```

## 📚 Additional Resources

### Documentation Links

- [Databricks Streaming Guide](https://docs.databricks.com/structured-streaming/index.html)
- [Delta Lake CDC](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)

### Best Practices

1. **Monitoring** - Set up comprehensive monitoring and alerting
2. **Testing** - Use mock data for development and testing
3. **Performance** - Optimize cluster size based on workload
4. **Security** - Implement proper access controls
5. **Backup** - Regular backup of checkpoint locations

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:

- Create an issue in the repository
- Contact the data engineering team
- Check the troubleshooting section

---

**🎉 Happy Streaming!** 🎉
