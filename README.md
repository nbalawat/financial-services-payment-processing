# Payment Transaction Generator and Processing System

This project implements a scalable payment transaction generation and processing system using Python, Apache Kafka, and multiple databases. It generates synthetic payment transaction data and provides a comprehensive pipeline for processing and querying payment data.

## Features

- Generate synthetic B2B payment transactions with realistic data
- Multiple payment types support (ACH, Wire, Check, etc.)
- Real-time transaction streaming with Kafka
- Multi-database support (HBase, PostgreSQL, MongoDB, Redis)
- Apache Beam pipeline for stream processing
- Comprehensive error handling and logging
- Health monitoring for all services
- Web UI for Kafka monitoring

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- 8GB RAM minimum (recommended: 16GB)
- 20GB free disk space

## Project Structure

```
generate_payment_transactions/
├── src/
│   └── payment_system/
│       ├── core/                 # Core payment processing functionality
│       │   ├── payment_processor.py
│       │   ├── payment_producer.py
│       │   ├── payment_query.py
│       │   ├── transaction_generator.py
│       │   └── generate_transactions.py
│       ├── api/                  # API endpoints and models
│       │   ├── main.py
│       │   ├── models.py
│       │   └── run_api.py
│       └── utils/               # Utility functions and scripts
│           ├── verify_hbase.py
│           ├── setup_hbase.py
│           ├── simple_processor.py
│           └── test_producer.py
├── config/                     # Configuration files
│   ├── docker-compose.yml
│   └── create_table.hbase
├── data/                      # Data storage
│   ├── generated_transactions/  # Generated payment data
│   ├── hbase/                  # HBase data files
│   ├── mongodb/                # MongoDB data files
│   └── postgres/               # PostgreSQL data files
├── tests/                     # Test files
│   ├── conftest.py
│   ├── test_payment_processor.py
│   ├── test_payment_producer.py
│   ├── test_transaction_generator.py
│   └── test_integration.py
├── setup.py                   # Package installation configuration
├── requirements.txt           # Python dependencies
└── README.md                 # Project documentation
```

## Installation

1. **Create and activate a virtual environment**:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install the package in development mode**:
```bash
pip install -e .
```

3. **Verify installation**:
```bash
python -c "import payment_system; print(payment_system.__version__)"
```

You should see the version number (0.1.0) printed. If you get an import error, make sure you:
1. Are in the correct directory (generate_payment_transactions)
2. Have activated the virtual environment
3. Have run the pip install command successfully

## Usage

1. Generate sample transaction data:
```bash
python src/payment_system/core/transaction_generator.py
```

2. Start the Kafka producer to stream transactions:
```bash
python src/payment_system/core/payment_producer.py
```

3. Run the processing pipeline:
```bash
python src/payment_system/core/payment_processor.py
```

4. Start the API server:
```bash
python src/payment_system/api/main.py
```

## Data Flow

1. **Generation**: `transaction_generator.py` creates synthetic payment data
2. **Streaming**: `payment_producer.py` sends data to Kafka
3. **Processing**: `payment_processor.py` processes streams using Apache Beam
4. **Storage**: Processed data is stored in HBase/PostgreSQL
5. **Querying**: `payment_query.py` provides interfaces for data retrieval
6. **API**: `main.py` exposes REST endpoints for external access

## Monitoring

1. **Kafka UI**: http://localhost:8080
   - Monitor topics, partitions, and messages
   - Track producer/consumer metrics

2. **HBase UI**: http://localhost:16010
   - Monitor regions and tables
   - Track cluster health

3. **Service Health**:
```bash
docker-compose ps
```

## Kafka Message Keys

### Understanding Message Keys

Message keys in Kafka serve several important purposes:

1. **Partitioning**:
   - Messages with the same key are guaranteed to go to the same partition
   - This ensures ordered processing for related messages
   - Without a key, messages are distributed round-robin across partitions

2. **Use Cases for Keys**:
   - **Payment Processing**: Using `originator_id` as key ensures all payments from the same customer go to the same partition
   - **Transaction Ordering**: Using `payment_id` as key when you need to process updates to the same payment in order
   - **Load Balancing**: Using `payment_type` as key to group similar payment types together

3. **Performance Implications**:
   - Keys enable parallel processing across partitions
   - They support scalability by allowing multiple consumers to process different partitions
   - Help in maintaining message order when needed

### Example Key Strategies

1. **Customer-Based Keys**:
```python
# Using originator_id as key
key = payment_dict['originator']['party_id']
```

2. **Payment-Based Keys**:
```python
# Using payment_id as key
key = payment_dict['payment_id']
```

3. **Type-Based Keys**:
```python
# Using payment_type as key
key = payment_dict['payment_type']
```

### When to Use Keys

1. **Use Keys When**:
   - You need ordered processing of related messages
   - You want to ensure similar messages go to the same consumer
   - You need to scale processing across multiple consumers

2. **Skip Keys When**:
   - Message order doesn't matter
   - You want maximum parallelism
   - You prefer simple round-robin distribution

### Current Implementation

In our current setup, we're not using message keys (hence the 0 Bytes key size in metrics). To add keys, modify the producer's `publish_payment` method:

```python
def publish_payment(self, payment_dict, topic='payment-transactions'):
    try:
        # Use payment_id as key
        key = payment_dict['payment_id'].encode('utf-8')
        
        future = self.producer.send(
            topic,
            key=key,
            value=payment_dict
        )
```

This would ensure that any updates to the same payment ID are processed in order.

## Streaming Simulation

For real-time transaction processing simulation, follow these steps:

1. **Start Infrastructure**:
```bash
docker-compose -f config/docker-compose.yml up -d
```

2. **Create HBase Table**:
```bash
docker exec hbase hbase shell /opt/create_table.hbase
```

3. **Start the Streaming Producer**:
This will generate continuous payment transactions (5 per second by default):
```bash
python src/payment_system/utils/streaming_producer.py
```

4. **Start the Payment Processor**:
In a new terminal, start the Apache Beam pipeline:
```bash
python src/payment_system/core/payment_processor.py
```

5. **Monitor the System**:
In a new terminal, start the monitoring dashboard:
```bash
python src/payment_system/utils/monitor_streaming.py
```

The monitoring dashboard shows:
- Kafka lag (difference between produced and consumed messages)
- Average processing time per transaction
- Current transaction rate
- Error count

You can adjust the transaction rate by modifying the `transactions_per_second` parameter when running the streaming producer:
```bash
python src/payment_system/utils/streaming_producer.py --tps 10  # 10 transactions per second
```

To stop the simulation:
1. Press Ctrl+C in each terminal window
2. The processes will shut down gracefully
3. Run `docker-compose -f config/docker-compose.yml down` to stop the infrastructure

## HBase Integration and Management

### HBase Configuration
The HBase container is configured with:
- Thrift server running on port 9091 (mapped from container port 9090)
- Proper Zookeeper integration via the payment-network
- Volume mount for table creation script

### HBase Table Schema
- Table: `payments`
- Column Family: `cf`
- Row Key Format: `{timestamp}_{paymentType}_{originatorId}`
- Columns:
  - paymentId
  - paymentType
  - amountValue
  - amountCurrency
  - originatorId
  - originatorName
  - beneficiaryId
  - beneficiaryName
  - createdAt

### Setting Up HBase

1. **Start HBase Container**:
The HBase container is configured in docker-compose.yml with necessary port mappings and volume mounts:
```yaml
hbase:
  image: dajobe/hbase
  container_name: hbase
  ports:
    - "16000:16000"  # HBase Master
    - "16010:16010"  # HBase Master Web UI
    - "16020:16020"  # HBase RegionServer
    - "16030:16030"  # HBase RegionServer Web UI
    - "9091:9090"    # Thrift server
    - "8082:8080"    # REST server
  environment:
    HBASE_CONF_hbase_cluster_distributed: "false"
    HBASE_CONF_hbase_zookeeper_quorum: zookeeper
  volumes:
    - ./create_table.hbase:/opt/create_table.hbase
  networks:
    - payment-network
```

2. **Create HBase Table**:
```bash
docker exec hbase hbase shell /opt/create_table.hbase
```

3. **Verify Table Creation**:
```bash
python verify_hbase.py
```

### Apache Beam Pipeline
The `payment_processor.py` script implements an Apache Beam pipeline that:
1. Reads messages from Kafka topic 'payment-transactions'
2. Normalizes the payment data
3. Writes to HBase using the Thrift protocol on port 9091

### Monitoring HBase

1. **HBase Web UIs**:
   - Master UI: http://localhost:16010
   - RegionServer UI: http://localhost:16030

2. **Verify Data**:
   - Use `verify_hbase.py` to scan and display all records
   - Check logs: `docker logs hbase`

3. **Troubleshooting**:
   - If connection issues occur, ensure Zookeeper is healthy
   - Check HBase Thrift server status in the container logs
   - Verify network connectivity between containers

### Common HBase Shell Commands

1. **List Tables**:
```bash
docker exec -it hbase hbase shell
> list
```

2. **Describe Table**:
```bash
> describe 'payments'
```

3. **Scan Table**:
```bash
> scan 'payments'
```

4. **Count Records**:
```bash
> count 'payments'
```

5. **Delete Table**:
```bash
> disable 'payments'
> drop 'payments'
```

### Data Cleanup

To clear all data and start fresh:
1. Stop the containers:
```bash
docker-compose down
```

2. Remove HBase data directory:
```bash
rm -rf data/hbase/*
```

3. Restart containers:
```bash
docker-compose up -d
```

4. Recreate the table:
```bash
docker exec hbase hbase shell /opt/create_table.hbase
```

## Monitoring

### HBase Data
To view data in HBase, first connect to the HBase shell:
```bash
docker exec -it hbase hbase shell
```

Once in the HBase shell, you can use these commands:

#### Basic Commands
```bash
# List all tables
list

# View table structure
describe 'payments'

# Count total records
count 'payments'

# View 5 most recent payments
scan 'payments', {LIMIT => 5}
```

#### Viewing Specific Data
```bash
# View specific row by key
get 'payments', 'your-row-key'

# Scan for specific payment type (e.g., WIRE transfers from today)
scan 'payments', {FILTER => "PrefixFilter('20241222_WIRE')"}

# Scan with column filters
scan 'payments', {COLUMNS => ['transaction:payment_type', 'transaction:amount']}

# Scan with time range (last hour)
scan 'payments', {TIMERANGE => [Time.now.to_i * 1000 - 3600000, Time.now.to_i * 1000]}
```

#### Table Management
```bash
# Disable table (required before modifications)
disable 'payments'

# Enable table
enable 'payments'

# Check if table is enabled
is_enabled 'payments'

# Get table status
status 'payments'
```

Note: Row keys in our system are formatted as: `timestamp_paymentType_originatorId`
Example: `20241222001530_WIRE_CUST123` (December 22, 2024 00:15:30 WIRE payment from CUST123)

## Error Handling

The system implements comprehensive error handling:
- Input validation
- Connection error recovery
- Data consistency checks
- Detailed error logging
- Graceful degradation

## Troubleshooting

1. **Service Issues**:
```bash
# Restart all services
docker-compose restart

# Check service logs
docker-compose logs [service-name]
```

2. **Data Issues**:
```bash
# Clear all data and restart
docker-compose down -v
docker-compose up -d
```

3. **Common Problems**:
- Port conflicts: Check if ports are already in use
- Memory issues: Ensure enough RAM is available
- Connection timeouts: Check network connectivity

## Maintenance

1. **Backup**:
- PostgreSQL data: `/var/lib/postgresql/data`
- MongoDB data: `/data/db`
- Redis data: `/data`

2. **Scaling**:
- Adjust `transactions_per_batch` in producer
- Modify Kafka partition count
- Scale HBase regions

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Note

Remember to update this README when making significant changes to:
- Infrastructure configuration
- API endpoints
- Data models
- Processing pipeline
- Deployment instructions

# Payment Transaction Processing System

A robust payment transaction processing system that handles various payment types (ACH, WIRE, RTP, SEPA, etc.) using Kafka for message streaming and HBase for persistent storage.

## Features

- **Real-time Payment Processing**
  - Supports multiple payment types: ACH, WIRE, RTP, SEPA, CARD, DIRECT_DEBIT
  - Configurable transaction generation rate
  - Real-time metrics and monitoring

- **Fault Tolerance**
  - Automatic backup of failed transactions to disk
  - Recovery mechanism for processing backed-up payments
  - Graceful handling of HBase connection issues

- **Data Storage**
  - HBase for persistent storage with optimized column families
  - Kafka for message streaming and queuing
  - Local JSON backup for failed transactions

## System Components

### 1. Transaction Producer (`streaming_producer.py`)
- Generates sample payment transactions
- Configurable transactions per second (TPS)
- Supports multiple payment types with realistic data

### 2. Transaction Processor (`streaming_processor.py`)
- Consumes transactions from Kafka
- Validates and processes payments
- Stores transactions in HBase
- Handles failures with automatic backup and recovery
- Tracks processing metrics

## Getting Started

1. **Start the Infrastructure**
```bash
cd config
docker-compose up -d
```

2. **Start the Transaction Producer**
```bash
# Default: 5 TPS
python src/payment_system/utils/streaming_producer.py

# Custom TPS:
python src/payment_system/utils/streaming_producer.py --tps 10
```

3. **Start the Transaction Processor**
```bash
python src/payment_system/core/streaming_processor.py
```

## Monitoring

### HBase Data
To view data in HBase:
```bash
# Connect to HBase shell
docker exec -it hbase hbase shell

# View recent payments
scan 'payments', {LIMIT => 5}

# Count total records
count 'payments'
```

### Kafka UI
Access the Kafka UI dashboard at http://localhost:8080 to monitor:
- Topic statistics
- Message throughput
- Consumer groups

### Processor Metrics
The processor logs metrics periodically, including:
- Processed payment count
- Failed payment count
- Processing latency
- Recovery statistics

## Directory Structure
```
.
├── config/
│   ├── docker-compose.yml    # Infrastructure setup
│   └── create_table.hbase    # HBase table creation script
├── src/
│   └── payment_system/
│       ├── core/
│       │   ├── streaming_processor.py   # Main processor
│       │   └── transaction_models.py    # Payment models
│       └── utils/
│           └── streaming_producer.py    # Transaction generator
├── failed_payments/          # Backup directory for failed transactions
└── README.md
```

## Error Handling
- Failed HBase writes are automatically backed up to `failed_payments/` directory
- Payments are recovered automatically when HBase becomes available
- Each backup file contains the complete payment data in JSON format

## Monitoring and Maintenance
- Monitor the `failed_payments/` directory for backup files
- Check processor logs for error messages and metrics
- Use HBase shell commands to verify data persistence
- Monitor Kafka UI for message flow and consumer health
