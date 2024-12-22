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
source venv/bin/activate
```

2. **Install the package in development mode**:
```bash
pip install -e ".[dev]"
```

This will install:
- All required dependencies
- Development tools (pytest, black, flake8)
- The payment_system package in editable mode

3. **Start the infrastructure services**:
```bash
docker-compose -f config/docker-compose.yml up -d
```

4. **Create the HBase table**:
```bash
docker exec hbase hbase shell /opt/create_table.hbase
```

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

## Monitoring Kafka

There are several ways to monitor and inspect your Kafka cluster and messages:

### 1. Kafka UI Dashboard
The project includes a web-based Kafka UI for easy monitoring:
- Access the UI at: http://localhost:8080
- Features:
  - View all topics and their configurations
  - Browse messages in topics
  - Monitor broker metrics
  - Track consumer groups
  - View partition information
  - Real-time message monitoring

### 2. Command Line Tools

#### View Messages in Real-time
To see messages as they are being published:
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic payment-transactions
```
This command will show new messages as they arrive. Press Ctrl+C to stop.

#### View All Messages from Beginning
To see all messages, including historical ones:
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic payment-transactions --from-beginning
```
This is useful for debugging or verifying historical data. Note that this will show all messages ever published to the topic.

#### Check Topic Information
To view detailed information about the topic:
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --describe --topic payment-transactions
```
This shows:
- Number of partitions
- Replication factor
- Partition leaders
- Replica information
- Configuration settings

#### Check Message Counts
To see the number of messages in each partition:
```bash
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:29092 --topic payment-transactions
```
Output format:
- `[topic]:[partition]:[offset]`
- The offset number represents the total number of messages in that partition

### Best Practices for Monitoring

1. **Regular Health Checks**:
   - Use the Kafka UI to monitor broker health
   - Check consumer lag to ensure messages are being processed
   - Monitor disk usage and partition distribution

2. **Debugging Tips**:
   - Start with the Kafka UI for a high-level overview
   - Use console consumer with `--from-beginning` to verify data integrity
   - Check topic configuration if experiencing performance issues

3. **Performance Monitoring**:
   - Watch for growing consumer lag
   - Monitor partition distribution
   - Check message throughput rates

### Common Issues and Solutions

1. **Messages Not Visible**:
   - Verify producer is publishing to correct topic
   - Check consumer group offsets
   - Ensure topic exists and is properly configured

2. **Performance Issues**:
   - Check number of partitions
   - Verify consumer group configurations
   - Monitor broker resources

3. **Connection Issues**:
   - Verify Kafka and Zookeeper are running
   - Check network connectivity
   - Confirm broker listener configurations

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

## HBase Integration and Management

### HBase Configuration
The HBase container is configured with:
- Thrift server running on port 9091 (mapped from container port 9090)
- Proper Zookeeper integration via the payment-network
- Volume mount for table creation script

### HBase Table Schema
- Table: `payments`
- Column Family: `cf`
- Row Key Format: `{timestamp}_{payment_type}_{originator_id}`
- Columns:
  - payment_id
  - payment_type
  - amount_value
  - amount_currency
  - originator_id
  - originator_name
  - beneficiary_id
  - beneficiary_name
  - created_at

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
