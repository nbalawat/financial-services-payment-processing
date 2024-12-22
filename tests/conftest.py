import pytest
import os
import docker
from time import sleep

@pytest.fixture(scope="session")
def docker_compose_up():
    """Fixture to start docker-compose services for integration tests"""
    client = docker.from_env()
    
    # Start services
    os.system('docker-compose up -d')
    
    # Wait for services to be healthy
    max_retries = 30
    retry_interval = 2
    services = ['kafka', 'zookeeper', 'hbase', 'postgres', 'mongodb', 'redis']
    
    for _ in range(max_retries):
        all_healthy = True
        for service in services:
            try:
                container = client.containers.get(f'payment_transactions_{service}_1')
                health = container.attrs['State']['Health']['Status']
                if health != 'healthy':
                    all_healthy = False
                    break
            except:
                all_healthy = False
                break
        
        if all_healthy:
            break
            
        sleep(retry_interval)
    
    yield
    
    # Cleanup
    os.system('docker-compose down')

@pytest.fixture(scope="session")
def kafka_config():
    """Fixture for Kafka configuration"""
    return {
        'bootstrap_servers': ['localhost:9092'],
        'topic': 'payment_transactions'
    }

@pytest.fixture(scope="session")
def hbase_config():
    """Fixture for HBase configuration"""
    return {
        'host': 'localhost',
        'port': 16000,
        'table': 'payments',
        'column_family': 'cf'
    }

@pytest.fixture(scope="session")
def postgres_config():
    """Fixture for PostgreSQL configuration"""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'payment_benchmark',
        'user': 'benchmark_user',
        'password': 'benchmark_pass'
    }

@pytest.fixture(scope="session")
def sample_payment():
    """Fixture for sample payment data"""
    return {
        'payment_id': 'TEST123',
        'payment_type': 'ACH',
        'amount': 1000.00,
        'currency': 'USD',
        'status': 'pending',
        'originator_id': 'CUST123',
        'originator_name': 'Test Customer',
        'beneficiary_id': 'BEN456',
        'beneficiary_name': 'Test Beneficiary',
        'created_at': '2024-01-01T12:00:00'
    }
