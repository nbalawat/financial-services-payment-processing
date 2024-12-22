"""
Monitor streaming performance of the payment processing system.
Tracks transaction rates, processing latency, and system health.
"""

import happybase
import time
import logging
from datetime import datetime, timedelta
from collections import deque
from prettytable import PrettyTable
import threading
import json
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import os
import curses

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class StreamingMonitor:
    def __init__(self):
        self.running = True
        self.stats = {
            'kafka_lag': deque(maxlen=100),
            'processing_times': deque(maxlen=100),
            'transaction_rates': deque(maxlen=100),
            'error_count': 0
        }
        
        # Initialize connections
        self.hbase = happybase.Connection('localhost', port=9091)
        self.consumer = KafkaConsumer(
            'payment-transactions',
            bootstrap_servers='localhost:9092',
            group_id='monitor_group',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Initialize curses screen
        self.screen = curses.initscr()
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_GREEN, -1)
        curses.init_pair(2, curses.COLOR_RED, -1)
        curses.init_pair(3, curses.COLOR_YELLOW, -1)
        
    def calculate_kafka_lag(self):
        """Calculate the lag between producer and consumer"""
        try:
            end_offsets = self.consumer.end_offsets(self.consumer.assignment())
            current_positions = {tp: self.consumer.position(tp) for tp in self.consumer.assignment()}
            total_lag = sum(end_offsets[tp] - current_positions[tp] for tp in end_offsets)
            self.stats['kafka_lag'].append(total_lag)
        except Exception as e:
            logger.error(f"Error calculating Kafka lag: {str(e)}")
            self.stats['error_count'] += 1

    def calculate_processing_time(self, message):
        """Calculate processing time for a transaction"""
        try:
            generated_time = datetime.fromisoformat(message['generated_at'])
            processing_time = (datetime.now() - generated_time).total_seconds()
            self.stats['processing_times'].append(processing_time)
        except Exception as e:
            logger.error(f"Error calculating processing time: {str(e)}")
            self.stats['error_count'] += 1

    def update_transaction_rate(self):
        """Calculate current transaction rate"""
        try:
            count = len(self.stats['processing_times'])
            self.stats['transaction_rates'].append(count)
        except Exception as e:
            logger.error(f"Error calculating transaction rate: {str(e)}")
            self.stats['error_count'] += 1

    def display_stats(self):
        """Display monitoring statistics using curses"""
        try:
            self.screen.clear()
            height, width = self.screen.getmaxyx()
            
            # Title
            title = "Payment System Monitoring"
            self.screen.addstr(0, (width - len(title)) // 2, title, curses.A_BOLD)
            
            # Stats
            y = 2
            self.screen.addstr(y, 2, "Kafka Statistics:")
            y += 1
            if self.stats['kafka_lag']:
                lag = self.stats['kafka_lag'][-1]
                color = curses.color_pair(1) if lag < 100 else curses.color_pair(2)
                self.screen.addstr(y, 4, f"Current Lag: {lag} messages", color)
            
            y += 2
            self.screen.addstr(y, 2, "Processing Statistics:")
            y += 1
            if self.stats['processing_times']:
                avg_time = sum(self.stats['processing_times']) / len(self.stats['processing_times'])
                color = curses.color_pair(1) if avg_time < 1.0 else curses.color_pair(2)
                self.screen.addstr(y, 4, f"Average Processing Time: {avg_time:.2f} seconds", color)
            
            y += 2
            if self.stats['transaction_rates']:
                rate = self.stats['transaction_rates'][-1]
                self.screen.addstr(y, 4, f"Transaction Rate: {rate} tps")
            
            y += 2
            error_color = curses.color_pair(1) if self.stats['error_count'] == 0 else curses.color_pair(2)
            self.screen.addstr(y, 2, f"Error Count: {self.stats['error_count']}", error_color)
            
            # Update screen
            self.screen.refresh()
            
        except Exception as e:
            logger.error(f"Error updating display: {str(e)}")

    def run(self):
        """Run the monitoring system"""
        try:
            while self.running:
                # Process messages
                messages = self.consumer.poll(timeout_ms=1000)
                for tp, msgs in messages.items():
                    for msg in msgs:
                        self.calculate_processing_time(msg.value)
                
                # Update stats
                self.calculate_kafka_lag()
                self.update_transaction_rate()
                self.display_stats()
                
                time.sleep(1)  # Update every second
                
        except KeyboardInterrupt:
            self.running = False
        finally:
            curses.endwin()
            self.consumer.close()
            self.hbase.close()

if __name__ == "__main__":
    monitor = StreamingMonitor()
    try:
        monitor.run()
    except KeyboardInterrupt:
        logger.info("Monitor stopped by user")
    finally:
        curses.endwin()
