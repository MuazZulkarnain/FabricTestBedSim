#!/usr/bin/env python3

import socket
import sys
import time
import random
import logging
import hashlib
import json
import numpy as np
from collections import deque
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='[Client] %(message)s')
logger = logging.getLogger()

ENDORSEMENT_PORT = 7052

class ClientMetrics:
    def __init__(self, metrics_file):
        self.metrics_file = metrics_file
        self.start_time = time.time()
        self.transactions_sent = 0
        self.transactions_processed = 0
        self.failed_transactions = 0
        self.latencies = []
        
        # Using deques for rolling window calculations
        self.throughput_window = deque(maxlen=60)  # 1-minute window
        self.send_rate_window = deque(maxlen=60)   # 1-minute window
        
        # Last timestamp for rate calculations
        self.last_throughput_time = self.start_time
        self.last_send_time = self.start_time

    def record_transaction_sent(self):
        self.transactions_sent += 1
        current_time = time.time()
        elapsed_time = current_time - self.last_send_time
        
        if elapsed_time >= 1.0:  # Calculate send rate every second
            send_rate = self.transactions_sent / (current_time - self.start_time)
            self.send_rate_window.append(send_rate)
            self.last_send_time = current_time

    def record_transaction_processed(self, latency):
        self.transactions_processed += 1
        self.latencies.append(latency)
        
        current_time = time.time()
        elapsed_time = current_time - self.last_throughput_time
        
        if elapsed_time >= 1.0:  # Calculate throughput every second
            throughput = self.transactions_processed / (current_time - self.start_time)
            self.throughput_window.append(throughput)
            self.last_throughput_time = current_time

    def record_transaction_failed(self):
        self.failed_transactions += 1

    def save_metrics(self):
        metrics = {
            'timing': {
                'start_time': self.start_time,
                'end_time': time.time(),
                'total_duration': time.time() - self.start_time
            },
            'transactions': {
                'sent': self.transactions_sent,
                'processed': self.transactions_processed,
                'failed': self.failed_transactions,
                'error_rate': self.failed_transactions / max(self.transactions_sent, 1)
            },
            'performance': {
                'average_latency': np.mean(self.latencies) if self.latencies else 0,
                'max_latency': max(self.latencies) if self.latencies else 0,
                'latency_std_dev': np.std(self.latencies) if self.latencies else 0,
                'average_throughput': np.mean(list(self.throughput_window)) if self.throughput_window else 0,
                'throughput_std_dev': np.std(list(self.throughput_window)) if self.throughput_window else 0,
                'average_send_rate': np.mean(list(self.send_rate_window)) if self.send_rate_window else 0,
                'send_rate_std_dev': np.std(list(self.send_rate_window)) if self.send_rate_window else 0
            }
        }

        with open(self.metrics_file, 'w') as f:
            json.dump(metrics, f, indent=4)
        
        return metrics

def wait_for_endorser(endorser_ip, port, timeout=30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((endorser_ip, port), timeout=5):
                logger.info(f"Endorser at {endorser_ip}:{port} is up")
                return True
        except (ConnectionRefusedError, socket.timeout):
            logger.info(f"Endorser at {endorser_ip}:{port} not available yet, retrying...")
            time.sleep(2)
    logger.error(f"Endorser at {endorser_ip}:{port} did not become available in time")
    return False

def start_client(endorser_ips, metrics_file):
    logger.info("Client process started")
    metrics = ClientMetrics(metrics_file)
    transaction_counter = 0

    try:
        while True:
            endorser_ip = random.choice(endorser_ips)
            if not wait_for_endorser(endorser_ip, ENDORSEMENT_PORT):
                logger.error(f"Skipping endorser at {endorser_ip}")
                metrics.record_transaction_failed()
                time.sleep(2)
                continue

            try:
                transaction_start_time = time.time()
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((endorser_ip, ENDORSEMENT_PORT))
                    
                    transaction_data = f'Transaction {transaction_counter} from Client'
                    transaction_hash = hashlib.sha256(transaction_data.encode()).hexdigest()
                    transaction = f'{transaction_hash}:{transaction_data}'

                    s.sendall(transaction.encode())
                    metrics.record_transaction_sent()
                    logger.info(f"Sent transaction with hash: {transaction_hash}")

                    data = s.recv(1024)
                    if data:
                        transaction_end_time = time.time()
                        latency = transaction_end_time - transaction_start_time
                        metrics.record_transaction_processed(latency)
                        logger.info(f"Received from endorser: {data.decode()}")
                    else:
                        logger.info("No response from endorser")
                        metrics.record_transaction_failed()

                    transaction_counter += 1

            except Exception as e:
                logger.error(f"Exception occurred while communicating with endorser: {e}")
                metrics.record_transaction_failed()

            # Periodically save metrics
            if transaction_counter % 10 == 0:
                metrics.save_metrics()

            time.sleep(0.1)  # Reduced sleep time for higher transaction rate

    except KeyboardInterrupt:
        logger.info("Client process interrupted by user")
    except Exception as e:
        logger.error(f"Exception in client process: {e}")
    finally:
        # Save final metrics before exiting
        final_metrics = metrics.save_metrics()
        logger.info("Final metrics saved")
        logger.info(f"Total transactions processed: {final_metrics['transactions']['processed']}")
        logger.info(f"Average latency: {final_metrics['performance']['average_latency']:.3f} seconds")
        sys.exit(0)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: client_node.py <endorser_ips> <metrics_file>")
        sys.exit(1)

    endorser_ips = sys.argv[1].split(',')
    metrics_file = sys.argv[2]

    start_client(endorser_ips, metrics_file)