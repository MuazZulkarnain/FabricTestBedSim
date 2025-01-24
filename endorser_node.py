#!/usr/bin/env python3

import socket
import threading
import sys
import time
import random
import logging
import signal
import json
import numpy as np
from collections import deque

# Setup logging
logging.basicConfig(level=logging.INFO, format='[Endorser] %(message)s')
logger = logging.getLogger()

ORDERER_PORT = 7050
CLIENT_PORT = 7052

class EndorserMetrics:
    def __init__(self, metrics_file):
        self.metrics_file = metrics_file
        self.start_time = time.time()
        self.lock = threading.Lock()
        
        # Transaction metrics
        self.transactions_received = 0
        self.transactions_endorsed = 0
        self.transactions_failed = 0
        self.endorsement_times = []
        
        # Throughput tracking
        self.throughput_window = deque(maxlen=60)  # 1-minute window
        self.last_throughput_time = self.start_time
        
        # Orderer communication metrics
        self.orderer_success = 0
        self.orderer_failures = 0
        self.orderer_latencies = []
        
        # Client communication metrics
        self.client_response_times = []

    def record_transaction_received(self):
        with self.lock:
            self.transactions_received += 1

    def record_transaction_endorsed(self, endorsement_time):
        with self.lock:
            self.transactions_endorsed += 1
            self.endorsement_times.append(endorsement_time)
            
            current_time = time.time()
            elapsed_time = current_time - self.last_throughput_time
            
            if elapsed_time >= 1.0:
                throughput = self.transactions_endorsed / (current_time - self.start_time)
                self.throughput_window.append(throughput)
                self.last_throughput_time = current_time

    def record_transaction_failed(self):
        with self.lock:
            self.transactions_failed += 1

    def record_orderer_communication(self, success, latency=None):
        with self.lock:
            if success:
                self.orderer_success += 1
                if latency is not None:
                    self.orderer_latencies.append(latency)
            else:
                self.orderer_failures += 1

    def record_client_response_time(self, response_time):
        with self.lock:
            self.client_response_times.append(response_time)

    def save_metrics(self):
        with self.lock:
            metrics = {
                'timing': {
                    'start_time': self.start_time,
                    'current_time': time.time(),
                    'uptime': time.time() - self.start_time
                },
                'transactions': {
                    'received': self.transactions_received,
                    'endorsed': self.transactions_endorsed,
                    'failed': self.transactions_failed,
                    'success_rate': self.transactions_endorsed / max(self.transactions_received, 1)
                },
                'performance': {
                    'average_endorsement_time': np.mean(self.endorsement_times) if self.endorsement_times else 0,
                    'max_endorsement_time': max(self.endorsement_times) if self.endorsement_times else 0,
                    'endorsement_time_std_dev': np.std(self.endorsement_times) if self.endorsement_times else 0,
                    'average_throughput': np.mean(list(self.throughput_window)) if self.throughput_window else 0,
                    'throughput_std_dev': np.std(list(self.throughput_window)) if self.throughput_window else 0
                },
                'orderer_metrics': {
                    'successful_forwards': self.orderer_success,
                    'failed_forwards': self.orderer_failures,
                    'average_orderer_latency': np.mean(self.orderer_latencies) if self.orderer_latencies else 0,
                    'max_orderer_latency': max(self.orderer_latencies) if self.orderer_latencies else 0
                },
                'client_metrics': {
                    'average_response_time': np.mean(self.client_response_times) if self.client_response_times else 0,
                    'max_response_time': max(self.client_response_times) if self.client_response_times else 0,
                    'response_time_std_dev': np.std(self.client_response_times) if self.client_response_times else 0
                }
            }

            with open(self.metrics_file, 'w') as f:
                json.dump(metrics, f, indent=4)
            
            return metrics

def send_to_orderer(transaction, metrics):
    orderer_ip = random.choice(orderer_ips)
    start_time = time.time()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((orderer_ip, ORDERER_PORT))
            s.sendall(transaction.encode())
            logger.info(f"Sent transaction to orderer at {orderer_ip}:{ORDERER_PORT}")
            response = s.recv(1024)
            if response:
                latency = time.time() - start_time
                metrics.record_orderer_communication(True, latency)
                logger.info(f"Received from orderer: {response.decode()}")
            else:
                metrics.record_orderer_communication(False)
                logger.info("No response from orderer")
    except Exception as e:
        metrics.record_orderer_communication(False)
        logger.error(f"Failed to send transaction to orderer {orderer_ip}: {e}")

def handle_client(conn, addr, metrics):
    logger.info(f"Connection from client at {addr}")
    start_time = time.time()
    try:
        data = conn.recv(1024)
        if data:
            metrics.record_transaction_received()
            transaction = data.decode()
            transaction_parts = transaction.split(':', 1)
            if len(transaction_parts) != 2:
                logger.error("Invalid transaction format received from client")
                conn.sendall(b'Invalid transaction format')
                metrics.record_transaction_failed()
                return
            
            transaction_hash, transaction_data = transaction_parts
            logger.info(f"Received transaction with hash: {transaction_hash}")

            # Endorse transaction
            logger.info("Endorsing the transaction")
            conn.sendall(b'Transaction endorsed by Endorser')
            
            # Record metrics
            endorsement_time = time.time() - start_time
            metrics.record_transaction_endorsed(endorsement_time)
            metrics.record_client_response_time(endorsement_time)
            
            # Forward to orderer
            send_to_orderer(transaction, metrics)
    except Exception as e:
        metrics.record_transaction_failed()
        logger.error(f"Exception while handling client {addr}: {e}")
    finally:
        conn.close()

def start_endorser(metrics_file):
    global server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    metrics = EndorserMetrics(metrics_file)
    
    try:
        server.bind(('0.0.0.0', CLIENT_PORT))
        server.listen()
        logger.info("Endorser process started")
        logger.info(f"Listening for clients on port {CLIENT_PORT}")

        signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, metrics))
        signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, metrics))

        logger.info(f"Possible orderers: {orderer_ips}")

        # Start metrics saving thread
        def periodic_metrics_save():
            while True:
                time.sleep(10)  # Save metrics every 10 seconds
                metrics.save_metrics()

        metrics_thread = threading.Thread(target=periodic_metrics_save, daemon=True)
        metrics_thread.start()

        while True:
            try:
                conn, addr = server.accept()
                threading.Thread(target=handle_client, args=(conn, addr, metrics), daemon=True).start()
            except Exception as e:
                logger.error(f"Exception during accept: {e}")
                break
    except Exception as e:
        logger.error(f"Failed to start endorser server: {e}")
    finally:
        server.close()
        metrics.save_metrics()  # Save final metrics

def signal_handler(sig, frame, metrics):
    logger.info('Shutting down endorser...')
    final_metrics = metrics.save_metrics()
    logger.info("Final metrics saved")
    logger.info(f"Total transactions endorsed: {final_metrics['transactions']['endorsed']}")
    logger.info(f"Average endorsement time: {final_metrics['performance']['average_endorsement_time']:.3f} seconds")
    server.close()
    sys.exit(0)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: endorser_node.py <orderer_ips> <metrics_file>")
        sys.exit(1)
    
    orderer_ips = sys.argv[1].split(',')
    metrics_file = sys.argv[2]
    start_endorser(metrics_file)