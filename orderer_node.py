#!/usr/bin/env python3

import socket
import threading
import sys
import logging
import signal
import random
import time
import json
import numpy as np
from collections import deque

# Setup logging
logging.basicConfig(level=logging.INFO, format='[Orderer] %(message)s')
logger = logging.getLogger()

HOST = '0.0.0.0'
PORT = 7050
COMMITTER_PORT = 7051

class OrdererMetrics:
    def __init__(self, metrics_file):
        self.metrics_file = metrics_file
        self.start_time = time.time()
        self.lock = threading.Lock()
        
        # Transaction metrics
        self.transactions_received = 0
        self.transactions_processed = 0
        self.transactions_failed = 0
        self.processing_times = []
        
        # Throughput tracking
        self.throughput_window = deque(maxlen=60)  # 1-minute window
        self.last_throughput_time = self.start_time
        
        # Committer communication metrics
        self.committer_success = 0
        self.committer_failures = 0
        self.committer_latencies = []

    def record_transaction_received(self):
        with self.lock:
            self.transactions_received += 1

    def record_transaction_processed(self, processing_time):
        with self.lock:
            self.transactions_processed += 1
            self.processing_times.append(processing_time)
            
            current_time = time.time()
            elapsed_time = current_time - self.last_throughput_time
            
            if elapsed_time >= 1.0:
                throughput = self.transactions_processed / (current_time - self.start_time)
                self.throughput_window.append(throughput)
                self.last_throughput_time = current_time

    def record_transaction_failed(self):
        with self.lock:
            self.transactions_failed += 1

    def record_committer_communication(self, success, latency=None):
        with self.lock:
            if success:
                self.committer_success += 1
                if latency is not None:
                    self.committer_latencies.append(latency)
            else:
                self.committer_failures += 1

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
                    'processed': self.transactions_processed,
                    'failed': self.transactions_failed,
                    'success_rate': self.transactions_processed / max(self.transactions_received, 1)
                },
                'performance': {
                    'average_processing_time': np.mean(self.processing_times) if self.processing_times else 0,
                    'max_processing_time': max(self.processing_times) if self.processing_times else 0,
                    'processing_time_std_dev': np.std(self.processing_times) if self.processing_times else 0,
                    'average_throughput': np.mean(list(self.throughput_window)) if self.throughput_window else 0,
                    'throughput_std_dev': np.std(list(self.throughput_window)) if self.throughput_window else 0
                },
                'committer_metrics': {
                    'successful_forwards': self.committer_success,
                    'failed_forwards': self.committer_failures,
                    'average_committer_latency': np.mean(self.committer_latencies) if self.committer_latencies else 0,
                    'max_committer_latency': max(self.committer_latencies) if self.committer_latencies else 0
                }
            }

            with open(self.metrics_file, 'w') as f:
                json.dump(metrics, f, indent=4)
            
            return metrics

def send_to_committer(transaction, metrics):
    COMMITTER_IP = random.choice(committer_ips)
    start_time = time.time()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((COMMITTER_IP, COMMITTER_PORT))
            s.sendall(transaction.encode())
            logger.info(f"Sent transaction to committer at {COMMITTER_IP}:{COMMITTER_PORT}")
            response = s.recv(1024)
            if response:
                latency = time.time() - start_time
                metrics.record_committer_communication(True, latency)
                logger.info(f"Received from committer {COMMITTER_IP}: {response.decode()}")
            else:
                metrics.record_committer_communication(False)
                logger.info(f"No response from committer {COMMITTER_IP}")
    except Exception as e:
        metrics.record_committer_communication(False)
        logger.error(f"Failed to send transaction to committer {COMMITTER_IP}: {e}")

def handle_endorser(conn, addr, metrics):
    logger.info(f"Connection from endorser at {addr}")
    start_time = time.time()
    try:
        data = conn.recv(1024)
        if data:
            metrics.record_transaction_received()
            transaction = data.decode()
            transaction_parts = transaction.split(':', 1)
            if len(transaction_parts) != 2:
                logger.error("Invalid transaction format received from endorser")
                conn.sendall(b'Invalid transaction format')
                metrics.record_transaction_failed()
                return
            
            transaction_hash, transaction_data = transaction_parts
            logger.info(f"Received endorsed transaction with hash: {transaction_hash}")
            
            # Process transaction
            logger.info("Ordering the transaction")
            conn.sendall(b'Orderer processed the transaction')
            
            # Record processing time and success
            processing_time = time.time() - start_time
            metrics.record_transaction_processed(processing_time)
            
            # Forward to committer
            send_to_committer(transaction, metrics)
    except Exception as e:
        metrics.record_transaction_failed()
        logger.error(f"Exception while handling endorser {addr}: {e}")
    finally:
        conn.close()

def start_orderer(host, port, metrics_file):
    global server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    metrics = OrdererMetrics(metrics_file)
    
    try:
        server.bind((host, port))
        server.listen()
        logger.info("Orderer process started")
        logger.info(f"Listening for endorsers on {host}:{port}")

        signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, metrics))
        signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, metrics))

        logger.info(f"Possible committers: {committer_ips}")

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
                threading.Thread(target=handle_endorser, args=(conn, addr, metrics), daemon=True).start()
            except Exception as e:
                logger.error(f"Exception during accept: {e}")
                break
    except Exception as e:
        logger.error(f"Failed to start orderer server: {e}")
    finally:
        server.close()
        metrics.save_metrics()  # Save final metrics

def signal_handler(sig, frame, metrics):
    logger.info('Shutting down orderer...')
    final_metrics = metrics.save_metrics()
    logger.info("Final metrics saved")
    logger.info(f"Total transactions processed: {final_metrics['transactions']['processed']}")
    logger.info(f"Average processing time: {final_metrics['performance']['average_processing_time']:.3f} seconds")
    server.close()
    sys.exit(0)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        logger.error("Usage: orderer_node.py <committer_ips> <metrics_file>")
        sys.exit(1)
    
    committer_ips = sys.argv[1].split(',')
    metrics_file = sys.argv[2]
    start_orderer(HOST, PORT, metrics_file)