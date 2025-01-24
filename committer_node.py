#!/usr/bin/env python3

import socket
import threading
import logging
import signal
import sys
import time
import hashlib
import json
import numpy as np
from collections import deque

# Setup logging
logging.basicConfig(level=logging.INFO, format='[Committer %(name)s] %(message)s')
logger = logging.getLogger()
logger.name = ""

HOST = '0.0.0.0'
ORDERER_PORT = 7051
GOSSIP_PORT = 7053

class CommitterMetrics:
    def __init__(self, metrics_file):
        self.metrics_file = metrics_file
        self.start_time = time.time()
        self.lock = threading.Lock()
        
        # Transaction metrics
        self.transactions_received = 0
        self.transactions_committed = 0
        self.transactions_failed = 0
        self.commit_times = []
        
        # Throughput tracking
        self.throughput_window = deque(maxlen=60)
        self.last_throughput_time = self.start_time
        
        # Gossip metrics
        self.gossip_messages_sent = 0
        self.gossip_messages_received = 0
        self.gossip_failures = 0
        
        # Ledger metrics
        self.ledger_size = 0
        self.duplicate_transactions = 0

    def record_transaction_received(self):
        with self.lock:
            self.transactions_received += 1

    def record_transaction_committed(self, commit_time):
        with self.lock:
            self.transactions_committed += 1
            self.commit_times.append(commit_time)
            self.ledger_size = len(ledger)
            
            current_time = time.time()
            elapsed_time = current_time - self.last_throughput_time
            
            if elapsed_time >= 1.0:
                throughput = self.transactions_committed / (current_time - self.start_time)
                self.throughput_window.append(throughput)
                self.last_throughput_time = current_time

    def record_transaction_failed(self):
        with self.lock:
            self.transactions_failed += 1

    def record_gossip_sent(self):
        with self.lock:
            self.gossip_messages_sent += 1

    def record_gossip_received(self):
        with self.lock:
            self.gossip_messages_received += 1

    def record_gossip_failed(self):
        with self.lock:
            self.gossip_failures += 1

    def record_duplicate_transaction(self):
        with self.lock:
            self.duplicate_transactions += 1

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
                    'committed': self.transactions_committed,
                    'failed': self.transactions_failed,
                    'duplicates': self.duplicate_transactions,
                    'success_rate': self.transactions_committed / max(self.transactions_received, 1)
                },
                'performance': {
                    'average_commit_time': np.mean(self.commit_times) if self.commit_times else 0,
                    'max_commit_time': max(self.commit_times) if self.commit_times else 0,
                    'commit_time_std_dev': np.std(self.commit_times) if self.commit_times else 0,
                    'average_throughput': np.mean(list(self.throughput_window)) if self.throughput_window else 0,
                    'throughput_std_dev': np.std(list(self.throughput_window)) if self.throughput_window else 0
                },
                'gossip': {
                    'messages_sent': self.gossip_messages_sent,
                    'messages_received': self.gossip_messages_received,
                    'failures': self.gossip_failures
                },
                'ledger': {
                    'size': self.ledger_size,
                    'unique_transactions': len(ledger)
                }
            }

            with open(self.metrics_file, 'w') as f:
                json.dump(metrics, f, indent=4)
            
            return metrics

def handle_orderer(conn, addr, metrics):
    logger.info(f"Connection from orderer at {addr}")
    start_time = time.time()
    try:
        data = conn.recv(1024)
        if data:
            metrics.record_transaction_received()
            transaction = data.decode()
            transaction_parts = transaction.split(':', 1)
            if len(transaction_parts) != 2:
                logger.error("Invalid transaction format received from orderer")
                conn.sendall(b'Invalid transaction format')
                metrics.record_transaction_failed()
                return
            
            transaction_hash, transaction_data = transaction_parts
            logger.info(f"Received transaction with hash: {transaction_hash}")
            
            commit_transaction(transaction_hash, transaction_data, metrics)
            conn.sendall(b'Transaction committed')
            
    except Exception as e:
        metrics.record_transaction_failed()
        logger.error(f"Exception while handling transaction from {addr}: {e}")
    finally:
        conn.close()

def handle_gossip(conn, addr, metrics):
    try:
        data = conn.recv(1024)
        if data:
            metrics.record_gossip_received()
            transaction = data.decode()
            transaction_parts = transaction.split(':', 1)
            if len(transaction_parts) != 2:
                logger.error("Invalid transaction format received via gossip")
                metrics.record_transaction_failed()
                return
            
            transaction_hash, transaction_data = transaction_parts
            logger.info(f"Received transaction via gossip with hash: {transaction_hash}")
            
            if transaction_hash not in ledger:
                commit_transaction(transaction_hash, transaction_data, metrics)
            else:
                metrics.record_duplicate_transaction()
                logger.info(f"Transaction already in ledger: {transaction_hash}")
    except Exception as e:
        metrics.record_gossip_failed()
        logger.error(f"Exception while handling gossip from {addr}: {e}")
    finally:
        conn.close()

def commit_transaction(transaction_hash, transaction_data, metrics):
    start_time = time.time()
    if transaction_hash not in ledger:
        time.sleep(1)  # Simulate processing
        ledger[transaction_hash] = transaction_data
        commit_time = time.time() - start_time
        metrics.record_transaction_committed(commit_time)
        logger.info(f"Committed transaction: {transaction_hash}")
        gossip_transaction(transaction_hash, transaction_data, metrics)
    else:
        metrics.record_duplicate_transaction()
        logger.info(f"Transaction already committed: {transaction_hash}")

def gossip_transaction(transaction_hash, transaction_data, metrics):
    transaction = f"{transaction_hash}:{transaction_data}"
    for ip in committers_ips:
        threading.Thread(target=send_gossip, args=(ip, transaction, metrics), daemon=True).start()

def send_gossip(ip, transaction, metrics):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ip, GOSSIP_PORT))
            s.sendall(transaction.encode())
            metrics.record_gossip_sent()
            logger.info(f"Gossiped transaction to committer at {ip}:{GOSSIP_PORT}")
    except Exception as e:
        metrics.record_gossip_failed()
        logger.error(f"Failed to gossip transaction to committer {ip}: {e}")

# ... [Previous helper functions remain the same] ...

def start_committer(metrics_file):
    metrics = CommitterMetrics(metrics_file)
    
    # Start orderer listener
    def orderer_listener():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind((HOST, ORDERER_PORT))
            server.listen()
            logger.info(f"Listening for orderers on {HOST}:{ORDERER_PORT}")
            
            while True:
                conn, addr = server.accept()
                threading.Thread(target=handle_orderer, args=(conn, addr, metrics), daemon=True).start()
    
    # Start gossip listener
    def gossip_listener():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind((HOST, GOSSIP_PORT))
            server.listen()
            logger.info(f"Listening for gossip on {HOST}:{GOSSIP_PORT}")
            
            while True:
                conn, addr = server.accept()
                threading.Thread(target=handle_gossip, args=(conn, addr, metrics), daemon=True).start()
    
    # Start metrics saving thread
    def periodic_metrics_save():
        while True:
            time.sleep(10)
            metrics.save_metrics()
    
    # Start all threads
    threading.Thread(target=orderer_listener, daemon=True).start()
    threading.Thread(target=gossip_listener, daemon=True).start()
    threading.Thread(target=periodic_metrics_save, daemon=True).start()
    
    return metrics

def main():
    if len(sys.argv) != 3:
        logger.error("Usage: committer_node.py <committer_ips> <metrics_file>")
        sys.exit(1)

    global committers_ips, ledger
    committers_ips = sys.argv[1].split(',')
    metrics_file = sys.argv[2]
    ledger = {}

    own_ip = get_host_ip()
    committers_ips = [ip for ip in committers_ips if ip != own_ip]
    logger.name = own_ip
    logger.info(f"Other committers for gossip: {committers_ips}")

    metrics = start_committer(metrics_file)

    def cleanup(sig, frame):
        logger.info('Shutting down committer...')
        final_metrics = metrics.save_metrics()
        logger.info("Final metrics saved")
        logger.info(f"Total transactions committed: {final_metrics['transactions']['committed']}")
        logger.info(f"Final ledger size: {final_metrics['ledger']['size']}")
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    while True:
        time.sleep(1)

if __name__ == '__main__':
    main()