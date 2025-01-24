#!/usr/bin/env python3

import socket
import threading
import sys
import logging
import signal
import random  # For selecting committer nodes

# Setup logging
logging.basicConfig(level=logging.INFO, format='[Orderer] %(message)s')
logger = logging.getLogger()

HOST = '0.0.0.0'    # Listen on all interfaces within the namespace
PORT = 7050         # Port for the orderer node to listen for endorsers
COMMITTER_PORT = 7051  # Port for connecting to committer nodes

def send_to_committer(transaction):
    # Randomly select a committer IP
    COMMITTER_IP = random.choice(committer_ips)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((COMMITTER_IP, COMMITTER_PORT))
            s.sendall(transaction.encode())
            logger.info(f"Sent transaction to committer at {COMMITTER_IP}:{COMMITTER_PORT}")
            response = s.recv(1024)
            if response:
                logger.info(f"Received from committer {COMMITTER_IP}: {response.decode()}")
            else:
                logger.info(f"No response from committer {COMMITTER_IP}")
    except Exception as e:
        logger.error(f"Failed to send transaction to committer {COMMITTER_IP}: {e}")

def handle_endorser(conn, addr):
    logger.info(f"Connection from endorser at {addr}")
    try:
        data = conn.recv(1024)
        if data:
            transaction = data.decode()
            # Extract hash and data
            transaction_parts = transaction.split(':', 1)
            if len(transaction_parts) != 2:
                logger.error("Invalid transaction format received from endorser")
                conn.sendall(b'Invalid transaction format')
                return
            transaction_hash, transaction_data = transaction_parts
            logger.info(f"Received endorsed transaction with hash: {transaction_hash}")
            # Simulate ordering service processing
            logger.info("Ordering the transaction")
            # Send a response back to the endorser
            conn.sendall(b'Orderer processed the transaction')

            # Forward the transaction to a committer node (include hash and data)
            send_to_committer(transaction)
    except Exception as e:
        logger.error(f"Exception while handling endorser {addr}: {e}")
    finally:
        conn.close()

def start_orderer(host, port):
    global server  # So it can be accessed in the signal handler
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server.bind((host, port))
        server.listen()
        logger.info("Orderer process started")
        logger.info(f"Listening for endorsers on {host}:{port}")

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info(f"Possible committers: {committer_ips}")

        while True:
            try:
                conn, addr = server.accept()
                # Handle each endorser connection in a new thread
                threading.Thread(target=handle_endorser, args=(conn, addr), daemon=True).start()
            except Exception as e:
                logger.error(f"Exception during accept: {e}")
                break
    except Exception as e:
        logger.error(f"Failed to start orderer server: {e}")
    finally:
        server.close()

def signal_handler(sig, frame):
    logger.info('Shutting down orderer...')
    server.close()
    sys.exit(0)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        logger.error("Usage: orderer_node.py <committer_ips>")
        sys.exit(1)
    committer_ips = sys.argv[1].split(',')
    start_orderer(HOST, PORT)