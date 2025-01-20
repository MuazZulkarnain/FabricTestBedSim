#!/usr/bin/env python3

import socket
import threading
import logging
import signal
import sys

# Setup logging
logging.basicConfig(level=logging.INFO, format='[Committer] %(message)s')
logger = logging.getLogger()

HOST = '0.0.0.0'    # Listen on all interfaces within the namespace
PORT = 7051         # Port for the committer node to listen for orderers

def handle_orderer(conn, addr):
    logger.info(f"Connection from orderer at {addr}")
    try:
        data = conn.recv(1024)
        if data:
            transaction = data.decode()
            logger.info(f"Received transaction: {transaction}")
            # Simulate committing the transaction
            logger.info("Committing the transaction to the ledger")
            # Send a response back to the orderer
            conn.sendall(b'Transaction committed')
    except Exception as e:
        logger.error(f"Exception while handling transaction from {addr}: {e}")
    finally:
        conn.close()

def start_committer(host, port):
    global server  # So it can be accessed in the signal handler
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server.bind((host, port))
        server.listen()
        logger.info("Committer process started")
        logger.info(f"Listening for orderers on {host}:{port}")

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        while True:
            try:
                conn, addr = server.accept()
                # Handle each orderer connection in a new thread
                threading.Thread(target=handle_orderer, args=(conn, addr), daemon=True).start()
            except Exception as e:
                logger.error(f"Exception during accept: {e}")
                break
    except Exception as e:
        logger.error(f"Failed to start committer server: {e}")
    finally:
        server.close()

def signal_handler(sig, frame):
    logger.info('Shutting down committer...')
    server.close()
    sys.exit(0)

if __name__ == '__main__':
    start_committer(HOST, PORT)