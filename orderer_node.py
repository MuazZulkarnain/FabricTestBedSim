#!/usr/bin/env python3

import socket
import threading
import sys
import logging
import signal

# Setup logging
logging.basicConfig(level=logging.INFO, format='[Orderer] %(message)s')
logger = logging.getLogger()

HOST = '0.0.0.0'    # Listen on all interfaces within the namespace
PORT = 7050         # Port for the orderer node to listen for endorsers
COMMITTER_PORT = 7051  # Port for connecting to the committer node

def send_to_committer(transaction_data):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((COMMITTER_IP, COMMITTER_PORT))
            s.sendall(transaction_data.encode())
            response = s.recv(1024)
            if response:
                logger.info(f"Received from committer: {response.decode()}")
            else:
                logger.info("No response from committer")
    except Exception as e:
        logger.error(f"Failed to send transaction to committer: {e}")

def handle_client(conn, addr):
    logger.info(f"Connection from {addr}")
    try:
        data = conn.recv(1024)
        if data:
            message = data.decode()
            logger.info(f"Received data from endorser: {message}")
            # Simulate ordering service processing
            conn.sendall(b'Orderer processed the transaction')

            # Forward the transaction to the committer
            send_to_committer(message)
    except Exception as e:
        logger.error(f"Exception while handling client {addr}: {e}")
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

        while True:
            try:
                conn, addr = server.accept()
                # Handle each endorser connection in a new thread
                threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
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
        logger.error("Usage: orderer_node.py <committer_ip>")
        sys.exit(1)
    COMMITTER_IP = sys.argv[1]
    logger.info(f"Connecting to committer at {COMMITTER_IP}:{COMMITTER_PORT}")

    start_orderer(HOST, PORT)