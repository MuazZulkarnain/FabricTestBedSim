#!/usr/bin/env python3

import socket
import threading
import sys
import time
import random
import logging
import signal

# Setup logging
logging.basicConfig(level=logging.INFO, format='[Endorser] %(message)s')
logger = logging.getLogger()

ORDERER_PORT = 7050  # Port to connect to on orderer nodes (from endorser to orderer)
CLIENT_PORT = 7052   # Port to listen on for client connections (from client to endorser)

def send_to_orderer(transaction_data):
    # Randomly select an orderer IP
    orderer_ip = random.choice(orderer_ips)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((orderer_ip, ORDERER_PORT))
            s.sendall(transaction_data.encode())
            logger.info(f"Sent transaction to orderer at {orderer_ip}:{ORDERER_PORT}")
            response = s.recv(1024)
            if response:
                logger.info(f"Received from orderer: {response.decode()}")
            else:
                logger.info("No response from orderer")
    except Exception as e:
        logger.error(f"Failed to send transaction to orderer {orderer_ip}: {e}")

def handle_client(conn, addr):
    logger.info(f"Connection from client at {addr}")
    try:
        data = conn.recv(1024)
        if data:
            transaction_proposal = data.decode()
            logger.info(f"Received transaction proposal: {transaction_proposal}")
            # Simulate endorsing the transaction
            logger.info("Endorsing the transaction")
            # Send a response back to the client
            conn.sendall(b'Transaction endorsed by Endorser')
            # Forward the endorsed transaction to an orderer
            send_to_orderer(transaction_proposal)
    except Exception as e:
        logger.error(f"Exception while handling client {addr}: {e}")
    finally:
        conn.close()

def start_endorser():
    global server  # So it can be accessed in the signal handler
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server.bind(('0.0.0.0', CLIENT_PORT))
        server.listen()
        logger.info("Endorser process started")
        logger.info(f"Listening for clients on port {CLIENT_PORT}")

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info(f"Possible orderers: {orderer_ips}")

        while True:
            try:
                conn, addr = server.accept()
                # Handle each client connection in a new thread
                threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
            except Exception as e:
                logger.error(f"Exception during accept: {e}")
                break
    except Exception as e:
        logger.error(f"Failed to start endorser server: {e}")
    finally:
        server.close()

def signal_handler(sig, frame):
    logger.info('Shutting down endorser...')
    server.close()
    sys.exit(0)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: endorser_node.py <orderer_ips>")
        sys.exit(1)
    orderer_ips = sys.argv[1].split(',')

    start_endorser()