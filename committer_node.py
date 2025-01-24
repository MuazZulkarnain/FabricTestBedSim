#!/usr/bin/env python3

import socket
import threading
import logging
import signal
import sys
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='[Committer %(name)s] %(message)s')
logger = logging.getLogger()

HOST = '0.0.0.0'    # Listen on all interfaces within the namespace
ORDERER_PORT = 7051         # Port for the committer node to listen for orderers
GOSSIP_PORT = 7053          # Port for gossip communication with other committers

ledger = []  # Ledger to store committed transactions
committers_ips = []  # List of other committer IPs for gossip

def handle_orderer(conn, addr):
    logger.info(f"Connection from orderer at {addr}")
    try:
        data = conn.recv(1024)
        if data:
            transaction = data.decode()
            logger.info(f"Received transaction: {transaction}")
            # Simulate committing the transaction
            commit_transaction(transaction)
            # Send a response back to the orderer
            conn.sendall(b'Transaction committed')
    except Exception as e:
        logger.error(f"Exception while handling transaction from {addr}: {e}")
    finally:
        conn.close()

def handle_gossip(conn, addr):
    try:
        data = conn.recv(1024)
        if data:
            transaction = data.decode()
            logger.info(f"Received transaction via gossip: {transaction}")
            # Add the transaction to the ledger if not already present
            if transaction not in ledger:
                ledger.append(transaction)
                logger.info(f"Transaction added to ledger via gossip: {transaction}")
                # Optionally, gossip the transaction further (to prevent loops, we can add logic)
                # gossip_transaction(transaction)
            else:
                logger.info(f"Transaction already in ledger: {transaction}")
    except Exception as e:
        logger.error(f"Exception while handling gossip from {addr}: {e}")
    finally:
        conn.close()

def commit_transaction(transaction):
    if transaction not in ledger:
        # Simulate some processing time
        time.sleep(1)
        ledger.append(transaction)
        logger.info(f"Committed transaction: {transaction}")
        # Gossip the new transaction to other committers
        gossip_transaction(transaction)
    else:
        logger.info(f"Transaction already committed: {transaction}")

def gossip_transaction(transaction):
    for ip in committers_ips:
        threading.Thread(target=send_gossip, args=(ip, transaction), daemon=True).start()

def send_gossip(ip, transaction):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ip, GOSSIP_PORT))
            s.sendall(transaction.encode())
            logger.info(f"Gossiped transaction to committer at {ip}:{GOSSIP_PORT}")
    except Exception as e:
        logger.error(f"Failed to gossip transaction to committer {ip}: {e}")

def start_orderer_listener():
    global orderer_server  # So it can be accessed in the signal handler
    orderer_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        orderer_server.bind((HOST, ORDERER_PORT))
        orderer_server.listen()
        logger.info(f"Listening for orderers on {HOST}:{ORDERER_PORT}")

        while True:
            try:
                conn, addr = orderer_server.accept()
                # Handle each orderer connection in a new thread
                threading.Thread(target=handle_orderer, args=(conn, addr), daemon=True).start()
            except Exception as e:
                logger.error(f"Exception during accept on orderer listener: {e}")
                break
    except Exception as e:
        logger.error(f"Failed to start orderer listener: {e}")
    finally:
        orderer_server.close()

def start_gossip_listener():
    global gossip_server  # So it can be accessed in the signal handler
    gossip_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        gossip_server.bind((HOST, GOSSIP_PORT))
        gossip_server.listen()
        logger.info(f"Listening for gossip on {HOST}:{GOSSIP_PORT}")

        while True:
            try:
                conn, addr = gossip_server.accept()
                # Handle each gossip connection in a new thread
                threading.Thread(target=handle_gossip, args=(conn, addr), daemon=True).start()
            except Exception as e:
                logger.error(f"Exception during accept on gossip listener: {e}")
                break
    except Exception as e:
        logger.error(f"Failed to start gossip listener: {e}")
    finally:
        gossip_server.close()

def signal_handler(sig, frame):
    logger.info('Shutting down committer...')
    orderer_server.close()
    gossip_server.close()
    sys.exit(0)

def main():
    if len(sys.argv) != 2:
        logger.error("Usage: committer_node.py <committer_ips>")
        sys.exit(1)

    global committers_ips
    committers_ips = sys.argv[1].split(',')

    # Remove own IP from the list to prevent sending gossip to itself
    own_ip = get_host_ip()
    committers_ips = [ip for ip in committers_ips if ip != own_ip]
    logger.info(f"Other committers for gossip: {committers_ips}")

    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start orderer listener and gossip listener in separate threads
    threading.Thread(target=start_orderer_listener, daemon=True).start()
    threading.Thread(target=start_gossip_listener, daemon=True).start()

    # Keep the main thread alive
    while True:
        time.sleep(1)

def get_host_ip():
    # Get the IP address of the host
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't matter if the IP is reachable
        s.connect(('10.0.0.0', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

if __name__ == '__main__':
    main()