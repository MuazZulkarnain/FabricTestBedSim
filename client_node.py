#!/usr/bin/env python3

import socket
import sys
import time
import random
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='[Client] %(message)s')
logger = logging.getLogger()

ENDORSEMENT_PORT = 7052  # Port where endorsers are listening for clients

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

def start_client(endorser_ips):
    logger.info("Client process started")
    try:
        while True:
            # Randomly select an endorser IP
            endorser_ip = random.choice(endorser_ips)
            # Wait for the selected endorser to become available
            if not wait_for_endorser(endorser_ip, ENDORSEMENT_PORT):
                logger.error(f"Skipping endorser at {endorser_ip}")
                time.sleep(2)
                continue  # Try another endorser

            logger.info(f"Connecting to endorser at {endorser_ip}:{ENDORSEMENT_PORT}")
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((endorser_ip, ENDORSEMENT_PORT))
                    # Send transaction proposal
                    transaction_proposal = 'Transaction Proposal from Client'
                    s.sendall(transaction_proposal.encode())
                    logger.info(f"Sent: {transaction_proposal}")

                    # Receive response from endorser
                    data = s.recv(1024)
                    if data:
                        logger.info(f"Received from endorser: {data.decode()}")
                    else:
                        logger.info("No response from endorser")
            except Exception as e:
                logger.error(f"Exception occurred while communicating with endorser: {e}")

            # Wait before sending the next transaction
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Client process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Exception in client process: {e}")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: client_node.py <endorser_ips>")
        sys.exit(1)
    endorser_ips = sys.argv[1].split(',')

    start_client(endorser_ips)