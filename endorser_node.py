#!/usr/bin/env python3

import socket
import sys
import time
import random
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='[Endorser] %(message)s')
logger = logging.getLogger()

ORDERER_PORT = 7050  # Port to connect to on orderer nodes

def wait_for_orderer(orderer_ip, port, timeout=30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((orderer_ip, port), timeout=5):
                logger.info(f"Orderer at {orderer_ip}:{port} is up")
                return True
        except (ConnectionRefusedError, socket.timeout):
            logger.info(f"Orderer at {orderer_ip}:{port} not available yet, retrying...")
            time.sleep(2)
    logger.error(f"Orderer at {orderer_ip}:{port} did not become available in time")
    return False

def start_endorser(orderer_ips):
    logger.info("Endorser process started")
    try:
        while True:
            # Randomly select an orderer IP
            orderer_ip = random.choice(orderer_ips)
            # Wait for the selected orderer to become available
            if not wait_for_orderer(orderer_ip, ORDERER_PORT):
                logger.error(f"Skipping orderer at {orderer_ip}")
                time.sleep(2)
                continue  # Try another orderer

            logger.info(f"Connecting to orderer at {orderer_ip}:{ORDERER_PORT}")
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((orderer_ip, ORDERER_PORT))
                    # Send transaction proposal
                    transaction_proposal = 'Transaction Proposal from Endorser'
                    s.sendall(transaction_proposal.encode())
                    logger.info(f"Sent: {transaction_proposal}")

                    # Receive response from orderer
                    data = s.recv(1024)
                    if data:
                        logger.info(f"Received from orderer: {data.decode()}")
                    else:
                        logger.info("No response from orderer")
            except Exception as e:
                logger.error(f"Exception occurred while communicating with orderer: {e}")

            # Wait before sending the next transaction
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Endorser process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Exception in endorser process: {e}")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: endorser_node.py <orderer_ips>")
        sys.exit(1)
    orderer_ips = sys.argv[1].split(',')

    start_endorser(orderer_ips)