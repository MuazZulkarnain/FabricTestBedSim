#!/usr/bin/env python3

import json
import os
import numpy as np
from datetime import datetime

def aggregate_metrics(base_log_dir):
    """
    Aggregates metrics from all nodes and creates a comprehensive report.
    
    Args:
        base_log_dir (str): Base directory containing all log files
    
    Returns:
        dict: Aggregated metrics
    """
    try:
        # Initialize containers for metrics
        all_client_metrics = []
        all_endorser_metrics = []
        all_orderer_metrics = []
        all_committer_metrics = []

        # Collect metrics from all nodes
        for dir_name in os.listdir(base_log_dir):
            dir_path = os.path.join(base_log_dir, dir_name)
            if not os.path.isdir(dir_path):
                continue

            metrics_file = os.path.join(dir_path, f'{dir_name}_metrics.json')
            if not os.path.exists(metrics_file):
                continue

            with open(metrics_file, 'r') as f:
                metrics = json.load(f)

            # Categorize metrics based on node type
            if 'client' in dir_name:
                all_client_metrics.append(metrics)
            elif 'endorser' in dir_name:
                all_endorser_metrics.append(metrics)
            elif 'orderer' in dir_name:
                all_orderer_metrics.append(metrics)
            elif 'committer' in dir_name:
                all_committer_metrics.append(metrics)

        # Aggregate client metrics
        total_transactions = sum(m.get('transactions_sent', 0) for m in all_client_metrics)
        total_processed = sum(m.get('transactions_processed', 0) for m in all_client_metrics)
        total_failed = sum(m.get('failed_transactions', 0) for m in all_client_metrics)
        
        # Collect all latencies across clients
        all_latencies = []
        for metrics in all_client_metrics:
            all_latencies.extend(metrics.get('latencies', []))

        # Collect all throughput and send rate samples
        all_throughput_samples = []
        all_send_rate_samples = []
        for metrics in all_client_metrics:
            all_throughput_samples.extend(metrics.get('throughput_samples', []))
            all_send_rate_samples.extend(metrics.get('send_rate_samples', []))

        # Calculate aggregate metrics
        aggregate_metrics = {
            'timestamp': datetime.now().isoformat(),
            'transaction_metrics': {
                'total_transactions_sent': total_transactions,
                'total_transactions_processed': total_processed,
                'total_transactions_failed': total_failed,
                'error_rate': (total_failed / total_transactions) if total_transactions > 0 else 0,
            },
            'performance_metrics': {
                'average_latency': np.mean(all_latencies) if all_latencies else 0,
                'max_latency': max(all_latencies) if all_latencies else 0,
                'latency_std_dev': np.std(all_latencies) if all_latencies else 0,
                'average_throughput': np.mean(all_throughput_samples) if all_throughput_samples else 0,
                'throughput_std_dev': np.std(all_throughput_samples) if all_throughput_samples else 0,
                'average_send_rate': np.mean(all_send_rate_samples) if all_send_rate_samples else 0,
                'send_rate_std_dev': np.std(all_send_rate_samples) if all_send_rate_samples else 0
            },
            'node_statistics': {
                'total_clients': len(all_client_metrics),
                'total_endorsers': len(all_endorser_metrics),
                'total_orderers': len(all_orderer_metrics),
                'total_committers': len(all_committer_metrics)
            }
        }

        # Save aggregate metrics to file
        output_file = os.path.join(base_log_dir, f'aggregate_metrics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(output_file, 'w') as f:
            json.dump(aggregate_metrics, f, indent=4)

        # Also save a summary in human-readable format
        summary_file = os.path.join(base_log_dir, f'summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
        with open(summary_file, 'w') as f:
            f.write("=== Hyperledger Fabric Simulation Summary ===\n\n")
            f.write(f"Timestamp: {aggregate_metrics['timestamp']}\n\n")
            
            f.write("Transaction Statistics:\n")
            f.write(f"- Total Transactions Sent: {aggregate_metrics['transaction_metrics']['total_transactions_sent']}\n")
            f.write(f"- Total Transactions Processed: {aggregate_metrics['transaction_metrics']['total_transactions_processed']}\n")
            f.write(f"- Total Transactions Failed: {aggregate_metrics['transaction_metrics']['total_transactions_failed']}\n")
            f.write(f"- Error Rate: {aggregate_metrics['transaction_metrics']['error_rate']:.2%}\n\n")
            
            f.write("Performance Metrics:\n")
            f.write(f"- Average Latency: {aggregate_metrics['performance_metrics']['average_latency']:.3f} seconds\n")
            f.write(f"- Maximum Latency: {aggregate_metrics['performance_metrics']['max_latency']:.3f} seconds\n")
            f.write(f"- Average Throughput: {aggregate_metrics['performance_metrics']['average_throughput']:.2f} tps\n")
            f.write(f"- Average Send Rate: {aggregate_metrics['performance_metrics']['average_send_rate']:.2f} tps\n\n")
            
            f.write("Network Configuration:\n")
            f.write(f"- Number of Clients: {aggregate_metrics['node_statistics']['total_clients']}\n")
            f.write(f"- Number of Endorsers: {aggregate_metrics['node_statistics']['total_endorsers']}\n")
            f.write(f"- Number of Orderers: {aggregate_metrics['node_statistics']['total_orderers']}\n")
            f.write(f"- Number of Committers: {aggregate_metrics['node_statistics']['total_committers']}\n")

        return aggregate_metrics

    except Exception as e:
        print(f"Error aggregating metrics: {str(e)}")
        return None

if __name__ == '__main__':
    # This allows the script to be run standalone to aggregate metrics
    import sys
    if len(sys.argv) > 1:
        log_dir = sys.argv[1]
    else:
        log_dir = 'logs'
    
    aggregate_metrics(log_dir)