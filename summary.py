#!/usr/bin/env python3

import csv
import glob
import os

def aggregate_metrics():
    # Aggregate client metrics
    client_files = glob.glob('/home/ubuntu/FabricTestBedSim/logs/client*_metrics.csv')
    clients_summary = []
    for file in client_files:
        with open(file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            if rows:
                summary_row = rows[-1]  # Last row contains summary
                clients_summary.append(summary_row)
    
    # Aggregate committer metrics
    committer_files = glob.glob('/home/ubuntu/FabricTestBedSim/logs/committer*_metrics.csv')
    committers_summary = []
    for file in committer_files:
        with open(file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            if rows:
                summary_row = rows[-1]  # Last row contains summary
                committers_summary.append(summary_row)
    
    # Write aggregated summary to CSV
    with open('simulation_summary.csv', 'w', newline='') as csvfile:
        fieldnames = ['node', 'total_transactions', 'failed_transactions', 'total_time',
                      'average_send_rate', 'send_rate_std_dev', 'average_throughput',
                      'throughput_std_dev']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        # Write client summaries
        for summary in clients_summary:
            writer.writerow({
                'node': 'Client',
                'total_transactions': summary.get('total_transactions'),
                'failed_transactions': summary.get('failed_transactions'),
                'total_time': summary.get('total_time'),
                'average_send_rate': summary.get('average_send_rate'),
                'send_rate_std_dev': summary.get('send_rate_std_dev'),
                'average_throughput': '',
                'throughput_std_dev': ''
            })
        # Write committer summaries
        for summary in committers_summary:
            writer.writerow({
                'node': 'Committer',
                'total_transactions': summary.get('total_transactions'),
                'failed_transactions': '',
                'total_time': summary.get('total_time'),
                'average_send_rate': '',
                'send_rate_std_dev': '',
                'average_throughput': summary.get('average_throughput'),
                'throughput_std_dev': summary.get('throughput_std_dev')
            })

if __name__ == '__main__':
    aggregate_metrics()