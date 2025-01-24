#!/usr/bin/env python3

import time
import json
import numpy as np
from datetime import datetime

class MetricsLogger:
    def __init__(self, base_log_dir):
        self.base_log_dir = base_log_dir
        self.simulation_start_time = time.time()
        self.metrics = {
            'timing': {
                'start_time': self.simulation_start_time,
                'end_time': None,
                'sending_completion_time': None,
                'processing_completion_time': None
            },
            'transactions': {
                'total_processed': 0,
                'total_failed': 0,
                'throughput_samples': [],
                'send_rate_samples': [],
                'latencies': []
            }
        }

    def log_transaction_processed(self, latency):
        self.metrics['transactions']['total_processed'] += 1
        self.metrics['transactions']['latencies'].append(latency)

    def log_transaction_failed(self):
        self.metrics['transactions']['total_failed'] += 1

    def log_throughput_sample(self, transactions_per_second):
        self.metrics['transactions']['throughput_samples'].append(transactions_per_second)

    def log_send_rate_sample(self, sends_per_second):
        self.metrics['transactions']['send_rate_samples'].append(sends_per_second)

    def mark_sending_complete(self):
        self.metrics['timing']['sending_completion_time'] = time.time()

    def mark_processing_complete(self):
        self.metrics['timing']['processing_completion_time'] = time.time()

    def finalize_metrics(self):
        try:
            self.metrics['timing']['end_time'] = time.time()
            
            # Calculate total transactions
            total_transactions = (self.metrics['transactions']['total_processed'] + 
                                self.metrics['transactions']['total_failed'])

            # Calculate final metrics with safety checks
            final_metrics = {
                'timing_metrics': {
                    'total_simulation_time': self.metrics['timing']['end_time'] - self.metrics['timing']['start_time'],
                    'sending_time': (self.metrics['timing']['sending_completion_time'] or time.time()) - 
                                  self.metrics['timing']['start_time'],
                    'processing_time': (self.metrics['timing']['processing_completion_time'] or time.time()) - 
                                     self.metrics['timing']['start_time']
                },
                'transaction_metrics': {
                    'total_processed': self.metrics['transactions']['total_processed'],
                    'total_failed': self.metrics['transactions']['total_failed'],
                    'error_rate': (self.metrics['transactions']['total_failed'] / total_transactions 
                                 if total_transactions > 0 else 0.0),
                    'average_throughput': np.mean(self.metrics['transactions']['throughput_samples']) 
                                        if self.metrics['transactions']['throughput_samples'] else 0.0,
                    'throughput_std_dev': np.std(self.metrics['transactions']['throughput_samples']) 
                                        if self.metrics['transactions']['throughput_samples'] else 0.0,
                    'average_send_rate': np.mean(self.metrics['transactions']['send_rate_samples']) 
                                       if self.metrics['transactions']['send_rate_samples'] else 0.0,
                    'send_rate_std_dev': np.std(self.metrics['transactions']['send_rate_samples']) 
                                       if self.metrics['transactions']['send_rate_samples'] else 0.0,
                    'average_latency': np.mean(self.metrics['transactions']['latencies']) 
                                     if self.metrics['transactions']['latencies'] else 0.0,
                    'max_latency': max(self.metrics['transactions']['latencies']) 
                                  if self.metrics['transactions']['latencies'] else 0.0
                }
            }

            # Save metrics to file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            metrics_file = f"{self.base_log_dir}/metrics_{timestamp}.json"
            with open(metrics_file, 'w') as f:
                json.dump(final_metrics, f, indent=4)

            # Also save a human-readable summary
            summary_file = f"{self.base_log_dir}/summary_{timestamp}.txt"
            with open(summary_file, 'w') as f:
                f.write("=== Fabric Simulation Summary ===\n\n")
                f.write(f"Simulation Duration: {final_metrics['timing_metrics']['total_simulation_time']:.2f} seconds\n")
                f.write(f"Total Transactions Processed: {final_metrics['transaction_metrics']['total_processed']}\n")
                f.write(f"Total Transactions Failed: {final_metrics['transaction_metrics']['total_failed']}\n")
                f.write(f"Error Rate: {final_metrics['transaction_metrics']['error_rate']:.2%}\n")
                f.write(f"Average Throughput: {final_metrics['transaction_metrics']['average_throughput']:.2f} tps\n")
                f.write(f"Average Latency: {final_metrics['transaction_metrics']['average_latency']:.3f} seconds\n")
                f.write(f"Maximum Latency: {final_metrics['transaction_metrics']['max_latency']:.3f} seconds\n")

            return final_metrics

        except Exception as e:
            print(f"Error in finalize_metrics: {str(e)}")
            return None

if __name__ == '__main__':
    # This allows the script to be run standalone for testing
    logger = MetricsLogger('test_logs')
    # Add some test data
    logger.log_transaction_processed(0.1)
    logger.log_transaction_failed()
    logger.log_throughput_sample(10)
    logger.mark_sending_complete()
    logger.mark_processing_complete()
    # Finalize and save metrics
    logger.finalize_metrics()