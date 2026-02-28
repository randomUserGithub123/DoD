# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean
import random

from benchmark.utils import Print


class FairnessParseError(Exception):
    pass


class FairnessLogParser:
    def __init__(self, clients, primaries, workers, faults=0):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers =  len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise FairnessParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples = zip(*results)
        self.misses = sum(misses)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise FairnessParseError(f'Failed to parse nodes\' logs: {e}')
        proposals, commits, self.configs, primary_ips = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise FairnessParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips, all_batch_maker_tx_uids, all_global_dependency_graph_tx_uids, all_global_order_maker_tx_uids, all_parallel_execution_tx_uids, all_processor_tx_uids, all_tx_finalized = zip(*results)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        # Collect all TX_FINALIZED transactions with their timestamps across all workers
        self.tx_finalized = {}
        for finalized_dict in all_tx_finalized:
            for tx_uid, timestamp in finalized_dict.items():
                # Keep the earliest timestamp for each transaction (in case of duplicates)
                if tx_uid not in self.tx_finalized or timestamp < self.tx_finalized[tx_uid]:
                    self.tx_finalized[tx_uid] = timestamp
        
        print(f"Number of unique TX_FINALIZED transactions: {len(self.tx_finalized)}")
        print(f"Number of batch maker transactions: {len(all_batch_maker_tx_uids[0])}")
        print(f"Number of processor transactions: {len(all_processor_tx_uids[0])}")
        print(f"Number of global dependency graph transactions: {len(all_global_dependency_graph_tx_uids[0])}")
        print(f"Number of global order maker transactions: {len(all_global_order_maker_tx_uids[0])}")
        print(f"Number of parallel execution transactions: {len(all_parallel_execution_tx_uids[0])}")

        worker_bm_tx_uids = all_batch_maker_tx_uids[0]
        worker_gdg_tx_uids = all_global_dependency_graph_tx_uids[0]
        worker_gom_tx_uids = all_global_order_maker_tx_uids[0]
        worker_pe_tx_uids = all_parallel_execution_tx_uids[0]
        worker_processor_tx_uids = all_processor_tx_uids[0]
        worker_tx_finalized = all_tx_finalized[0]

        txs_not_found_in_gdg = 0
        txs_not_found_in_gom = 0
        txs_not_found_in_pe = 0
        txs_not_found_in_processor = 0
        txs_not_found_in_finalized = 0
        for tx_uid in worker_bm_tx_uids:
            if tx_uid not in worker_gdg_tx_uids:
                txs_not_found_in_gdg += 1
            if tx_uid not in worker_gom_tx_uids:
                txs_not_found_in_gom += 1
            if tx_uid not in worker_pe_tx_uids:
                txs_not_found_in_pe += 1
            if tx_uid not in worker_processor_tx_uids:
                txs_not_found_in_processor += 1
            if tx_uid not in worker_tx_finalized:
                txs_not_found_in_finalized += 1
        
        print(f"Number of transactions not found in global dependency graph: {txs_not_found_in_gdg}")
        print(f"Number of transactions not found in global order maker: {txs_not_found_in_gom}")
        print(f"Number of transactions not found in parallel execution: {txs_not_found_in_pe}")
        print(f"Number of transactions not found in processor: {txs_not_found_in_processor}")
        print(f"Number of transactions not found in TX_FINALIZED logs: {txs_not_found_in_finalized}")

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise FairnessParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* fairness Sending tx (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise FairnessParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        configs = {
            'header_size': int(
                search(r'Header size .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return proposals, commits, configs, ip

    def _parse_workers(self, log):
        # if search(r'(?:panic|Error)', log) is not None:
        #     raise FairnessParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        tmp = findall(r'batch_maker::seal : tx_uid to store = (\d+)', log)
        all_batch_maker_tx_uids = {int(s) for s in tmp}

        tmp = findall(r'global_dependency_graph::new : tx = (\d+)', log)
        all_global_dependency_graph_tx_uids = {int(s) for s in tmp}

        tmp = findall(r'global_order_maker::run : sending node = (\d+)', log)
        all_global_order_maker_tx_uids = {int(s) for s in tmp}

        tmp = findall(r'ParallelExecution::execute : tx_uid = (\d+)', log)
        all_parallel_execution_tx_uids = {int(s) for s in tmp}

        tmp = findall(r'Processor::spawn : tx_uid = (\d+)', log)
        all_processor_tx_uids = {int(s) for s in tmp}

        # Extract TX_FINALIZED logs with timestamps
        # Format: [2026-02-28T17:02:25.196Z INFO  worker::execution_threadpool] TX_FINALIZED: tx_uid=3329957383421940139
        tmp = findall(r'\[(.*Z) .* TX_FINALIZED: tx_uid=(\d+)', log)
        tx_finalized = {int(tx_uid): self._to_posix(timestamp) for timestamp, tx_uid in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return sizes, samples, ip, all_batch_maker_tx_uids, all_global_dependency_graph_tx_uids, all_global_order_maker_tx_uids, all_parallel_execution_tx_uids, all_processor_tx_uids, tx_finalized

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)
    
    def _consensus_duration(self):
        if not self.commits:
            return 0
        start = min(self.start)
        end = max(self.commits.values())
        return end - start

    def _end_to_end_throughput(self):
        """Calculate throughput based on TX_FINALIZED transactions"""
        if not self.tx_finalized:
            return 0, 0, 0
        
        # Count total finalized transactions
        total_txs = len(self.tx_finalized)
        
        # Calculate total bytes (size per transaction * number of transactions)
        total_bytes = total_txs * self.size[0]
        
        # Calculate duration from first client start to last finalization
        if self.tx_finalized:
            end_time = max(self.tx_finalized.values())
            start_time = min(self.start)
            duration = end_time - start_time if end_time > start_time else 1
        else:
            duration = 1
        
        return total_txs / duration, total_bytes / duration, duration

    def _end_to_end_latency(self):
        """Calculate latency from client send to finalization"""
        latencies = []
        
        for client_sent in self.sent_samples:
            for tx_uid, send_time in client_sent.items():
                if tx_uid in self.tx_finalized:
                    finalize_time = self.tx_finalized[tx_uid]
                    latency = finalize_time - send_time
                    if latency > 0:  # Only include positive latencies
                        latencies.append(latency)
        
        return mean(latencies) if latencies else 0

    def _end_to_end_client_sending_rate(self):
        txs = 0
        max_end_time = 0
        for client_txs in self.sent_samples:
            txs += len(client_txs)
            if client_txs:
                max_end_time = max(max_end_time, max(client_txs.values()))
        duration = max_end_time - min(self.start) if max_end_time > 0 else 1
        return txs / duration

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1000  # Convert to milliseconds
        client_sending_rate = self._end_to_end_client_sending_rate()

        # Calculate min/max/avg latency if we have data
        latencies = []
        for client_sent in self.sent_samples:
            for tx_uid, send_time in client_sent.items():
                if tx_uid in self.tx_finalized:
                    latencies.append((self.tx_finalized[tx_uid] - send_time) * 1000)
        
        latency_stats = ""
        if latencies:
            latency_stats = f'   Min latency: {round(min(latencies)):,} ms\n' \
                           f'   Max latency: {round(max(latencies)):,} ms\n' \
                           f'   Avg latency: {round(mean(latencies)):,} ms\n'

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Consensus time: {round(self._consensus_duration()):,} s\n'
            f' Execution window: {round(duration):,} s\n'
            '\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Total finalized transactions: {len(self.tx_finalized):,}\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' Client Sending Rate: {round(client_sending_rate):,} tx/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            f'{latency_stats}'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(clients, primaries, workers, faults=faults)