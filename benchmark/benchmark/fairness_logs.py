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
        self.size, self.rate, self.start, misses, self.sent_samples \
            = zip(*results)
        self.misses = sum(misses)

        print(len(self.sent_samples[0]))

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
        sizes, self.received_samples, workers_ips, \
            all_batch_maker_tx_uids, all_global_dependency_graph_tx_uids, \
            all_global_order_maker_tx_uids, all_parallel_execution_tx_uids, \
            all_processor_tx_uids, all_finalized_txs = zip(*results)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        # Merge TX_FINALIZED across all workers: keep earliest timestamp per tx_uid
        self.finalized_txs = {}
        for worker_finalized in all_finalized_txs:
            for tx_uid, ts in worker_finalized.items():
                if tx_uid not in self.finalized_txs or ts < self.finalized_txs[tx_uid]:
                    self.finalized_txs[tx_uid] = ts

        print(f"Total unique TX_FINALIZED: {len(self.finalized_txs)}")

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

        txs_not_found_in_gdg = 0
        txs_not_found_in_gom = 0
        txs_not_found_in_pe = 0
        txs_not_found_in_processor = 0
        for tx_uid in worker_bm_tx_uids:
            if tx_uid not in worker_gdg_tx_uids:
                txs_not_found_in_gdg += 1
            if tx_uid not in worker_gom_tx_uids:
                txs_not_found_in_gom += 1
            if tx_uid not in worker_pe_tx_uids:
                txs_not_found_in_pe += 1
            if tx_uid not in worker_processor_tx_uids:
                txs_not_found_in_processor += 1
        print(f"Number of transactions not found in global dependency graph: {txs_not_found_in_gdg}")
        print(f"Number of transactions not found in global order maker: {txs_not_found_in_gom}")
        print(f"Number of transactions not found in parallel execution: {txs_not_found_in_pe}")
        print(f"Number of transactions not found in processor: {txs_not_found_in_processor}")

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

        # Parse TX_FINALIZED lines: keep earliest timestamp per tx_uid
        tmp = findall(r'\[(.*Z) .* TX_FINALIZED: tx_uid=(\d+)', log)
        finalized_txs = {}
        for t, s in tmp:
            tx_uid = int(s)
            ts = self._to_posix(t)
            if tx_uid not in finalized_txs or ts < finalized_txs[tx_uid]:
                finalized_txs[tx_uid] = ts

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return sizes, samples, ip, all_batch_maker_tx_uids, \
            all_global_dependency_graph_tx_uids, all_global_order_maker_tx_uids, \
            all_parallel_execution_tx_uids, all_processor_tx_uids, finalized_txs

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
        """Compute TPS and BPS from TX_FINALIZED logs."""
        if not self.finalized_txs:
            return 0, 0, 0
        start = min(self.start)
        end = max(self.finalized_txs.values())
        duration = end - start
        if duration <= 0:
            return 0, 0, 0
        num_finalized = len(self.finalized_txs)
        tx_size = self.size[0]
        tps = num_finalized / duration
        bps = (num_finalized * tx_size) / duration
        return tps, bps, duration

    def _end_to_end_latency(self):
        """Compute average latency: finalized_ts - sent_ts for matching tx_uids."""
        latency = []
        not_found = 0
        for sent in self.sent_samples:
            for tx_uid, finalized_ts in self.finalized_txs.items():
                if tx_uid in sent:
                    start_time = sent[tx_uid]
                    latency.append(finalized_ts - start_time)
                else:
                    not_found += 1
        print(f"{not_found} finalized tx_ids not found in sent_samples")
        return mean(latency) if latency else 0

    def _end_to_end_client_sending_rate(self):
        txs = 0
        max_end_time = 0
        for client_txs in self.sent_samples:
            txs += len(client_txs)
            max_end_time = max(max_end_time, max(client_txs.values()))
        duration = max_end_time - min(self.start)
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
        end_to_end_latency = self._end_to_end_latency() * 1_000
        client_sending_rate = self._end_to_end_client_sending_rate()

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
            f' Execution time: {round(duration):,} s\n'
            f' Total finalized txs: {len(self.finalized_txs):,}\n'
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
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            f' Client Sending Rate: {round(client_sending_rate):,} tx/s\n'
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