# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess
from math import ceil
from os.path import basename, splitext
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import Key, LocalCommittee, NodeParameters, BenchParameters, ConfigError
from benchmark.logs import LogParser, ParseError
from benchmark.fairness_logs import FairnessParseError, FairnessLogParser
from benchmark.utils import Print, BenchError, PathMaker

USE_FAIRNESS_PARSER = True

class LocalBench:
    BASE_PORT = 3000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
            self.gamma = node_parameters_dict['gamma']
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')
            nodes, rate = self.nodes[0], self.rate[0]

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            committee = LocalCommittee(names, self.BASE_PORT, self.workers, self.gamma)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # Assign shard to each worker
            workers_addresses = committee.workers_addresses(self.faults)

            num_clients = len(workers_addresses) * self.workers
            Print.info(f'clients = {num_clients}, n_users = {self.n_users}, Shards = {self.shards}, skew_factor = {self.skew_factor}, prob_choose_mtx = {self.prob_choose_mtx}')

            worker_to_shard_assignment = {}
            shard_assignment_list = [str(self.workers)]
            for shard in self.bench_parameters.shards:
                shard_assignment_list.append(str(shard[0]))
            for i, addresses in enumerate(workers_addresses):
                shard_idx = 0
                for (id, address) in addresses:
                    shard_range = self.bench_parameters.shards[shard_idx]
                    worker_to_shard_assignment.update({address:shard_range})
                    shard_assignment_list.append(address)
                    shard_idx += 1
            Print.info(f'shard_assignment_list = {shard_assignment_list}')
            Print.info(f'worker_to_shard_assignment = {worker_to_shard_assignment}')

            # Run the clients (they will wait for the nodes to be ready).
            # TODO: find correct rate_share
            rate_share = ceil(rate / committee.workers())
            Print.info(f'workers_addresses = {workers_addresses}')
            for i in range(num_clients):
                # TODO: remove worker address altogether
                # Print.info(f'parties = {max(self.nodes)} party idx = {int(i/max(self.nodes))} worker idx = {i%self.workers}')
                (id, address) = workers_addresses[int(i/max(self.nodes))][i%self.workers]
                cmd = CommandMaker.run_client(
                    address,
                    self.tx_size,
                    self.n_users,
                    shard_assignment_list,
                    self.skew_factor,
                    self.prob_choose_mtx,
                    rate_share,
                    [x for y in workers_addresses for _, x in y]
                )
                log_file = PathMaker.client_log_file(i)
                self._background_run(cmd, log_file)

            # Run the primaries (except the faulty ones).
            for i, address in enumerate(committee.primary_addresses(self.faults)):
                cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i),
                    PathMaker.parameters_file(),
                    self.tx_size,
                    self.n_users,
                    self.skew_factor,
                    self.prob_choose_mtx,
                    debug=debug
                )
                log_file = PathMaker.primary_log_file(i)
                self._background_run(cmd, log_file)

            # Run the workers (except the faulty ones).
            for i, addresses in enumerate(workers_addresses):
                for (id, address) in addresses:
                    shard_assignment = str(worker_to_shard_assignment[address][0]) + "," + str(worker_to_shard_assignment[address][1])
                    # Print.info(f'shard_assignment for the worker {address} is {shard_assignment}')
                    cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, id),
                        PathMaker.parameters_file(),
                        id,  # The worker's id.
                        self.tx_size,
                        self.n_users,
                        shard_assignment,
                        self.skew_factor,
                        self.prob_choose_mtx,
                        debug=debug
                    )
                    log_file = PathMaker.worker_log_file(i, id)
                    self._background_run(cmd, log_file)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            if USE_FAIRNESS_PARSER:
                return FairnessLogParser.process(PathMaker.logs_path(), faults=self.faults)
            else:
                return LogParser.process(PathMaker.logs_path(), faults=self.faults)

        except (subprocess.SubprocessError, FairnessParseError if USE_FAIRNESS_PARSER else ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)
