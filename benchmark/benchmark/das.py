# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess, os
import datetime
from math import ceil
from time import sleep
from random import shuffle

from benchmark.commands import CommandMaker
from benchmark.config import (
    Key,
    NodeParameters,
    BenchParameters,
    ConfigError,
    DASCommittee
)
from benchmark.logs import LogParser, ParseError
from benchmark.fairness_logs import FairnessLogParser, FairnessParseError
from benchmark.utils import Print, BenchError, PathMaker
from benchmark.preserve import *

USE_FAIRNESS_PARSER = True

BANNED_NODES = ["node008", "node009", "node018", "node019", "node028", "node036", "node043", "node056", "node058"]


class DASBench:
    BASE_PORT = 4000

    def __init__(self, bench_parameters_dict, node_parameters_dict, username):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
            self.gamma = node_parameters_dict['gamma']
        except ConfigError as e:
            raise BenchError("Invalid nodes or bench parameters", e)
        self.preserve_manager = PreserveManager(username)
        self.username = username
        self._wd = os.getcwd()
        self._hostnames = None

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file, hostname):
        cmd = f"{command} 2> {log_file}"
        process = f"ssh {hostname} 'source /etc/profile; cd {self._wd}; {cmd}'"
        subprocess.Popen(process, shell=True)

    def _kill_nodes(self):
        try:
            hosts = self._get_hostnames()
            cmd = CommandMaker.cleanup(username=self.username)

            for host in hosts:
                self._background_run(cmd, "/dev/null", host)
        except Exception as e:
            pass

        try:
            self.preserve_manager.kill_reservation("LAST")
        except Exception as e:
            import traceback
            print(
                f"""Exception : {str(e)}\nTraceback: {traceback.format_exc()}"""
            )

    def _preserve_machines(self, nodes):
        if self.collocate:
            self._amount_for_nodes = nodes
            # All workers on same machines as primaries, clients on separate machines
            # Number of client machines = ceil(num_clients / 4)
            num_clients = nodes * self.workers
            self._num_machines = self._amount_for_nodes + ceil(num_clients / 4)
        else:
            self._amount_for_nodes = nodes + nodes * self.workers
            num_clients = nodes * self.workers
            self._num_machines = self._amount_for_nodes + ceil(num_clients / 4)

        time_string = str(datetime.timedelta(seconds=self.duration + 75 + 2 * nodes))
        self.reservation_id = self.preserve_manager.create_reservation(
            self._num_machines + len(BANNED_NODES), time_string
        )

    def _get_hostnames(self):
        if self._hostnames:
            return self._hostnames

        reservations = self.preserve_manager.get_own_reservations()
        for v in reservations.values():
            self._hostnames = v.assigned_machines
            return self._hostnames
        return []

    def run(self, debug=False, console=False, build=True):
        assert isinstance(debug, bool)
        Print.heading("Starting DAS benchmark")

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info("Setting up testbed...")
            nodes, rate = self.nodes[0], self.rate[0]

            # Cleanup all files.
            cmd = f"{CommandMaker.clean_logs()} ; {CommandMaker.cleanup(self.username)}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)

            if build:
                Print.info("Rebuilding binaries")
                cmd = CommandMaker.compile().split()
                subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

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

            self._preserve_machines(nodes=nodes)
            sleep(5)
            all_hostnames = self._get_hostnames()
            for banned_node in BANNED_NODES:
                if banned_node in all_hostnames:
                    all_hostnames.remove(banned_node)
            all_hostnames = all_hostnames[:self._num_machines]
            shuffle(all_hostnames)
            nodes_amount = self._amount_for_nodes

            nodes_hostnames = all_hostnames[:nodes_amount]
            clients_hostnames = all_hostnames[nodes_amount:]

            # Create DASCommittee (you may need to adjust this class for shards)
            committee = DASCommittee(
                names,
                self.BASE_PORT,
                self.workers,
                self.gamma,
                nodes_hostnames
            )
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # Assign shard to each worker
            workers_addresses = committee.workers_addresses(self.faults)

            num_clients = len(workers_addresses) * self.workers
            Print.info(
                f'num_clients = {num_clients}, n_users = {self.n_users}, '
                f'Shards = {self.shards}, skew_factor = {self.skew_factor}, '
                f'prob_choose_mtx = {self.prob_choose_mtx}'
            )

            worker_to_shard_assignment = {}
            shard_assignment_list = [str(self.workers)]
            for shard in self.bench_parameters.shards:
                shard_assignment_list.append(str(shard[0]))
            for i, addresses in enumerate(workers_addresses):
                shard_idx = 0
                for (id, address) in addresses:
                    shard_range = self.bench_parameters.shards[shard_idx]
                    worker_to_shard_assignment.update({address: shard_range})
                    shard_assignment_list.append(address)
                    shard_idx += 1
            Print.info(f'shard_assignment_list = {shard_assignment_list}')
            Print.info(f'worker_to_shard_assignment = {worker_to_shard_assignment}')

            # Run the clients
            # Rate calculation: num_clients derived from topology
            rate_share = ceil(rate / num_clients)
            Print.info(f'workers_addresses = {workers_addresses}')
            Print.info(f'num_clients = {num_clients}, rate_share = {rate_share}')

            for i in range(num_clients):
                (id, address) = workers_addresses[i // self.workers][i % self.workers]
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
                client_host = clients_hostnames[i // 4]
                Print.info(f"Launching client {i} on {client_host}")
                self._background_run(cmd, log_file, client_host)

            # Run the primaries (except the faulty ones).
            for i, address in enumerate(committee.primary_addresses(self.faults)):
                cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i, username=self.username),
                    PathMaker.parameters_file(),
                    self.tx_size,
                    self.n_users,
                    self.skew_factor,
                    self.prob_choose_mtx,
                    debug=debug
                )
                log_file = PathMaker.primary_log_file(i)
                Print.info(f"Launching primary {i} on {address}")
                self._background_run(cmd, log_file, address.split(":")[0])

            # Run the workers (except the faulty ones).
            for i, addresses in enumerate(workers_addresses):
                for (id, address) in addresses:
                    shard_assignment = (
                        str(worker_to_shard_assignment[address][0]) + "," +
                        str(worker_to_shard_assignment[address][1])
                    )
                    cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, id, username=self.username),
                        PathMaker.parameters_file(),
                        id,
                        self.tx_size,
                        self.n_users,
                        shard_assignment,
                        self.skew_factor,
                        self.prob_choose_mtx,
                        debug=debug
                    )
                    log_file = PathMaker.worker_log_file(i, id)
                    Print.info(f"Launching worker {i}-{id} on {address}")
                    self._background_run(cmd, log_file, address.split(":")[0])

            # Wait for all transactions to be processed.
            Print.info(f"Running benchmark ({self.duration} sec)...")
            sleep(self.duration)
            self._kill_nodes()

            sleep(nodes * 2)

            # Parse logs and return the parser.
            Print.info("Parsing logs...")
            if USE_FAIRNESS_PARSER:
                log_values = FairnessLogParser.process(
                    PathMaker.logs_path(),
                    faults=self.faults,
                )
            else:
                log_values = LogParser.process(
                    PathMaker.logs_path(),
                    faults=self.faults,
                )

            cmd = f"{CommandMaker.cleanup(username=self.username)}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

            return log_values

        except subprocess.SubprocessError as e:
            cmd = f"{CommandMaker.cleanup(username=self.username)}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            self._kill_nodes()
            raise BenchError("Failed to run benchmark", e)
        except (FairnessParseError if USE_FAIRNESS_PARSER else ParseError) as e:
            raise BenchError("Error parsing logs, maybe panic", e)