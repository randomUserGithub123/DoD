# Copyright(C) Facebook, Inc. and its affiliates.
from os.path import join

from benchmark.utils import Print, PathMaker


class CommandMaker:

    @staticmethod
    def cleanup(username=None):
        if username:
            return f'rm -r /var/scratch/{username}/narwhal/benchmark/.db-* ; rm /var/scratch/{username}/narwhal/benchmark/*.json ; mkdir -p {PathMaker.results_path()}'
        else:
            return f"rm .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}"

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node generate_keys --filename {filename}'

    @staticmethod
    def run_primary(keys, committee, store, parameters, size, n_users, skew_factor, prob_choose_mtx, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} --size {size} --n_users {n_users} --skew_factor {skew_factor} --prob_choose_mtx {prob_choose_mtx} primary')

    @staticmethod
    def run_worker(keys, committee, store, parameters, id, size, n_users, shard_assignment, skew_factor, prob_choose_mtx, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(shard_assignment, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} --size {size} --n_users {n_users} --shard {shard_assignment} --skew_factor {skew_factor} --prob_choose_mtx {prob_choose_mtx} worker --id {id}')

    @staticmethod
    def run_client(address, size, n_users, shard_assignment, skew_factor, prob_choose_mtx, rate, nodes):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        assert isinstance(shard_assignment, list)
        shards = f'--shards {" ".join(shard_assignment)}' if shard_assignment else ''
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        return f'./benchmark_client {address} --size {size} --n_users {n_users} {shards} --skew_factor {skew_factor} --prob_choose_mtx {prob_choose_mtx} --rate {rate} {nodes}'

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'benchmark_client')
        return f'rm node ; rm benchmark_client ; ln -s {node} . ; ln -s {client} .'
