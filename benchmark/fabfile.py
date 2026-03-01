# Copyright(C) Facebook, Inc. and its affiliates.
from fabric import task

from benchmark.local import LocalBench, USE_FAIRNESS_PARSER
from benchmark.das import DASBench, USE_FAIRNESS_PARSER
from benchmark.logs import ParseError, LogParser
from benchmark.fairness_logs import FairnessParseError, FairnessLogParser
from benchmark.utils import Print, PathMaker
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, CloudLabBench, BenchError


@task
def local(ctx, debug=True):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 1,
        'nodes': 5,
        'workers': 4,
        'tx_size': 128,
        'n_users': 100,
        'shards': [[0,25],[26,50],[51,75],[76,99]],
        'skew_factor': 0.01,
        'prob_choose_mtx': 1.0,
        'duration': 20,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 51_200,  # bytes
        'max_batch_delay': 200,  # ms
        'gamma': 1.0,
        'execution_threadpool_size': 20,
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug)
        print(ret.result())
    except BenchError as e:
        Print.error(e)

@task
def das(ctx, debug=False, username='mputnik'):
    ''' Run benchmarks on DAS cluster '''

    for faults, gamma, workers_per_node, nodes, runs, input_rate in [
        ### Impact of GAMMA ###
        # (1, 1.0, 1, 5, 5, 4000),
        # (1, 0.9, 1, 6, 5, 4000),
        # (1, 0.8, 1, 7, 5, 4000),
        # (1, 0.7, 1, 11, 5, 4000),
        # (1, 0.6, 1, 21, 5, 4000),
        # (1, 1.0, 1, 5, 5, 3000),
        #######################
        # (4, 1.0, 4, 17, 5, 500),
        # (4, 1.0, 4, 17, 5, 1000),
        # (4, 1.0, 4, 17, 5, 1500),
        # (4, 1.0, 4, 17, 5, 2000),
        # (4, 1.0, 4, 17, 5, 2500),
        # (4, 1.0, 4, 17, 5, 3000),
        # (4, 1.0, 4, 17, 5, 3500),
        # (4, 1.0, 4, 17, 5, 4000),
        # (4, 1.0, 4, 17, 5, 4500),
        # (4, 1.0, 4, 17, 5, 5000),
        ##### Changing N ######
        (1, 1.0, 1, 5, 5, 4000),
        (1, 1.0, 1, 6, 5, 4000),
        (1, 1.0, 1, 7, 5, 4000),
        (1, 1.0, 1, 8, 5, 4000),
        (2, 1.0, 1, 9, 5, 4000),
        (2, 1.0, 1, 10, 5, 4000),
        (2, 1.0, 1, 11, 5, 4000),
        (2, 1.0, 1, 12, 5, 4000),
        (3, 1.0, 1, 13, 5, 4000),
        (3, 1.0, 1, 14, 5, 4000),
        (3, 1.0, 1, 15, 5, 4000),
        (3, 1.0, 1, 16, 5, 4000),
        (4, 1.0, 1, 17, 5, 4000),
        (4, 1.0, 1, 18, 5, 4000),
    ]:
        
        assert gamma > 0.5 and gamma <= 1.0

        bench_params = {
            'faults': faults,
            'nodes': nodes,
            'workers': workers_per_node,
            'collocate': True,
            'rate': input_rate,
            'tx_size': 128,
            'n_users': 100,
            'shards': [[0,99]],
            'skew_factor': 0.01,
            'prob_choose_mtx': 1.0,
            'duration': 60,
        }
        node_params = {
            'header_size': 512,
            'max_header_delay': 1000,
            'gc_depth': 50,
            'sync_retry_delay': 5_000,
            'sync_retry_nodes': 3,
            'batch_size': 4_000,
            'max_batch_delay': 1000,
            'gamma': gamma,
            'execution_threadpool_size': 20,
        }

        filename = PathMaker.local_result_file(
            faults,
            nodes,
            workers_per_node,
            input_rate,
        )

        run = 0
        while run < runs:
            try:
                ret = DASBench(
                    bench_params, 
                    node_params, 
                    username
                ).run(debug)

                if ret._consensus_duration() <= 30:
                    continue

                print(ret.result())
                ret.print(filename)
                run += 1
            except BenchError as e:
                Print.error(e)

@task
def create(ctx, nodes=2):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        # Bench(ctx).install()
        CloudLabBench(ctx, 'heenan').install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, debug=False):
    ''' Run benchmarks on AWS '''
    bench_params = {
        'faults': 0,
        'nodes': [5],
        'workers': 4,
        'clients': 6,
        'collocate': True,
        'rate': [100_000],
        'tx_size': 512,
        'n_users': 100,
        'shards': [[0,25],[26,50],[51,75],[76,99]],
        'skew_factor': 0.01,
        'prob_choose_mtx': 1.0,
        'duration': 300,
        'runs': 1,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 51_200,  # bytes
        'max_batch_delay': 200,  # ms
        'gamma': 0.75,
        'execution_threadpool_size': 4,
    }
    try:
        # Bench(ctx).run(bench_params, node_params, debug)
        CloudLabBench(ctx, 'heenan').run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'faults': [0],
        'nodes': [4],
        'workers': [2],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [3_500, 4_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    if USE_FAIRNESS_PARSER:
        try:
            print(FairnessLogParser.process('./logs', faults='?').result())
        except FairnessParseError as e:
            Print.error(BenchError('Failed to parse logs', e))
    else:
        try:
            print(LogParser.process('./logs', faults='?').result())
        except ParseError as e:
            Print.error(BenchError('Failed to parse logs', e))
