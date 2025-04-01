# Running Benchmarks
This document explains how to benchmark the codebase and read benchmarks' results. It also provides a step-by-step tutorial to run benchmarks on [Cloudlab](https://www.cloudlab.us/).

## Local Benchmarks
When running benchmarks, the codebase is automatically compiled with the feature flag `benchmark`. This enables the node to print some special log entries that are then read by the python scripts and used to compute performance. These special log entries are clearly indicated with comments in the code: make sure to not alter them (otherwise the benchmark scripts will fail to interpret the logs).

### Parametrize the benchmark
After cloning the repo and [installing all dependencies](https://github.com/HeenaNagda/DoD#quick-start), you can use [Fabric](http://www.fabfile.org/) to run benchmarks on your local machine.  Locate the task called `local` in the file [fabfile.py](https://github.com/HeenaNagda/DoD/blob/main/benchmark/fabfile.py):
```python
@task
def local(ctx):
    ...
```
The task specifies two types of parameters, the *benchmark parameters* and the *nodes parameters*. The benchmark parameters look as follows:
```python
bench_params = {
    'faults': 1,
    'nodes': 5,
    'workers': 4,
    'clients': 6,
    'rate': 100_000,
    'tx_size': 512,
    'n_users': 1000,
    'shards': [[0,25],[26,50],[51,75],[76,99]],
    'skew_factor': 0.01,
    'prob_choose_mtx': 1.0,
    'duration': 20,
}
```
They specify the number of primaries (`nodes`), workers per primary (`workers`), and number of clients(`clients`) to deploy, the input rate (tx/s) at which the clients submits transactions to the system (`rate`), the size of each transaction in bytes (`tx_size`), the number of faulty nodes ('faults), and the duration of the benchmark in seconds (`duration`). The minimum transaction size is 9 bytes, this ensure that the transactions of a client are all different. The benchmarking script will deploy as many clients as workers and divide the input rate equally amongst each client. For instance, if you configure the testbed with 4 nodes, 1 worker per node, and an input rate of 1,000 tx/s (as in the example above), the scripts will deploy 4 clients each submitting transactions to one node at a rate of 250 tx/s. When the parameters `faults` is set to `f > 0`, the last `f` nodes and clients are not booted; the system will thus run with `n-f` nodes (and `n-f` clients). 

The nodes parameters determine the configuration for the primaries and workers:
```python
node_params = {
    'header_size': 1_000,  # bytes
    'max_header_delay': 200,  # ms
    'gc_depth': 50,  # rounds
    'sync_retry_delay': 10_000,  # ms
    'sync_retry_nodes': 3,  # number of nodes
    'batch_size': 51_200,  # bytes
    'max_batch_delay': 200,  # ms
    'gamma': 0.9,
    'execution_threadpool_size': 20,
}
```
They are defined as follows:
* `header_size`: The preferred header size. The primary creates a new header when it has enough parents and enough batches' digests to reach `header_size`. Denominated in bytes.
* `max_header_delay`: The maximum delay that the primary waits between generating two headers, even if the header did not reach `max_header_size`. Denominated in ms.
* `gc_depth`: The depth of the garbage collection (Denominated in number of rounds).
* `sync_retry_delay`: The delay after which the synchronizer retries to send sync requests. Denominated in ms.
* `sync_retry_nodes`: Determine with how many nodes to sync when re-trying to send sync-request. These nodes are picked at random from the committee.
* `batch_size`: The preferred batch size. The workers seal a batch of transactions when it reaches this size. Denominated in bytes.
* `max_batch_delay`: The delay after which the workers seal a batch of transactions, even if `max_batch_size` is not reached. Denominated in ms.

### Run the benchmark
Once you specified both `bench_params` and `node_params` as desired, run:
```
$ fab local
```
This command first recompiles your code in `release` mode (and with the `benchmark` feature flag activated), thus ensuring you always benchmark the latest version of your code. This may take a long time the first time you run it. It then generates the configuration files and keys for each node, and runs the benchmarks with the specified parameters. It finally parses the logs and displays a summary of the execution similarly to the one below. All the configuration and key files are hidden JSON files; i.e., their name starts with a dot (`.`), such as `.committee.json`.
```
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Faults: 0 node(s)
 Committee size: 4 node(s)
 Worker(s) per node: 1 worker(s)
 Collocate primary and workers: True
 Input rate: 50,000 tx/s
 Transaction size: 512 B
 Execution time: 19 s

 Header size: 1,000 B
 Max header delay: 100 ms
 GC depth: 50 round(s)
 Sync retry delay: 10,000 ms
 Sync retry nodes: 3 node(s)
 batch size: 500,000 B
 Max batch delay: 100 ms

 + RESULTS:
 End-to-end TPS: 46,149 tx/s
 End-to-end BPS: 23,628,541 B/s
 End-to-end latency: 557 ms
-----------------------------------------
```
'End-to-end TPS' and 'End-to-end latency' report the performance of the whole system, starting from when the client submits the transaction. The end-to-end latency is often called 'client-perceived latency'.

## Cloudlab Benchmarks
This repo integrates various python scripts to deploy and benchmark the codebase on [Cloudlab](https://www.cloudlab.us/). This section provides a step-by-step tutorial explaining how to use them.

### Step 1. Add your SSH public key to your Cloudlab account
You must now [add your SSH public key to your Cloudlab account](https://docs.cloudlab.us/users.html#(part._ssh-access):~:text=2.1.3-,Setting%20up%20SSH%20access,-%F0%9F%94%97). This operation is manual.
If you don't have an SSH key, you can create one using [ssh-keygen](https://www.ssh.com/ssh/keygen/):
```
$ ssh-keygen -m PEM -f ~/.ssh
```

### Step 2. Configure the testbed
The file [settings.json](https://github.com/asonnino/narwhal/blob/master/benchmark/settings.json) (located in [DoD/benchmarks](https://github.com/HeenaNagda/DoD/blob/main/benchmark/settings.json)) contains all the configuration parameters of the testbed to deploy. Its content looks as follows:
```json
{
    "key": {
        "name": "aws",
        "path": "/absolute/key/path"
    },
    "port": 5000,
    "repo": {
        "name": "DoD",
        "url": "https://github.com/HeenaNagda/DoD.git",
        "branch": "heena/in-mem-missing-edge-store"
    },
    "instances": {
        "type": "m5d.8xlarge",
        "regions": ["us-east-1", "eu-north-1", "ap-southeast-2", "us-west-1", "ap-northeast-1"]
    }
}
```
Ignore the first block (`key`), SECOND BLOCK (`port`), and the last block (`instances`). They are used to deploye on AWS by Narwhal which is the base code for DOD.

The third block (`repo`) contains the information regarding the repository's name, the URL of the repo, and the branch containing the code to deploy: 
```json
"repo": {
   "name": "DoD",
    "url": "https://github.com/HeenaNagda/DoD.git",
    "branch": "heena/in-mem-missing-edge-store"
},
```
Remember to update the `url` field to the name of your repo. Modifying the branch name is particularly useful when testing new functionalities without having to checkout the code locally. 

### Step 3. Start an experiment and update manifest file
[Start a new experiment](https://docs.cloudlab.us/getting-started.html) on [Cloudlab](https://www.cloudlab.us/) with 10 machines. Now copy the [manifest](https://docs.cloudlab.us/cloudlab-manual.html#%28part._.Manifest_.View%29:~:text=authentication%20to%20work.-,13.5.5%C2%A0Manifest%20View,-%F0%9F%94%97) content from the Cloudlab experiment to the [benchmark/manifest.xml](https://github.com/HeenaNagda/DoD/blob/main/benchmark/manifest.xml) file. The python scripts retreives the addresses of Cloudlab machines to run the benchmark.

### Step 4. Create a testbed
The Cloudlab instances are orchestrated with [Fabric](http://www.fabfile.org) from the file [fabfile.py](https://github.com/HeenaNagda/DoD/blob/main/benchmark/fabfile.pyy) (located in [DoD/benchmarks](https://github.com/HeenaNagda/DoD/blob/main/benchmark)); you can list all possible commands as follows:
```
$ cd narwhal/benchmark
$ fab --list
```
open [fabfile.py](https://github.com/HeenaNagda/DoD/blob/main/benchmark/fabfile.py)

You can then clone the repo and install rust on the remote instances with `fab install`:
```
$ fab install

Installing rust and cloning the repo...
Initialized testbed of 10 nodes
```
This may take a long time as the command will first update all instances.
The command `fab info` displays a nice summary of all available machines and information to manually connect to them (for debug).

### Step 5. Run a benchmark
After setting up the testbed, running a benchmark on Cloudlab is similar to running it locally (see [Run Local Benchmarks](https://github.com/HeenaNagda/DoD/blob/main/benchmark#local-benchmarks)). Locate the task `remote` in [fabfile.py](https://github.com/asonnino/narwhal/blob/master/benchmark/fabfile.py):
```python
@task
def remote(ctx):
    ...
```
The benchmark parameters are similar to [local benchmarks](https://github.com/HeenaNagda/DoD/blob/main/benchmark#local-benchmarks) but allow to specify the number of nodes and the input rate as arrays to automate multiple benchmarks with a single command. The parameter `runs` specifies the number of times to repeat each benchmark (to later compute the average and stdev of the results), and the parameter `collocate` specifies whether to collocate all the node's workers and the primary on the same machine. If `collocate` is set to `False`, the script will run each node with its primary and each of its worker running on a dedicated instance.
```python
bench_params = {
    'nodes': [10, 20, 30],
    'workers: 2,
    'collocate': True,
    'rate': [20_000, 30_000, 40_000],
    'tx_size': 512,
    'faults': 0,
    'duration': 300,
    'runs': 2,
}
```
Similarly to local benchmarks, the scripts will deploy as many clients as workers and divide the input rate equally amongst each client. Each client is colocated with a worker, and only submit transactions to the worker with whom they share the machine.

Once you specified both `bench_params` and `node_params` as desired, run:
```
$ fab remote
```
This command first updates all machines with the latest commit of the GitHub repo and branch specified in your file [settings.json](https://github.com/HeenaNagda/DoD/blob/main/benchmark/settings.json) (step 3); this ensures that benchmarks are always run with the latest version of the code. It then generates and uploads the configuration files to each machine, runs the benchmarks with the specified parameters, and downloads the logs. It finally parses the logs and prints the results into a folder called `results` (which is automatically created if it doesn't already exists). You can run `fab remote` multiple times without fearing to override previous results, the command either appends new results to a file containing existing ones or prints them in separate files.