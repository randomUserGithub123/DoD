# DoD: DAG of DAGs

This repo provides an implementation of [DoD]. The codebase has been designed to be small, efficient, and easy to benchmark and modify. It has not been designed to run in production but uses real cryptography ([dalek](https://doc.dalek.rs/ed25519_dalek)), networking ([tokio](https://docs.rs/tokio)), and storage ([rocksdb](https://docs.rs/rocksdb)).

## Quick Start
The core protocols are written in Rust, but all benchmarking scripts are written in Python and run with [Fabric](http://www.fabfile.org/).
To deploy and benchmark a testbed on your local machine, 

#### Run the following basic commands
```
$ sudo apt-get update
$ sudo apt-get -y upgrade
$ sudo apt-get -y autoremove
```
#### Install following dependencies
```
$ sudo apt-get -y install build-essential
$ sudo apt-get -y install cmake
```
#### Install rust (non-interactive)
```
$ curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
$ source $HOME/.cargo/env
$ rustup default stable
```
#### install clang (needed for Rocksdb)
```
$ sudo apt-get install -y clang
```
#### install pip if not installed already
```
$ sudo apt install python3-pip
```         
#### clone the repo and install the python dependencies:
```
$ git clone -b heena/in-mem-missing-edge-store https://github.com/HeenaNagda/DoD.git
$ cd DoD/benchmark
$ pip install -r requirements.txt
```
#### Finally, run a local benchmark using fabric:
```
$ fab local
```
This command may take a long time the first time you run it and you can customize a number of benchmark parameters in `fabfile.py`. When the benchmark terminates, it displays a summary of the execution similarly to the one below.
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
## Next Steps
The README file of the [benchmark folder](https://github.com/HeenaNagda/DoD/tree/main/benchmark) explains how to benchmark the codebase and read benchmarks' results. It also provides a step-by-step tutorial to run benchmarks on [Cloudlab](https://www.cloudlab.us/).

## License
This software is licensed as [Apache 2.0](LICENSE).
