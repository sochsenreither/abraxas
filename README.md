# Ditto

[![build status](https://img.shields.io/github/workflow/status/danielxiangzl/hotstuff/Build/main?style=flat-square&logo=github)](https://github.com/danielxiangzl/hotstuff/actions)
[![test status](https://img.shields.io/github/workflow/status/danielxiangzl/hotstuff/Tests/main?style=flat-square&logo=github&label=tests)](https://github.com/danielxiangzl/hotstuff/actions)
[![rustc](https://img.shields.io/badge/rustc-1.50.0+-blue?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![license](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](LICENSE)

This repo provides a minimal implementation of optimistic compiler. The codebase has been designed to be small, efficient, and easy to benchmark and modify. It has not been designed to run in production but uses real cryptography ([dalek](https://doc.dalek.rs/ed25519_dalek)), networking ([tokio](https://docs.rs/tokio)), and storage ([rocksdb](https://docs.rs/rocksdb)).

## Quick Start
Optimistic compiler is written in Rust, but all benchmarking scripts are written in Python and run with [Fabric](http://www.fabfile.org/).
To deploy and benchmark a testbed of 4 nodes on your local machine, clone the repo and install the python dependencies:
```
$ git clone https://github.com/danielxiangzl/hotstuff.git
$ cd hotstuff/benchmark
$ pip install -r requirements.txt
```
You also need to install Clang (required by rocksdb) and [tmux](https://linuxize.com/post/getting-started-with-tmux/#installing-tmux) (which runs all nodes and clients in the background). Finally, run a local benchmark using fabric:
```
$ fab local
```
This command may take a long time the first time you run it (compiling rust code in `release` mode may be slow) and you can customize a number of benchmark parameters in `fabfile.py`. When the benchmark terminates, it displays a summary of the execution similarly to the one below.
```
-----------------------------------------
 SUMMARY:
-----------------------------------------
+ CONFIG:
 DDOS attack: False
 Random DDOS attack: False
 Committee size: 4 nodes
 Loopback: 15
 Input rate: 10,000 tx/s
 Transaction size: 512 B
 Faults: 0 nodes
 Execution time: 20 s

 Consensus timeout delay: 2,000 ms
 Consensus sync retry delay: 10,000 ms
 Consensus max payloads size: 500 B
 Consensus min block delay: 0 ms
 Mempool queue capacity: 10,000 B
 Mempool max payloads size: 15,000 B
 Mempool min block delay: 0 ms

 + RESULTS:
 Consensus TPS: 9,899 tx/s
 Consensus BPS: 5,068,379 B/s
 Consensus latency: 79 ms

 End-to-end TPS: 9,882 tx/s
 End-to-end BPS: 5,059,501 B/s
 End-to-end latency: 194 ms
-----------------------------------------
```

## Next Steps
The [wiki](https://github.com/asonnino/hotstuff/wiki) documents the codebase, explains its architecture and how to read benchmarks' results, and provides a step-by-step tutorial to run [benchmarks on Amazon Web Services](https://github.com/asonnino/hotstuff/wiki/AWS-Benchmarks) accross multiple data centers (WAN).

## License
This software is licensed as [Apache 2.0](LICENSE).
