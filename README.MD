# Dependencies

```sh
sudo apt-get update
sudo apt-get install -y zip make cmake g++ libjemalloc-dev libboost-dev libgoogle-glog-dev
```

# Build

```
./compile.sh
```

# Project Structure

This project organizes the system as a
benchmark-driven transaction-processing framework.

- `bench_ycsb.cpp` and `bench_tpcc.cpp`: executable entry points. They parse
  command-line flags, construct the workload context, initialize the database,
  create a `Coordinator`, connect peers, and start the benchmark run.
- `core/`: shared runtime layer. `Coordinator` owns worker threads and I/O
  dispatchers; `Manager` coordinates protocol phases; `Worker` is the executor
  base class; `Context` and `Macros` define runtime configuration; `Table`,
  `Partitioner`, `ControlMessage`, and `Dispatcher` provide storage,
  partitioning, control-plane messaging, and networking support.
- `core/factory/`: protocol/workload binding layer. `WorkerFactory` maps the
  selected `--protocol` and benchmark context to the corresponding transaction,
  workload, manager, and executor types.
- `core/sql/`: lightweight SQL front-end and execution emulator. It provides
  parser, planner, plan cache, execution kernel, workload builder, and metrics
  components used to model SQL processing overhead inside benchmark
  transactions.
- `benchmark/ycsb/` and `benchmark/tpcc/`: benchmark definitions. Each
  benchmark provides schemas, database initialization, workload generation,
  transaction implementations, query definitions, random generators, and
  per-transaction storage.
- `protocol/`: concurrency-control implementations that share the same runtime
  interfaces. The repository includes `TwoPL`, `Calvin`, `Bohm`, `Aria`,
  `AriaFB`, `Pwv`, `DOCC`, and `Prodcc`.
- `protocol/<name>/`: each protocol directory contains its protocol logic,
  executor, manager when needed, transaction state, read/write key metadata,
  helper functions, and message handlers.
- `common/`: reusable infrastructure, including message buffers, message
  pieces, sockets, serialization, lock-free queues, hash maps, random and Zipf
  generators, percentile statistics, fixed strings, and timing utilities.
- `CMakeLists.txt`, `compile.sh`: build and experiment helpers.
  CMake builds the shared `common` library and the `bench_ycsb` and
  `bench_tpcc` executables.