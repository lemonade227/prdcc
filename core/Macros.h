//
// Created by Yi Lu on 3/18/19.
//

#pragma once

#include "glog/logging.h"
#include <boost/algorithm/string/split.hpp>

DEFINE_string(servers, "127.0.0.1:10010",
              "semicolon-separated list of servers");
DEFINE_int32(id, 0, "coordinator id");
DEFINE_int32(threads, 1, "the number of threads");
DEFINE_int32(io, 1, "the number of i/o threads");
DEFINE_int32(partition_num, 1, "the number of partitions");
DEFINE_string(partitioner, "hash", "database partitioner (hash, hash2, pb)");
DEFINE_bool(sleep_on_retry, true, "sleep when retry aborted transactions");
DEFINE_int32(batch_size, 100, "star or calvin batch size");
DEFINE_int32(group_time, 10, "group commit frequency");
DEFINE_int32(batch_flush, 50, "batch flush");
DEFINE_int32(sleep_time, 1000, "retry sleep time");
DEFINE_string(protocol, "aria", "transaction protocol");
DEFINE_string(replica_group, "1,3", "calvin replica group");
DEFINE_string(lock_manager, "1,1", "calvin lock manager");
DEFINE_bool(read_on_replica, false, "read from replicas");
DEFINE_bool(local_validation, false, "local validation");
DEFINE_bool(rts_sync, false, "rts sync");
DEFINE_bool(star_sync, false, "synchronous write in the single-master phase");
DEFINE_bool(star_dynamic_batch_size, true, "dynamic batch size");
DEFINE_bool(plv, true, "parallel locking and validation");
DEFINE_bool(same_batch, false,
            "always run the same batch of txns in calvin and bohm.");
DEFINE_bool(aria_read_only, true, "aria read only optimization");
DEFINE_bool(aria_reordering, true, "aria reordering optimization");
DEFINE_bool(aria_si, false, "aria snapshot isolation");
DEFINE_bool(enable_sql_emulator, true,
            "Enable compute-based SQL frontend overhead emulation.");
DEFINE_int32(sql_plan_cache_size, 256, "SQL emulator plan cache size.");
DEFINE_int32(sql_parser_complexity, 1,
             "SQL emulator parser complexity multiplier.");
DEFINE_int32(sql_optimizer_level, 1,
             "SQL emulator optimizer work factor.");
DEFINE_int32(sql_exec_vector_size, 8,
             "SQL emulator execution vector width.");
DEFINE_int32(sql_exec_expr_complexity, 4,
             "SQL emulator expression complexity multiplier.");
DEFINE_int32(sql_queue_workers, 0,
             "SQL emulator concurrent worker slots; 0 disables queueing.");
DEFINE_bool(sql_enable_retry_cache_bias, true,
            "SQL emulator retry path prefers cached plans and bind+exec.");
DEFINE_double(prodcc_abort_rate_threshold, 1.0,
              "Prodcc: enable conflict prediction when abort rate >= threshold "
              "(range [0,1]; default 1.0 means always bypass).");
DEFINE_double(prodcc_toxic_query_rate, 0.0,
              "Prodcc experiment knob: with probability p, skip predictor and "
              "force ConflictType::WAW when building the dependency graph "
              "(range [0,1]; default 0).");
DEFINE_int32(prodcc_aggregation_factor, 1,
             "Prodcc: number of micro-blocks to aggregate into one macro-block "
             "(default 1).");
DEFINE_int32(delay, 0, "delay time in us.");
DEFINE_string(cdf_path, "", "path to cdf");
DEFINE_string(log_path, "", "path to disk logging.");
DEFINE_bool(tcp_no_delay, true, "TCP Nagle algorithm, true: disable nagle");
DEFINE_bool(tcp_quick_ack, false, "TCP quick ack mode, true: enable quick ack");
DEFINE_bool(cpu_affinity, true, "pinning each thread to a separate core");
DEFINE_int32(cpu_core_id, 0, "cpu core id");
DEFINE_int32(benchmark_time, 25, "benchmark runtime in seconds");
DEFINE_int32(benchmark_warmup, 10, "warmup time in seconds");
DEFINE_int32(benchmark_cooldown, 5, "cooldown time in seconds");
DEFINE_int32(durable_write_cost, 0,
             "the cost of durable write in microseconds");
DEFINE_bool(exact_group_commit, false, "dynamically adjust group time.");
DEFINE_bool(mvcc, false, "use mvcc storage for BOHM.");
DEFINE_bool(bohm_local, false, "locality optimization for Bohm.");
DEFINE_bool(bohm_single_spin, false, "spin optimization for Bohm.");
DEFINE_int32(ariaFB_lock_manager, 0,
             "# of lock manager in aria's fallback mode.");

#define SETUP_CONTEXT(context)                                                 \
  boost::algorithm::split(context.peers, FLAGS_servers,                        \
                          boost::is_any_of(";"));                              \
  context.coordinator_num = context.peers.size();                              \
  context.coordinator_id = FLAGS_id;                                           \
  context.worker_num = FLAGS_threads;                                          \
  context.io_thread_num = FLAGS_io;                                            \
  context.partition_num = FLAGS_partition_num;                                 \
  context.partitioner = FLAGS_partitioner;                                     \
  context.sleep_on_retry = FLAGS_sleep_on_retry;                               \
  context.batch_size = FLAGS_batch_size;                                       \
  context.group_time = FLAGS_group_time;                                       \
  context.batch_flush = FLAGS_batch_flush;                                     \
  context.sleep_time = FLAGS_sleep_time;                                       \
  context.protocol = FLAGS_protocol;                                           \
  context.replica_group = FLAGS_replica_group;                                 \
  context.lock_manager = FLAGS_lock_manager;                                   \
  context.read_on_replica = FLAGS_read_on_replica;                             \
  context.local_validation = FLAGS_local_validation;                           \
  context.rts_sync = FLAGS_rts_sync;                                           \
  context.star_sync_in_single_master_phase = FLAGS_star_sync;                  \
  context.star_dynamic_batch_size = FLAGS_star_dynamic_batch_size;             \
  context.parallel_locking_and_validation = FLAGS_plv;                         \
  context.same_batch = FLAGS_same_batch;                                       \
  context.aria_read_only_optmization = FLAGS_aria_read_only;                   \
  context.aria_reordering_optmization = FLAGS_aria_reordering;                 \
  context.aria_snapshot_isolation = FLAGS_aria_si;                             \
  context.enable_sql_emulator = FLAGS_enable_sql_emulator;                     \
  context.sql_plan_cache_size = FLAGS_sql_plan_cache_size;                     \
  context.sql_parser_complexity = FLAGS_sql_parser_complexity;                 \
  context.sql_optimizer_level = FLAGS_sql_optimizer_level;                     \
  context.sql_exec_vector_size = FLAGS_sql_exec_vector_size;                   \
  context.sql_exec_expr_complexity = FLAGS_sql_exec_expr_complexity;           \
  context.sql_queue_workers = FLAGS_sql_queue_workers;                         \
  context.sql_enable_retry_cache_bias = FLAGS_sql_enable_retry_cache_bias;     \
  context.prodcc_abort_rate_threshold = FLAGS_prodcc_abort_rate_threshold;     \
  context.prodcc_toxic_query_rate = FLAGS_prodcc_toxic_query_rate;             \
  context.prodcc_aggregation_factor = FLAGS_prodcc_aggregation_factor;         \
  context.delay_time = FLAGS_delay;                                            \
  context.log_path = FLAGS_log_path;                                           \
  context.cdf_path = FLAGS_cdf_path;                                           \
  context.tcp_no_delay = FLAGS_tcp_no_delay;                                   \
  context.tcp_quick_ack = FLAGS_tcp_quick_ack;                                 \
  context.cpu_affinity = FLAGS_cpu_affinity;                                   \
  context.cpu_core_id = FLAGS_cpu_core_id;                                     \
  context.benchmark_time = FLAGS_benchmark_time;                               \
  context.benchmark_warmup = FLAGS_benchmark_warmup;                           \
  context.benchmark_cooldown = FLAGS_benchmark_cooldown;                       \
  context.durable_write_cost = FLAGS_durable_write_cost;                       \
  context.exact_group_commit = FLAGS_exact_group_commit;                       \
  context.mvcc = FLAGS_mvcc;                                                   \
  context.bohm_local = FLAGS_bohm_local;                                       \
  context.bohm_single_spin = FLAGS_bohm_single_spin;                           \
  context.ariaFB_lock_manager = FLAGS_ariaFB_lock_manager;                     \
  CHECK(context.coordinator_num == 1 || context.bohm_single_spin == false)     \
      << "bohm_single_spin must be used in single-node mode.";                 \
  CHECK((context.mvcc ^ (context.protocol == "Bohm" || context.protocol == "DOCC")) == 0)                    \
      << "MVCC must be used in Bohm or DOCC.";                                         \
  context.set_star_partitioner();
