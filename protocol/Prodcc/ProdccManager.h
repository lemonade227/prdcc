/**
 * @file ProdccManager.h
 * @brief Batch scheduler and phase coordinator for PRDCC.
 *
 * ProdccManager owns the macro-batch transaction array, optionally builds the
 * predicted dependency graph, partitions transactions into ordered execution
 * blocks that avoid WAW conflicts and predicted cycles, and drives all workers
 * through the PRDCC read, prediction, execution, validation, finalization, and
 * cleanup phases.
 */
#pragma once

#include "core/Manager.h"
#include "core/Partitioner.h"
#include "protocol/Prodcc/Prodcc.h"
#include "protocol/Prodcc/ProdccExecutor.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Prodcc/ProdccTransaction.h"
#include "protocol/Prodcc/ConflictPredictor.h"
#include "protocol/Prodcc/KeyConverter.h"
#include "benchmark/tpcc/Schema.h"
#include "benchmark/ycsb/Schema.h"
#include "common/Random.h"

#include <boost/dynamic_bitset.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <iterator>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace aria {

// Forward declaration
template <class Workload> class ProdccManager;

using MicroBlock = std::vector<ProdccTransaction*>;

template <class Workload> class ProdccManager : public aria::Manager {
public:
  using base_type = aria::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = ProdccTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  ProdccManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
              const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag),
        current_micro_block(nullptr),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
        db(db),
        epoch(0) {

    const std::size_t AGGREGATION_FACTOR =
        std::max<std::size_t>(1, context.prodcc_aggregation_factor);
    const std::size_t micro_block_size = context.batch_size;
    const std::size_t macro_block_size = micro_block_size * AGGREGATION_FACTOR;

    LOG(INFO) << "ProdccManager: Aggregating " << AGGREGATION_FACTOR 
              << " micro-blocks of size " << micro_block_size 
              << " into a macro-block of size " << macro_block_size;

    storages.resize(macro_block_size);
    transactions.resize(macro_block_size);

    LOG(INFO) << "Coordinator " << coordinator_id << ": Starting to build DataSummary...";

    const auto key_domain = db.get_key_domain();
    const bool dense_domain = db.get_key_domain_is_dense();
    if (dense_domain) {
      predictor_.build_summary({}, key_domain.first, key_domain.second,
                               true);
    } else {
      const std::vector<uint64_t>& all_db_keys = db.get_all_keys();
      predictor_.build_summary(all_db_keys, key_domain.first, key_domain.second,
                               false);
    }

    LOG(INFO) << "Conflict predictor data summary built successfully.";
  }

 private:
  void build_txns_to_schedule() {
    txns_to_schedule_.clear();
    txns_to_schedule_.reserve(transactions.size());
    for (auto &txn_ptr : transactions) {
      if (txn_ptr && !txn_ptr->abort_no_retry) {
        txns_to_schedule_.push_back(txn_ptr.get());
      }
    }

    std::sort(txns_to_schedule_.begin(), txns_to_schedule_.end(),
              [](const TransactionType *a, const TransactionType *b) {
                return a->get_id() < b->get_id();
              });
  }

  void prepare_dependency_graph(std::size_t graph_size) {
    if (dependency_graph.size() != graph_size) {
      dependency_graph.resize(graph_size);
    }
    for (auto &edges : dependency_graph) {
      edges.clear();
    }
  }

  void build_query_ranges_cache_if_needed(bool enable_prediction) {
    if (!enable_prediction) {
      txn_ranges_.clear();
      return;
    }

    txn_ranges_.resize(txns_to_schedule_.size());
    for (std::size_t i = 0; i < txns_to_schedule_.size(); ++i) {
      txn_ranges_[i].read.reserve(8);
      txn_ranges_[i].write.reserve(8);
      predictor_.fill_query_ranges(*txns_to_schedule_[i], txn_ranges_[i]);
    }
  }

  double last_macro_abort_rate() const {
    const auto last_total_valid =
        last_macro_total_valid.load(std::memory_order_acquire);
    if (last_total_valid == 0) {
      return 0.0;
    }
    return 1.0 * last_macro_total_delay.load(std::memory_order_acquire) / last_total_valid;
  }

  bool enable_conflict_prediction() const {
    if (prediction_bypass_cooldown_.load(std::memory_order_acquire) > 0) {
      return false;
    }
    const double threshold =
        std::clamp(context.prodcc_abort_rate_threshold, 0.0, 1.0);
    return last_macro_abort_rate() >= threshold;
  }

  std::vector<MicroBlock> partition_into_fixed_micro_blocks(
      const std::vector<TransactionType*>& transactions_to_schedule,
      std::size_t micro_block_size) {
    std::vector<MicroBlock> result;
    if (transactions_to_schedule.empty() || micro_block_size == 0) {
      return result;
    }

    result.reserve((transactions_to_schedule.size() + micro_block_size - 1) /
                   micro_block_size);

    for (std::size_t i = 0; i < transactions_to_schedule.size();
         i += micro_block_size) {
      MicroBlock mb;
      const std::size_t end =
          std::min(i + micro_block_size, transactions_to_schedule.size());
      mb.reserve(end - i);
      for (std::size_t j = i; j < end; ++j) {
        mb.push_back(transactions_to_schedule[j]);
      }
      result.push_back(std::move(mb));
    }
    return result;
  }

 public:
  void partition_task(std::size_t worker_id) {
      const bool enable_prediction = enable_conflict_prediction();
      if (!enable_prediction) {
        return;
      }

      const double toxic_query_rate =
          std::clamp(context.prodcc_toxic_query_rate, 0.0, 1.0);
      static thread_local aria::Random toxic_rng;
      static thread_local bool toxic_rng_seeded = false;
      if (!toxic_rng_seeded) {
        toxic_rng.init_seed(static_cast<uint64_t>(worker_id) ^
                            (reinterpret_cast<uint64_t>(this) >> 4));
        toxic_rng_seeded = true;
      }

      const auto &txns_to_schedule = txns_to_schedule_;
      const auto &txn_ranges = txn_ranges_;

      std::size_t total_workers = context.worker_num;
      if (worker_id >= total_workers) {
        return;
      }
      std::size_t n = txns_to_schedule.size();
      if (n <= 1) return;
      DCHECK(!dependency_graph.empty());

      std::unordered_map<std::size_t,
                         std::vector<std::pair<std::size_t, ConflictType>>>
          local_edges;
      local_edges.reserve(256);

      for (std::size_t i = worker_id; i < n; i += total_workers) {
          for (std::size_t j = i + 1; j < n; ++j) {
              TransactionType* t1 = txns_to_schedule[i];
              TransactionType* t2 = txns_to_schedule[j];

              ConflictType conflict =
                  predictor_.predict_conflict(txn_ranges[i], txn_ranges[j]);
              if (toxic_query_rate > 0.0 &&
                  toxic_rng.next_double() < toxic_query_rate) {
                conflict = ConflictType::WAW;
              } 
                
              if (conflict != ConflictType::NONE) {
                  local_edges[t1->get_id()].push_back({t2->get_id(), conflict});
              }
          }
      }

      for (auto &kv : local_edges) {
        const std::size_t src_id = kv.first;
        auto &src = kv.second;
        if (src.empty()) {
          continue;
        }

        auto &mtx = graph_build_locks_[src_id % kGraphLockShards];
        std::lock_guard<std::mutex> lock(mtx);
        auto &dst = dependency_graph[src_id];
        dst.insert(dst.end(), std::make_move_iterator(src.begin()),
                   std::make_move_iterator(src.end()));
      }
  }

private:
  std::vector<MicroBlock> partition_into_micro_blocks(
      const std::vector<TransactionType*>& transactions_to_schedule) {
      
      std::vector<MicroBlock> final_schedule;
      if (transactions_to_schedule.empty()) {
          return final_schedule;
      }

      std::vector<TransactionType*> remaining_txns = transactions_to_schedule;
      std::vector<TransactionType*> postponed_txns;

       while (!remaining_txns.empty()) {
           MicroBlock current_mb;
           postponed_txns.clear();

           for (auto* t_i : remaining_txns) {
               bool should_postpone = false;
               for (const auto* t_j : current_mb) {
                  const auto& neighbors = dependency_graph[t_j->get_id()];
                  for (const auto& edge : neighbors) {
                      if (edge.first == t_i->get_id()) {
                          ConflictType conflict = edge.second;
                          if (conflict == ConflictType::WAW || conflict == ConflictType::RAW) {
                              should_postpone = true;
                              break;
                          }
                          if (!context.aria_reordering_optmization && conflict == ConflictType::WAR) {
                              should_postpone = true;
                              break;
                          }
                      }
                  }
                  if (should_postpone) break;
              }

              if (should_postpone) {
                  postponed_txns.push_back(t_i);
              } else {
                  current_mb.push_back(t_i);
              }
          }

          if (current_mb.empty() && !remaining_txns.empty()) {
              current_mb.push_back(remaining_txns.front());
              postponed_txns.erase(std::remove(postponed_txns.begin(), postponed_txns.end(), remaining_txns.front()), postponed_txns.end());
          }

          if (!current_mb.empty()) {
              final_schedule.push_back(std::move(current_mb));
          }
          
          remaining_txns = std::move(postponed_txns);
      }
      return final_schedule;
  }


 private:
  void coordinator_run_worker_phase(ExecutorStatus status,
                                    std::size_t n_coordinators) {
    n_started_workers.store(0);
    n_completed_workers.store(0);
    signal_worker(status);
    wait_all_workers_start();
    wait_all_workers_finish();

    broadcast_stop();
    wait4_stop(n_coordinators - 1);
    n_completed_workers.store(0);
    set_worker_status(ExecutorStatus::STOP);
    wait_all_workers_finish();
    wait4_ack();
  }

  void follower_run_worker_phase(ExecutorStatus expected_status,
                                 std::size_t n_coordinators) {
    n_started_workers.store(0);
    n_completed_workers.store(0);
    set_worker_status(expected_status);
    wait_all_workers_start();
    wait_all_workers_finish();

    broadcast_stop();
    wait4_stop(n_coordinators - 1);
    n_completed_workers.store(0);
    set_worker_status(ExecutorStatus::STOP);
    wait_all_workers_finish();
    send_ack();
  }

  int receive_single_vector_value() {
    vector_in_queue.wait_till_non_empty();
    std::unique_ptr<Message> message(vector_in_queue.front());
    bool ok = vector_in_queue.pop();
    CHECK(ok);
    CHECK(message->get_message_count() == 1);

    MessagePiece messagePiece = *(message->begin());
    auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
    CHECK(type == ControlMessage::VECTOR);

    std::size_t sz;
    StringPiece stringPiece = messagePiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> sz;
    CHECK(sz == 1);

    int value;
    dec >> value;
    CHECK(dec.size() == 0);
    return value;
  }

  std::size_t align_schedule_size(std::size_t local_size,
                                  std::size_t n_coordinators) {
    if (coordinator_id == 0) {
      std::size_t max_size = local_size;
      for (std::size_t i = 0; i < n_coordinators - 1; ++i) {
        max_size = std::max<std::size_t>(
            max_size, static_cast<std::size_t>(receive_single_vector_value()));
      }

      const std::vector<int> payload{static_cast<int>(max_size)};
      for (std::size_t i = 1; i < n_coordinators; ++i) {
        ControlMessageFactory::new_vector_message(*messages[i], payload);
      }
      flush_messages();
      return max_size;
    }

    const std::vector<int> payload{static_cast<int>(local_size)};
    ControlMessageFactory::new_vector_message(*messages[0], payload);
    flush_messages();
    return static_cast<std::size_t>(receive_single_vector_value());
  }

  void pad_schedule_to_size(std::size_t target_size) {
    while (schedule.size() < target_size) {
      schedule.emplace_back();
    }
  }

 public:
  const MicroBlock& get_current_micro_block() const {
      DCHECK(current_micro_block != nullptr);
      return *current_micro_block;
  }

  void coordinator_start() override {
    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    while (!stopFlag.load()) {
        epoch.fetch_add(1);
        cleanup_batch(); 
        
        coordinator_run_worker_phase(ExecutorStatus::Aria_READ,
                                     n_coordinators);

        const bool enable_prediction = enable_conflict_prediction();
        last_macro_prediction_enabled_.store(enable_prediction,
                                            std::memory_order_release);
        if (enable_prediction) {
          prepare_dependency_graph(transactions.size() * n_coordinators + 1);
        }
        build_txns_to_schedule();
        build_query_ranges_cache_if_needed(enable_prediction);

        coordinator_run_worker_phase(ExecutorStatus::PRODCC_PARTITION,
                                     n_coordinators);

        if (enable_prediction) {
          schedule = partition_into_micro_blocks(txns_to_schedule_);
        } else {
          schedule =
              partition_into_fixed_micro_blocks(txns_to_schedule_, context.batch_size);
        }
        pad_schedule_to_size(align_schedule_size(schedule.size(), n_coordinators));

        for (std::size_t mb_idx = 0; mb_idx < schedule.size(); ++mb_idx) {
            auto& mb = schedule[mb_idx];
            current_micro_block = &mb;
            coordinator_run_worker_phase(
                ExecutorStatus::PRODCC_EXECUTE_MICRO_BLOCK, n_coordinators);
            coordinator_run_worker_phase(
                ExecutorStatus::PRODCC_RESERVE_MICRO_BLOCK, n_coordinators);
            coordinator_run_worker_phase(
                ExecutorStatus::PRODCC_ANALYZE_MICRO_BLOCK, n_coordinators);
            coordinator_run_worker_phase(
                ExecutorStatus::PRODCC_FINALIZE_MICRO_BLOCK, n_coordinators);
            coordinator_run_worker_phase(
                ExecutorStatus::PRODCC_CLEANUP_MICRO_BLOCK, n_coordinators);
        }
        current_micro_block = nullptr;
    }
    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override {
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {
        ExecutorStatus status = wait4_signal();
        if (status == ExecutorStatus::EXIT) {
            set_worker_status(ExecutorStatus::EXIT);
            break;
        }

        DCHECK(status == ExecutorStatus::Aria_READ);
        epoch.fetch_add(1);
        cleanup_batch();
        
        follower_run_worker_phase(ExecutorStatus::Aria_READ, n_coordinators);

        status = wait4_signal();
        DCHECK(status == ExecutorStatus::PRODCC_PARTITION);
        const bool enable_prediction = enable_conflict_prediction();
        last_macro_prediction_enabled_.store(enable_prediction,
                                            std::memory_order_release);
        if (enable_prediction) {
          prepare_dependency_graph(transactions.size() * n_coordinators + 1);
        }
        build_txns_to_schedule();
        build_query_ranges_cache_if_needed(enable_prediction);

        follower_run_worker_phase(ExecutorStatus::PRODCC_PARTITION,
                                  n_coordinators);

        if (enable_prediction) {
          schedule = partition_into_micro_blocks(txns_to_schedule_);
        } else {
          schedule =
              partition_into_fixed_micro_blocks(txns_to_schedule_, context.batch_size);
        }
        pad_schedule_to_size(align_schedule_size(schedule.size(), n_coordinators));

        for (std::size_t mb_idx = 0; mb_idx < schedule.size(); ++mb_idx) {
            auto& mb = schedule[mb_idx];
            current_micro_block = &mb;
            status = wait4_signal();
            DCHECK(status == ExecutorStatus::PRODCC_EXECUTE_MICRO_BLOCK);
            follower_run_worker_phase(
                ExecutorStatus::PRODCC_EXECUTE_MICRO_BLOCK, n_coordinators);
            status = wait4_signal();
            DCHECK(status == ExecutorStatus::PRODCC_RESERVE_MICRO_BLOCK);
            follower_run_worker_phase(
                ExecutorStatus::PRODCC_RESERVE_MICRO_BLOCK, n_coordinators);
            status = wait4_signal();
            DCHECK(status == ExecutorStatus::PRODCC_ANALYZE_MICRO_BLOCK);
            follower_run_worker_phase(
                ExecutorStatus::PRODCC_ANALYZE_MICRO_BLOCK, n_coordinators);
            status = wait4_signal();
            DCHECK(status == ExecutorStatus::PRODCC_FINALIZE_MICRO_BLOCK);
            follower_run_worker_phase(
                ExecutorStatus::PRODCC_FINALIZE_MICRO_BLOCK, n_coordinators);
            status = wait4_signal();
            DCHECK(status == ExecutorStatus::PRODCC_CLEANUP_MICRO_BLOCK);
            follower_run_worker_phase(
                ExecutorStatus::PRODCC_CLEANUP_MICRO_BLOCK, n_coordinators);
        }
        current_micro_block = nullptr;
    }
  }

  void cleanup_batch() {
    std::size_t it = 0;
    std::size_t it2 = 0;
    std::size_t total_valid = 0;
    for (auto i = 0u; i < transactions.size(); i++) {
      if (transactions[i] == nullptr) {
        break;
      }
      const bool should_retry = transactions[i]->abort_lock;
      const bool delayed = transactions[i]->delay_lock;
      if (!transactions[i]->abort_no_retry) {
        total_valid++;
      }
      if (should_retry) {
        transactions[it++].swap(transactions[i]);
      }
      if (delayed) {
        it2++;
      }
    }
    total_abort.store(it);
    last_macro_total_delay.store(it2);
    last_macro_total_valid.store(total_valid);
    const uint32_t abort_ppm =
        total_valid == 0 ? 0u
                         : static_cast<uint32_t>((1000000.0 * it2) / total_valid);
    last_macro_abort_ppm_.store(abort_ppm, std::memory_order_release);

    uint32_t cooldown =
        prediction_bypass_cooldown_.load(std::memory_order_acquire);
    if (cooldown > 0) {
      prediction_bypass_cooldown_.store(cooldown - 1, std::memory_order_release);
    }

    const bool last_used_prediction =
        last_macro_prediction_enabled_.load(std::memory_order_acquire);
    if (!last_used_prediction) {
      bypass_baseline_abort_ppm_.store(abort_ppm, std::memory_order_release);
      return;
    }

    const uint32_t baseline_ppm =
        bypass_baseline_abort_ppm_.load(std::memory_order_acquire);
    static constexpr uint32_t kRegressionMarginPpm = 10000;
    static constexpr uint32_t kBypassCooldownEpochs = 5;
    if (abort_ppm > baseline_ppm + kRegressionMarginPpm) {
      prediction_bypass_cooldown_.store(kBypassCooldownEpochs,
                                       std::memory_order_release);
    }
  }

private:
  std::vector<MicroBlock> schedule;
  MicroBlock* current_micro_block;
  ConflictPredictor predictor_;
  std::unique_ptr<Partitioner> partitioner;
  std::vector<std::vector<std::pair<std::size_t, ConflictType>>> dependency_graph;
  static constexpr std::size_t kGraphLockShards = 64;
  std::array<std::mutex, kGraphLockShards> graph_build_locks_{};
  std::vector<TransactionType*> txns_to_schedule_;
  std::vector<TxnQueryRanges> txn_ranges_;

public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
  std::atomic<uint32_t> total_abort;
  std::atomic<uint32_t> last_macro_total_delay{0};
  std::atomic<uint32_t> last_macro_total_valid{0};
  std::atomic<uint32_t> last_macro_abort_ppm_{0};
  std::atomic<bool> last_macro_prediction_enabled_{false};
  std::atomic<uint32_t> bypass_baseline_abort_ppm_{0};
  std::atomic<uint32_t> prediction_bypass_cooldown_{0};
};
} // namespace aria
