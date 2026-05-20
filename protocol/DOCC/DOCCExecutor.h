#pragma once

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Partitioner.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/DOCC/DOCC.h"

#include <chrono>
#include <cstring>
#include <thread>

namespace aria {

template <class Workload> class DOCCExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = DOCTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");

  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = DOCC<DatabaseType>;

  using MessageType = DOCCMessage;
  using MessageFactoryType = DOCCMessageFactory;
  using MessageHandlerType = DOCCMessageHandler;

  DOCCExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context,
               std::vector<std::unique_ptr<TransactionType>> &transactions,
               std::vector<StorageType> &storages, std::atomic<uint32_t> &epoch,
               std::vector<Percentile<int64_t>> &latency_percentiles,
               std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        transactions(transactions), storages(storages), epoch(epoch),
        latency_percentiles(latency_percentiles),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        enable_validation_retry(false),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
        workload(coordinator_id, db, random, *partitioner),
        random(reinterpret_cast<uint64_t>(this)),
        protocol(db, context, *partitioner),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  ~DOCCExecutor() = default;

  void start() override {
    sql::register_worker_metrics(&sql_metrics);
    LOG(INFO) << "DOCCExecutor " << id << " started. ";

    for (;;) {
      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());
        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "DOCCExecutor " << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::DOCC_EXECUTE);

      n_started_workers.fetch_add(1);
      execute_transactions();
      n_complete_workers.fetch_add(1);

      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::DOCC_EXECUTE) {
        process_request();
        std::this_thread::yield();
      }
      process_request();
      n_complete_workers.fetch_add(1);

      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::DOCC_COMMIT) {
        std::this_thread::yield();
      }

      n_started_workers.fetch_add(1);
      if (id == 0) {
        validate_and_commit_transactions();
      } else {
        process_request();
      }
      n_complete_workers.fetch_add(1);

      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::DOCC_COMMIT) {
        process_request();
        std::this_thread::yield();
      }
      process_request();
      n_complete_workers.fetch_add(1);
    }
  }

  void onExit() override {
    auto &worker_latency = latency_percentiles[id];
    LOG(INFO) << "Worker " << id << " latency: " << worker_latency.nth(50)
              << " us (50%) " << worker_latency.nth(75) << " us (75%) "
              << worker_latency.nth(95) << " us (95%) "
              << worker_latency.nth(99) << " us (99%).";
  }

  void push_message(Message *message) override { in_queue.push(message); }

  Message *pop_message() override {
    if (out_queue.empty()) {
      return nullptr;
    }

    Message *message = out_queue.front();

    if (delay->delay_enabled()) {
      auto now = std::chrono::steady_clock::now();
      if (std::chrono::duration_cast<std::chrono::microseconds>(now -
                                                                message->time)
              .count() < delay->message_delay()) {
        return nullptr;
      }
    }

    bool ok = out_queue.pop();
    CHECK(ok);
    return message;
  }

private:
  uint64_t make_tid(uint32_t epoch, uint32_t pos) {
    return (uint64_t(epoch) << 32) | uint64_t(pos);
  }

  std::size_t get_partition_id() {
    std::size_t partition_id;
    CHECK(context.partition_num % context.coordinator_num == 0);
    auto partition_num_per_node =
        context.partition_num / context.coordinator_num;
    CHECK(partition_num_per_node > 0)
        << "Invalid config: partition_num (" << context.partition_num
        << ") < coordinator_num (" << context.coordinator_num
        << "). Increase --partition_num or reduce --servers.";
    partition_id = random.uniform_dist(0, partition_num_per_node - 1) *
                       context.coordinator_num +
                   coordinator_id;
    CHECK(partitioner->has_master_partition(partition_id));
    return partition_id;
  }

  void setupHandlers(TransactionType &txn) {
    auto *txn_ptr = &txn;
    txn.readRequestHandler = [this, txn_ptr](DOCCRWKey &readKey, uint64_t tid,
                                    uint32_t tid_offset,
                                    uint32_t key_offset) {
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (partitioner->has_master_partition(partitionId)) {
        auto key = readKey.get_key();
        auto value_size = table->value_size();
        auto [meta, value, version] = table->search_latest(key);
        (void)meta;
        std::memcpy(readKey.get_value(), value, value_size);
        readKey.set_read_version(version);
      } else {
        auto coordinatorID = partitioner->master_coordinator(partitionId);
        txn_ptr->network_size += MessageFactoryType::new_search_message(
            *messages[coordinatorID], *table, tid, tid_offset, readKey.get_key(),
            key_offset);
        txn_ptr->distributed_transaction = true;
        txn_ptr->pendingResponses++;
      }
    };

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(); };
  }

  void execute_one_transaction(TransactionType &txn) {
    txn.execution_phase = false;
    auto result = txn.execute(id);
    if (result != TransactionResult::READY_TO_COMMIT) {
      txn.abort_no_retry = true;
      return;
    }

    flush_messages();
    while (txn.pendingResponses > 0) {
      process_request();
    }

    txn.execution_phase = true;
    txn.execute(id);
  }

  void execute_transactions() {
    if (context.coordinator_num != 1) {
      LOG(WARNING) << "DOCC is currently implemented for single-node mode. "
                      "Set --servers to a single node.";
    }

    auto cur_epoch = epoch.load();

    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      process_request();

      auto partition_id = get_partition_id();
      transactions[i] =
          workload.next_transaction(context, partition_id, storages[i]);

      transactions[i]->set_epoch(cur_epoch);
      transactions[i]->set_tid_offset(i);
      transactions[i]->set_id(make_tid(cur_epoch, static_cast<uint32_t>(i)));
      transactions[i]->startTime = std::chrono::steady_clock::now();

      setupHandlers(*transactions[i]);
      execute_one_transaction(*transactions[i]);
    }
  }

  bool validate(TransactionType &txn) {
    for (auto &readKey : txn.readSet) {
      if (readKey.get_local_index_read_bit()) {
        continue;
      }

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      if (!partitioner->has_master_partition(partitionId)) {
        CHECK(context.coordinator_num == 1)
            << "DOCC distributed validation is not implemented.";
      }

      auto table = db.find_table(tableId, partitionId);
      auto [meta, value, version] = table->search_latest(readKey.get_key());
      (void)meta;
      (void)value;
      if (version != readKey.get_read_version()) {
        return false;
      }
    }
    return true;
  }

  void validate_and_commit_transactions() {
    for (auto i = 0u; i < transactions.size(); i++) {
      process_request();

      if (transactions[i] == nullptr || transactions[i]->abort_no_retry) {
        n_abort_no_retry.fetch_add(1);
        continue;
      }

      TransactionType &txn = *transactions[i];

      if (!validate(txn)) {
        if (!enable_validation_retry) {
          n_abort_read_validation.fetch_add(1);
          continue;
        }

        uint32_t retry_count = 0;
        while (!validate(txn)) {
          retry_count++;
          if (retry_count > 1000) {
            LOG(ERROR) << "DOCC validation retry overflow (tid=" << txn.id
                       << ", tid_offset=" << txn.tid_offset
                       << ", epoch=" << txn.epoch
                       << "). Marking transaction as abort_no_retry to avoid stall.";
            txn.abort_no_retry = true;
            break;
          }
          txn.reset();
          setupHandlers(txn);
          execute_one_transaction(txn);
          CHECK(!txn.abort_no_retry);
        }

        if (txn.abort_no_retry) {
          n_abort_no_retry.fetch_add(1);
          continue;
        }
      }

      bool committed = protocol.commit(txn, messages);
      flush_messages();
      CHECK(committed);

      n_network_size.fetch_add(txn.network_size);
      n_commit.fetch_add(1);

      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - txn.startTime)
                         .count();
      latency_percentiles[i % context.worker_num].add(latency);

      // Keep only the latest committed version per written key.
      for (auto &writeKey : txn.writeSet) {
        auto table = db.find_table(writeKey.get_table_id(),
                                   writeKey.get_partition_id());
        if (partitioner->has_master_partition(writeKey.get_partition_id())) {
          table->garbage_collect(writeKey.get_key());
        }
      }
    }
    flush_messages();
  }

  void flush_messages() {
    for (auto i = 0u; i < messages.size(); i++) {
      if (i == coordinator_id) {
        continue;
      }
      if (messages[i]->get_message_count() == 0) {
        continue;
      }
      out_queue.push(messages[i].release());
      messages[i] = std::make_unique<Message>();
      init_message(messages[i].get(), i);
    }
  }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

  std::size_t process_request() {
    std::size_t size = 0;
    while (!in_queue.empty()) {
      std::unique_ptr<Message> message(in_queue.front());
      bool ok = in_queue.pop();
      CHECK(ok);

      for (auto it = message->begin(); it != message->end(); it++) {
        MessagePiece messagePiece = *it;
        auto type = messagePiece.get_message_type();
        DCHECK(type < messageHandlers.size());
        ITable *table = db.find_table(messagePiece.get_table_id(),
                                      messagePiece.get_partition_id());
        messageHandlers[type](messagePiece,
                              *messages[message->get_source_node_id()], *table,
                              transactions);
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::vector<StorageType> &storages;
  std::atomic<uint32_t> &epoch;
  std::vector<Percentile<int64_t>> &latency_percentiles;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  const bool enable_validation_retry;
  std::unique_ptr<Partitioner> partitioner;
  WorkloadType workload;
  RandomType random;
  ProtocolType protocol;
  std::unique_ptr<Delay> delay;

  std::vector<std::unique_ptr<Message>> messages;
  std::vector<
      std::function<void(MessagePiece, Message &, ITable &,
                         std::vector<std::unique_ptr<TransactionType>> &)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue;
};

} // namespace aria
