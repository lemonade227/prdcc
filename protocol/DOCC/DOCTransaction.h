#pragma once

#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "protocol/DOCC/DOCCRWKey.h"
#include "core/sql/SqlMetrics.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

namespace aria {

class DOCTransaction {
public:
  DOCTransaction(std::size_t coordinator_id, std::size_t partition_id,
                 Partitioner &partitioner)
      : coordinator_id(coordinator_id), partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner) {
    reset();
  }

  virtual ~DOCTransaction() = default;

  void reset() {
    sql::on_transaction_reset(sql_state);
    abort_no_retry = false;
    distributed_transaction = false;
    execution_phase = false;
    pendingResponses = 0;
    network_size = 0;
    operation.clear();
    readSet.clear();
    writeSet.clear();
  }

  virtual TransactionResult execute(std::size_t worker_id) = 0;
  virtual void reset_query() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }

    DOCCRWKey readKey;
    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);
    readKey.set_key(&key);
    readKey.set_value(&value);
    readKey.set_local_index_read_bit();
    readKey.set_read_request_bit();
    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }

    DOCCRWKey readKey;
    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);
    readKey.set_key(&key);
    readKey.set_value(&value);
    readKey.set_read_request_bit();
    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }

    DOCCRWKey readKey;
    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);
    readKey.set_key(&key);
    readKey.set_value(&value);
    readKey.set_read_request_bit();
    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id, const KeyType &key,
              const ValueType &value) {
    if (execution_phase) {
      return;
    }

    DOCCRWKey writeKey;
    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);
    writeKey.set_key(&key);
    writeKey.set_value(const_cast<ValueType *>(&value));
    add_to_write_set(writeKey);
  }

  std::size_t add_to_read_set(const DOCCRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const DOCCRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  void set_id(uint64_t id) { this->id = id; }
  void set_tid_offset(std::size_t offset) { this->tid_offset = offset; }
  void set_epoch(uint32_t epoch) { this->epoch = epoch; }

  bool process_requests(std::size_t worker_id) {
    (void)worker_id;
    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      if (!readSet[i].get_read_request_bit()) {
        break;
      }
      DOCCRWKey &readKey = readSet[i];
      readRequestHandler(readKey, id, static_cast<uint32_t>(tid_offset),
                         static_cast<uint32_t>(i));
      readSet[i].clear_read_request_bit();
    }
    return false;
  }

  bool is_read_only() const { return writeSet.empty(); }

public:
  std::size_t coordinator_id = 0;
  std::size_t partition_id = 0;
  uint64_t id = 0;
  std::size_t tid_offset = 0;
  uint32_t epoch = 0;

  std::chrono::steady_clock::time_point startTime;

  std::size_t pendingResponses = 0;
  std::size_t network_size = 0;

  bool abort_no_retry = false;
  bool distributed_transaction = false;
  bool execution_phase = false;

  // read_key, tid, tid_offset, key_offset
  std::function<void(DOCCRWKey &, uint64_t, uint32_t, uint32_t)>
      readRequestHandler;
  std::function<std::size_t(void)> remote_request_handler;
  std::function<void()> message_flusher;

  Partitioner &partitioner;
  sql::SqlTransactionState sql_state;
  Operation operation;
  std::vector<DOCCRWKey> readSet;
  std::vector<DOCCRWKey> writeSet;
};

} // namespace aria
