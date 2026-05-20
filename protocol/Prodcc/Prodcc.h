/**
 * @file Prodcc.h
 * @brief Commit and abort primitives for the PRDCC protocol.
 *
 * This module applies buffered transaction writes to local partitions and
 * emits remote write messages for distributed writes. It reuses Aria metadata
 * helpers for deterministic validation bookkeeping while PRDCC's scheduler
 * controls the execution-block order.
 */
#pragma once

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Prodcc/ProdccMessage.h"
#include "protocol/Prodcc/ProdccTransaction.h"

namespace aria {

template <class Database> class Prodcc {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = typename DatabaseType::ContextType;
  using MessageType = ProdccMessage;
  using TransactionType = ProdccTransaction;

  using MessageFactoryType = ProdccMessageFactory;
  using MessageHandlerType = ProdccMessageHandler;

  Prodcc(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
      : db(db), context(context), partitioner(partitioner) {}

  void abort(TransactionType &txn,
             std::vector<std::unique_ptr<Message>> &messages) {}

  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages) {

    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (partitioner.has_master_partition(partitionId)) {
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();
        table->update(key, value);
      } else {
        auto coordinatorID = partitioner.master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_write_message(
            *messages[coordinatorID], *table, txn.id, txn.tid_offset,
            writeKey.get_key(),
            writeKey.get_value());
        txn.pendingResponses++;
      }
    }

    return true;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  Partitioner &partitioner;
};
} // namespace aria
