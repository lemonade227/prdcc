#pragma once

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/DOCC/DOCCMessage.h"
#include "protocol/DOCC/DOCTransaction.h"

namespace aria {

template <class Database> class DOCC {
public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using MessageType = DOCCMessage;
  using TransactionType = DOCTransaction;

  using MessageFactoryType = DOCCMessageFactory;
  using MessageHandlerType = DOCCMessageHandler;

  DOCC(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
      : db(db), context(context), partitioner(partitioner) {}

  void abort(TransactionType &txn,
             std::vector<std::unique_ptr<Message>> &messages) {
    (void)txn;
    (void)messages;
  }

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
        table->insert(key, value, txn.id);
        table->update(key, value, txn.id);
      } else {
        auto coordinatorID = partitioner.master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_write_message(
            *messages[coordinatorID], *table, writeKey.get_key(),
            writeKey.get_value(), txn.id);
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

