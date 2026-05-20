#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/DOCC/DOCTransaction.h"
#include "protocol/DOCC/DOCCRWKey.h"

#include <cstring>
#include <functional>
#include <string>
#include <vector>

namespace aria {

enum class DOCCMessage {
  SEARCH_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  SEARCH_RESPONSE,
  WRITE_REQUEST,
  NFIELDS
};

class DOCCMessageFactory {
public:
  static std::size_t new_search_message(Message &message, ITable &table,
                                        uint64_t tid, uint32_t tid_offset,
                                        const void *key, uint32_t key_offset) {
    /*
     * SEARCH_REQUEST: (primary key, tid, tid_offset, read key offset)
     */
    auto key_size = table.key_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        sizeof(uint64_t) + sizeof(uint32_t) +
                        sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(DOCCMessage::SEARCH_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << tid << tid_offset << key_offset;
    message.flush();
    return message_size;
  }

  static std::size_t new_write_message(Message &message, ITable &table,
                                       const void *key, const void *value,
                                       uint64_t version) {
    /*
     * WRITE_REQUEST: (primary key, field value, version)
     */
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size + field_size +
                        sizeof(uint64_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(DOCCMessage::WRITE_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << version;
    message.flush();
    return message_size;
  }
};

class DOCCMessageHandler {
  using Transaction = DOCTransaction;

public:
  static void
  search_request_handler(MessagePiece inputPiece, Message &responseMessage,
                         ITable &table,
                         std::vector<std::unique_ptr<Transaction>> &txns) {
    (void)txns;
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(DOCCMessage::SEARCH_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * SEARCH_REQUEST: (primary key, tid, tid_offset, read key offset)
     * SEARCH_RESPONSE: (value, version, tid, tid_offset, read key offset)
     */
    auto stringPiece = inputPiece.toStringPiece();
    uint64_t tid;
    uint32_t tid_offset, key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(uint64_t) +
               sizeof(uint32_t) + sizeof(uint32_t));

    const void *key = stringPiece.data();
    auto [meta, value, version] = table.search_latest(key);
    (void)meta;

    stringPiece.remove_prefix(key_size);
    Decoder dec(stringPiece);
    dec >> tid >> tid_offset >> key_offset;
    DCHECK(dec.size() == 0);

    auto message_size = MessagePiece::get_header_size() + value_size +
                        sizeof(uint64_t) + sizeof(uint64_t) +
                        sizeof(uint32_t) + sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(DOCCMessage::SEARCH_RESPONSE), message_size,
        table_id, partition_id);

    Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.data.append(value_size, 0);
    void *dest =
        &responseMessage.data[0] + responseMessage.data.size() - value_size;
    std::memcpy(dest, value, value_size);
    encoder << version << tid << tid_offset << key_offset;
    responseMessage.flush();
  }

  static void
  search_response_handler(MessagePiece inputPiece, Message &responseMessage,
                          ITable &table,
                          std::vector<std::unique_ptr<Transaction>> &txns) {
    (void)responseMessage;
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(DOCCMessage::SEARCH_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto value_size = table.value_size();

    uint64_t version, tid;
    uint32_t tid_offset, key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + value_size + sizeof(uint64_t) +
               sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t));

    StringPiece stringPiece = inputPiece.toStringPiece();
    stringPiece.remove_prefix(value_size);
    Decoder dec(stringPiece);
    dec >> version >> tid >> tid_offset >> key_offset;

    CHECK(tid_offset < txns.size());
    CHECK(txns[tid_offset]->id == tid);
    CHECK(key_offset < txns[tid_offset]->readSet.size());

    DOCCRWKey &readKey = txns[tid_offset]->readSet[key_offset];
    readKey.set_read_version(version);
    dec = Decoder(inputPiece.toStringPiece());
    dec.read_n_bytes(readKey.get_value(), value_size);
    txns[tid_offset]->pendingResponses--;
    txns[tid_offset]->network_size += inputPiece.get_message_length();
  }

  static void
  write_request_handler(MessagePiece inputPiece, Message &responseMessage,
                        ITable &table,
                        std::vector<std::unique_ptr<Transaction>> &txns) {
    (void)responseMessage;
    (void)txns;
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(DOCCMessage::WRITE_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto stringPiece = inputPiece.toStringPiece();
    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    auto valueStringPiece = stringPiece;
    valueStringPiece.remove_suffix(sizeof(uint64_t));
    stringPiece.remove_prefix(field_size);

    uint64_t version = 0;
    Decoder dec(stringPiece);
    dec >> version;
    DCHECK(dec.size() == 0);

    // Reserve a new version and install value bytes.
    std::string zero_value(table.value_size(), 0);
    table.insert(key, zero_value.data(), version);
    table.deserialize_value(key, valueStringPiece, version);
    table.search_metadata(key, version).store(0);
  }

public:
  static std::vector<
      std::function<void(MessagePiece, Message &, ITable &,
                         std::vector<std::unique_ptr<Transaction>> &)>>
  get_message_handlers() {
    std::vector<
        std::function<void(MessagePiece, Message &, ITable &,
                           std::vector<std::unique_ptr<Transaction>> &)>>
        v;
    v.resize(static_cast<int>(DOCCMessage::NFIELDS));
    v[static_cast<int>(DOCCMessage::SEARCH_REQUEST)] = search_request_handler;
    v[static_cast<int>(DOCCMessage::SEARCH_RESPONSE)] = search_response_handler;
    v[static_cast<int>(DOCCMessage::WRITE_REQUEST)] = write_request_handler;
    return v;
  }
};

} // namespace aria
