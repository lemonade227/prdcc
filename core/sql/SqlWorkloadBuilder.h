#pragma once

#include "benchmark/tpcc/Query.h"
#include "benchmark/ycsb/Query.h"
#include "core/sql/SqlExecKernel.h"

#include <cstdint>
#include <string>
#include <vector>

namespace aria::sql {

template <std::size_t Keys>
inline std::vector<SqlStatementSpec>
build_ycsb_statements(const ycsb::YCSBQuery<Keys> &query) {
  std::vector<SqlStatementSpec> statements;
  statements.reserve(Keys);

  for (std::size_t i = 0; i < Keys; i++) {
    SqlStatementSpec statement;
    if (query.UPDATE[i]) {
      statement.sql =
          "UPDATE YCSB SET Y_F01 = ?, Y_F02 = ?, Y_F03 = ?, Y_F04 = ?, "
          "Y_F05 = ?, Y_F06 = ?, Y_F07 = ?, Y_F08 = ?, Y_F09 = ?, "
          "Y_F10 = ? WHERE Y_KEY = ?";
      for (std::size_t field = 0; field < 10; field++) {
        statement.params.push_back(
            static_cast<std::uint64_t>(query.Y_KEY[i] + field + 1));
      }
      statement.params.push_back(static_cast<std::uint64_t>(query.Y_KEY[i]));
    } else {
      statement.sql =
          "SELECT Y_F01, Y_F02, Y_F03, Y_F04, Y_F05, Y_F06, Y_F07, Y_F08, "
          "Y_F09, Y_F10 FROM YCSB WHERE Y_KEY = ?";
      statement.params.push_back(static_cast<std::uint64_t>(query.Y_KEY[i]));
    }
    statements.push_back(std::move(statement));
  }

  return statements;
}

inline std::vector<SqlStatementSpec>
build_new_order_statements(const tpcc::NewOrderQuery &query) {
  std::vector<SqlStatementSpec> statements;
  statements.reserve(static_cast<std::size_t>(query.O_OL_CNT) * 4 + 5);

  const auto warehouse_id = static_cast<std::uint64_t>(query.W_ID);
  const auto district_id = static_cast<std::uint64_t>(query.D_ID);
  const auto customer_id = static_cast<std::uint64_t>(query.C_ID);
  const auto order_id = static_cast<std::uint64_t>(query.O_ID);

  statements.push_back({"SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?",
                        {warehouse_id}});
  statements.push_back(
      {"UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ?",
       {order_id + 1, warehouse_id, district_id}});
  statements.push_back(
      {"SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? "
       "AND C_D_ID = ? AND C_ID = ?",
       {warehouse_id, district_id, customer_id}});

  for (int i = 0; i < query.O_OL_CNT; i++) {
    const auto item_id = static_cast<std::uint64_t>(query.INFO[i].OL_I_ID);
    const auto supply_w_id =
        static_cast<std::uint64_t>(query.INFO[i].OL_SUPPLY_W_ID);
    const auto quantity =
        static_cast<std::uint64_t>(query.INFO[i].OL_QUANTITY);

    statements.push_back({"SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE "
                          "I_ID = ?",
                          {item_id}});
    statements.push_back(
        {"UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, "
         "S_REMOTE_CNT = ? WHERE S_W_ID = ? AND S_I_ID = ?",
         {quantity + 1, quantity + 2, quantity + 3, quantity + 4, supply_w_id,
          item_id}});
    statements.push_back({"INSERT INTO ORDER_LINE VALUES (?, ?, ?, ?, ?, ?, ?)",
                          {warehouse_id, district_id, order_id,
                           static_cast<std::uint64_t>(i + 1), item_id, supply_w_id, quantity}});
  }

  statements.push_back(
      {"INSERT INTO ORDERS VALUES (?, ?, ?, ?, ?, ?)",
       {warehouse_id, district_id, order_id, customer_id,
        static_cast<std::uint64_t>(query.O_OL_CNT),
        static_cast<std::uint64_t>(query.isRemote() ? 1 : 0)}});
  statements.push_back({"INSERT INTO NEW_ORDER VALUES (?, ?, ?)",
                        {warehouse_id, district_id, order_id}});

  return statements;
}

inline std::vector<SqlStatementSpec>
build_payment_statements(const tpcc::PaymentQuery &query, bool write_to_w_ytd) {
  std::vector<SqlStatementSpec> statements;
  statements.reserve(8);

  const auto warehouse_id = static_cast<std::uint64_t>(query.W_ID);
  const auto district_id = static_cast<std::uint64_t>(query.D_ID);
  const auto customer_id = static_cast<std::uint64_t>(query.C_ID);
  const auto customer_w_id = static_cast<std::uint64_t>(query.C_W_ID);
  const auto customer_d_id = static_cast<std::uint64_t>(query.C_D_ID);
  const auto amount = static_cast<std::uint64_t>(query.H_AMOUNT * 100);

  if (write_to_w_ytd) {
    statements.push_back({"UPDATE WAREHOUSE SET W_YTD = ? WHERE W_ID = ?",
                          {amount, warehouse_id}});
  }

  statements.push_back(
      {"UPDATE DISTRICT SET D_YTD = ? WHERE D_W_ID = ? AND D_ID = ?",
       {amount, warehouse_id, district_id}});

  if (query.C_ID == 0) {
    statements.push_back(
        {"SELECT C_ID FROM CUSTOMER_NAME_IDX WHERE C_W_ID = ? AND C_D_ID = ? "
         "AND C_LAST = ?",
         {customer_w_id, customer_d_id,
          static_cast<std::uint64_t>(query.C_LAST.size())}});
  }

  statements.push_back(
      {"SELECT C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_CREDIT FROM CUSTOMER "
       "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",
       {customer_w_id, customer_d_id, customer_id}});
  statements.push_back(
      {"UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? "
       "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",
       {amount + 1, amount + 2, amount + 3, customer_w_id, customer_d_id,
        customer_id}});
  statements.push_back({"INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?)",
                        {warehouse_id, district_id, customer_w_id,
                         customer_d_id, customer_id, amount}});

  return statements;
}

inline std::vector<SqlStatementSpec>
build_delivery_statements(const tpcc::DeliveryQuery &query,
                          int district_count) {
  std::vector<SqlStatementSpec> statements;
  statements.reserve(static_cast<std::size_t>(district_count) * 7);

  const auto warehouse_id = static_cast<std::uint64_t>(query.W_ID);
  const auto carrier_id = static_cast<std::uint64_t>(query.O_CARRIER_ID);

  for (int district_id = 1; district_id <= district_count; district_id++) {
    if (query.NO_O_ID[district_id - 1] == 0) {
      continue;
    }
    const auto d_id = static_cast<std::uint64_t>(district_id);
    const auto order_id =
        static_cast<std::uint64_t>(query.NO_O_ID[district_id - 1]);
    const auto customer_id =
        static_cast<std::uint64_t>(query.O_C_ID[district_id - 1]);

    statements.push_back(
        {"SELECT NO_O_ID FROM NEW_ORDER WHERE NO_W_ID = ? AND NO_D_ID = ? "
         "ORDER BY NO_O_ID LIMIT 1",
         {warehouse_id, d_id}});
    statements.push_back(
        {"UPDATE NEW_ORDER SET NO_DUMMY = ? WHERE NO_W_ID = ? AND NO_D_ID = ? "
         "AND NO_O_ID = ?",
         {1, warehouse_id, d_id, order_id}});
    statements.push_back(
        {"SELECT O_C_ID, O_OL_CNT FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? "
         "AND O_ID = ?",
         {warehouse_id, d_id, order_id}});
    statements.push_back(
        {"UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_W_ID = ? AND O_D_ID = ? "
         "AND O_ID = ?",
         {carrier_id, warehouse_id, d_id, order_id}});
    statements.push_back(
        {"SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_W_ID = ? AND "
         "OL_D_ID = ? AND OL_O_ID = ?",
         {warehouse_id, d_id, order_id}});
    statements.push_back(
        {"UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_W_ID = ? AND "
         "OL_D_ID = ? AND OL_O_ID = ?",
         {carrier_id, warehouse_id, d_id, order_id}});
    statements.push_back(
        {"UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ?, "
         "C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_W_ID = ? AND "
         "C_D_ID = ? AND C_ID = ?",
         {order_id, warehouse_id, d_id, customer_id}});
  }

  return statements;
}

} // namespace aria::sql
