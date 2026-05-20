/**
 * @file KeyConverter.h
 * @brief Canonical key mapping for PRDCC prediction.
 *
 * PRDCC predicts conflicts over a unified numeric key space. This module maps
 * benchmark-specific YCSB and TPC-C primary/index keys into stable uint64_t
 * ranges so that the predictor can compare accesses from different tables
 * through a single DataSummary domain.
 */
#pragma once

#include "benchmark/tpcc/Schema.h"
#include "benchmark/ycsb/Schema.h"
#include <algorithm>
#include <array>
#include <cstdint>
#include <string_view>

namespace aria {

class KeyConverter {
public:
    static void configure_tpcc(std::size_t warehouse_count,
                               std::size_t districts_per_warehouse) {
        auto& layout = tpcc_layout();
        layout.warehouse_count = std::max<std::size_t>(warehouse_count, 1);
        layout.districts_per_warehouse =
            std::max<std::size_t>(districts_per_warehouse, 1);
    }

    static uint64_t tpcc_min_key() {
        return warehouse_base();
    }

    static uint64_t tpcc_max_key() {
        return order_line_base() + order_line_span() - 1;
    }

    // For YCSB, the key is already a single integer.
    static uint64_t to_uint64(const ycsb::ycsb::key& k) {
        return static_cast<uint64_t>(k.Y_KEY);
    }

    // warehouse::key (W_ID)
    static uint64_t to_uint64(const tpcc::warehouse::key& k) {
        return warehouse_base() + warehouse_index(k.W_ID);
    }

    // district::key (D_W_ID, D_ID)
    static uint64_t to_uint64(const tpcc::district::key& k) {
        return district_base() +
               warehouse_index(k.D_W_ID) * tpcc_layout().districts_per_warehouse +
               district_index(k.D_ID);
    }

    // customer::key (C_W_ID, C_D_ID, C_ID)
    static uint64_t to_uint64(const tpcc::customer::key& k) {
        return customer_base() +
               district_slot(k.C_W_ID, k.C_D_ID) * kCustomersPerDistrict +
               customer_index(k.C_ID);
    }

    // stock::key (S_W_ID, S_I_ID)
    static uint64_t to_uint64(const tpcc::stock::key& k) {
        return stock_base() +
               warehouse_index(k.S_W_ID) * kItemsPerWarehouse +
               item_index(k.S_I_ID);
    }

    // item::key (I_ID) - Note: Item table is not partitioned.
    static uint64_t to_uint64(const tpcc::item::key& k) {
        return item_base() + item_index(k.I_ID);
    }

    // customer_name_idx::key (C_W_ID, C_D_ID, C_LAST)
    static uint64_t to_uint64(const tpcc::customer_name_idx::key& k) {
        return customer_name_idx_base() +
               district_slot(k.C_W_ID, k.C_D_ID) * kLastNamesPerDistrict +
               last_name_index(k.C_LAST);
    }

    static uint64_t to_uint64(const tpcc::new_order::key& k) {
        return new_order_base() +
               district_slot(k.NO_W_ID, k.NO_D_ID) * kOrdersPerDistrict +
               order_index(k.NO_O_ID);
    }

    static uint64_t to_uint64(const tpcc::order::key& k) {
        return order_base() +
               district_slot(k.O_W_ID, k.O_D_ID) * kOrdersPerDistrict +
               order_index(k.O_ID);
    }

    static uint64_t to_uint64(const tpcc::order_line::key& k) {
        return order_line_base() +
               (district_slot(k.OL_W_ID, k.OL_D_ID) * kOrdersPerDistrict +
                order_index(k.OL_O_ID)) * kMaxOrderLinesPerOrder +
               order_line_index(k.OL_NUMBER);
    }

private:
    static constexpr uint64_t kCustomersPerDistrict = 3000;
    static constexpr uint64_t kLastNamesPerDistrict = 1000;
    static constexpr uint64_t kItemsPerWarehouse = 100000;
    static constexpr uint64_t kOrdersPerDistrict = 100000;
    static constexpr uint64_t kMaxOrderLinesPerOrder = 15;

    struct TpccLayout {
        uint64_t warehouse_count = 1;
        uint64_t districts_per_warehouse = 10;
    };

    static TpccLayout& tpcc_layout() {
        static TpccLayout layout;
        return layout;
    }

    static uint64_t warehouse_span() {
        return tpcc_layout().warehouse_count;
    }

    static uint64_t district_span() {
        return warehouse_span() * tpcc_layout().districts_per_warehouse;
    }

    static uint64_t customer_span() {
        return district_span() * kCustomersPerDistrict;
    }

    static uint64_t customer_name_idx_span() {
        return district_span() * kLastNamesPerDistrict;
    }

    static uint64_t stock_span() {
        return warehouse_span() * kItemsPerWarehouse;
    }

    static uint64_t new_order_span() {
        return district_span() * kOrdersPerDistrict;
    }

    static uint64_t order_span() {
        return district_span() * kOrdersPerDistrict;
    }

    static uint64_t order_line_span() {
        return district_span() * kOrdersPerDistrict * kMaxOrderLinesPerOrder;
    }

    static uint64_t warehouse_base() {
        return 0;
    }

    static uint64_t district_base() {
        return warehouse_base() + warehouse_span();
    }

    static uint64_t customer_base() {
        return district_base() + district_span();
    }

    static uint64_t customer_name_idx_base() {
        return customer_base() + customer_span();
    }

    static uint64_t item_base() {
        return customer_name_idx_base() + customer_name_idx_span();
    }

    static uint64_t stock_base() {
        return item_base() + kItemsPerWarehouse;
    }

    static uint64_t new_order_base() {
        return stock_base() + stock_span();
    }

    static uint64_t order_base() {
        return new_order_base() + new_order_span();
    }

    static uint64_t order_line_base() {
        return order_base() + order_span();
    }

    static uint64_t warehouse_index(int32_t warehouse_id) {
        DCHECK_GT(warehouse_id, 0);
        return static_cast<uint64_t>(warehouse_id - 1);
    }

    static uint64_t district_index(int32_t district_id) {
        DCHECK_GT(district_id, 0);
        return static_cast<uint64_t>(district_id - 1);
    }

    static uint64_t customer_index(int32_t customer_id) {
        DCHECK_GT(customer_id, 0);
        return static_cast<uint64_t>(customer_id - 1);
    }

    static uint64_t item_index(int32_t item_id) {
        DCHECK_GT(item_id, 0);
        return static_cast<uint64_t>(item_id - 1);
    }

    static uint64_t order_index(int32_t order_id) {
        DCHECK_GT(order_id, 0);
        return static_cast<uint64_t>(order_id - 1) % kOrdersPerDistrict;
    }

    static uint64_t order_line_index(int32_t order_line_number) {
        DCHECK_GT(order_line_number, 0);
        return static_cast<uint64_t>(order_line_number - 1) %
               kMaxOrderLinesPerOrder;
    }

    static uint64_t district_slot(int32_t warehouse_id, int32_t district_id) {
        return warehouse_index(warehouse_id) * tpcc_layout().districts_per_warehouse +
               district_index(district_id);
    }

    static uint64_t last_name_index(const aria::FixedString<16>& last_name) {
        static constexpr std::array<std::string_view, 10> kLastNameTokens = {
            "BAR", "OUGHT", "ABLE", "PRI", "PRES",
            "ESE", "ANTI", "CALLY", "ATION", "EING"
        };

        std::string last_name_str = last_name.toString();
        while (!last_name_str.empty() && last_name_str.back() == ' ') {
            last_name_str.pop_back();
        }

        std::string_view remaining(last_name_str);
        uint64_t index = 0;

        for (int i = 0; i < 3; ++i) {
            bool matched = false;
            for (uint64_t digit = 0; digit < kLastNameTokens.size(); ++digit) {
                const auto& token = kLastNameTokens[digit];
                if (remaining.substr(0, token.size()) == token) {
                    index = index * 10 + digit;
                    remaining.remove_prefix(token.size());
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                std::size_t c_last_hash = std::hash<aria::FixedString<16>>{}(last_name);
                return c_last_hash % kLastNamesPerDistrict;
            }
        }

        if (!remaining.empty()) {
            std::size_t c_last_hash = std::hash<aria::FixedString<16>>{}(last_name);
            return c_last_hash % kLastNamesPerDistrict;
        }

        return index;
    }
};

} // namespace aria
