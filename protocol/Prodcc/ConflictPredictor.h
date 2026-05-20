/**
 * @file ConflictPredictor.h
 * @brief Deterministic PRDCC conflict predictor.
 *
 * This module converts transactions with extractable point/range predicates
 * into conservative read/write key ranges and checks pairwise range
 * intersections through DataSummary. It reports predicted WAW, RAW, and WAR
 * dependencies before physical transaction execution, providing the dependency
 * graph input used by the PRDCC scheduler.
 */
#pragma once

#include "protocol/Prodcc/DataSummary.h"
#include "protocol/Prodcc/ProdccTransaction.h"
#include <algorithm>
#include <vector>
#include <utility>

namespace aria {

struct TxnQueryRanges {
    std::vector<std::pair<uint64_t, uint64_t>> read;
    std::vector<std::pair<uint64_t, uint64_t>> write;
};

class ConflictPredictor {
public:
    ConflictPredictor() = default;

    void build_summary(const std::vector<uint64_t>& keys, uint64_t min_key,
                       uint64_t max_key, bool dense_domain) {
        if (dense_domain) {
            data_summary_.build_dense(min_key, max_key);
            return;
        }
        data_summary_.build(keys, min_key, max_key);
    }

    void fill_query_ranges(const ProdccTransaction& txn, TxnQueryRanges& out) const {
        out.read.clear();
        out.write.clear();
        txn.get_query_ranges(out.read, out.write);
    }

    ConflictType predict_conflict(const ProdccTransaction& t1, const ProdccTransaction& t2) const {
        DCHECK(t1.get_id() < t2.get_id());

        thread_local TxnQueryRanges t1_ranges;
        thread_local TxnQueryRanges t2_ranges;

        fill_query_ranges(t1, t1_ranges);
        fill_query_ranges(t2, t2_ranges);

        return predict_conflict(t1_ranges, t2_ranges);
    }

    ConflictType predict_conflict(const TxnQueryRanges& t1, const TxnQueryRanges& t2) const {
        if (check_range_intersection(t1.write, t2.write)) {
            return ConflictType::WAW;
        }

        if (check_range_intersection(t1.write, t2.read)) {
            return ConflictType::RAW;
        }

        if (check_range_intersection(t1.read, t2.write)) {
            return ConflictType::WAR;
        }

        return ConflictType::NONE;
    }

private:
    bool check_range_intersection(const std::vector<std::pair<uint64_t, uint64_t>>& ranges1,
                                  const std::vector<std::pair<uint64_t, uint64_t>>& ranges2) const {
        if (ranges1.empty() || ranges2.empty()) {
            return false;
        }

        auto it1 = ranges1.begin();
        auto it2 = ranges2.begin();

        while (it1 != ranges1.end() && it2 != ranges2.end()) {
            uint64_t intersect_start = std::max(it1->first, it2->first);
            uint64_t intersect_end = std::min(it1->second, it2->second);

            if (intersect_start <= intersect_end) {
                if (data_summary_.query_range_exists(intersect_start, intersect_end)) {
                    return true;
                }
            }
            
            if (it1->second < it2->second) {
                ++it1;
            } else {
                ++it2;
            }
        }

        return false;
    }

private:
    DataSummary data_summary_;
};

} // namespace aria
