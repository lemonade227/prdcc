/**
 * @file DataSummary.h
 * @brief Two-level PRDCC database-key summary.
 *
 * DataSummary is the common query interface used by the conflict predictor.
 * It combines a coarse L1 range filter with an optional L2 verifier so that
 * predicted range intersections can be ruled out quickly while preserving the
 * conservative no-false-negative property required for supported extractable
 * point/range predicates.
 */
#pragma once

#include "protocol/Prodcc/L1Filter.h"
#include "protocol/Prodcc/L2Verifier.h"

namespace aria {

class DataSummary {
public:
    DataSummary() : dense_domain_(false) {}

    void build(const std::vector<uint64_t>& keys, uint64_t min_key,
               uint64_t max_key) {
        dense_domain_ = false;
        l1_filter_.build(keys, min_key, max_key);
        l2_verifier_.build(keys);
    }

    void build_dense(uint64_t min_key, uint64_t max_key) {
        dense_domain_ = true;
        l1_filter_.build_dense(min_key, max_key);
    }

    bool query_range_exists(uint64_t start, uint64_t end) const {
        const auto l1_result = l1_filter_.query(start, end);
        if (l1_result == L1Result::NO_CONFLICT) {
            return false;
        }
        if (dense_domain_) {
            return true;
        }
        return l2_verifier_.query(start, end) == L2Result::CONFLICT;
    }

private:
    L1Filter l1_filter_;
    L2Verifier l2_verifier_;
    bool dense_domain_;
};

} // namespace aria
