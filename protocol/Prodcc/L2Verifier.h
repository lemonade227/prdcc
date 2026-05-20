/**
 * @file L2Verifier.h
 * @brief High-precision range verifier for sparse PRDCC key domains.
 *
 * L2Verifier stores locality-preserving hashed key positions and answers
 * existence queries for ranges that the L1 filter cannot rule out. 
 */
#pragma once

#include <vector>
#include <set>
#include <glog/logging.h>

namespace aria {

enum class L2Result {
    NO_CONFLICT,
    CONFLICT
};

class L2Verifier {
public:
    static constexpr uint64_t R = 1 << 24;
    static constexpr uint64_t P = (1ULL << 61) - 1;

    L2Verifier() = default;
    
    void build(const std::vector<uint64_t>& keys) {
        LOG(INFO) << "Building L2Verifier with " << keys.size() << " keys...";
        hashed_summary_tree_.clear(); // Reset tree
        for (const auto& key_val : keys) {
            hashed_summary_tree_.insert(hash_func(key_val));
        }
        LOG(INFO) << "L2Verifier build complete.";
    }

    L2Result query(uint64_t start, uint64_t end) const {
        uint64_t h_start = hash_func(start);
        uint64_t h_end = hash_func(end);

        if (h_start <= h_end) {
            auto it = hashed_summary_tree_.lower_bound(h_start);
            if (it != hashed_summary_tree_.end() && *it <= h_end) {
                return L2Result::CONFLICT;
            }
        } 
        else {
            auto it = hashed_summary_tree_.lower_bound(0);
            if (it != hashed_summary_tree_.end() && *it <= h_end) {
                return L2Result::CONFLICT;
            }
            it = hashed_summary_tree_.lower_bound(h_start);
            if (it != hashed_summary_tree_.end()) {
                return L2Result::CONFLICT;
            }
        }

        return L2Result::NO_CONFLICT;
    }

private:
    uint64_t q_func(uint64_t y) const {
        // Pairwise independent hash function
        return ((alpha_ * y + beta_) % P) % R;
    }

    uint64_t hash_func(uint64_t x) const {
        // Locality-preserving double hashing
        return (q_func(x / R) + x) % R;
    }

private:
    std::set<uint64_t> hashed_summary_tree_;
    uint64_t alpha_ = 1;
    uint64_t beta_ = 0;
};

} // namespace aria
