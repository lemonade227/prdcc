/**
 * @file L1Filter.h
 * @brief Coarse range-presence filter for PRDCC DataSummary.
 *
 * L1Filter partitions the configured key domain into fixed buckets and records
 * whether each bucket may contain data. It is used as the fast first stage of
 * conflict prediction: empty buckets allow PRDCC to reject a possible
 * intersection without consulting the higher-precision verifier.
 */
#pragma once

#include <vector>
#include <glog/logging.h>

namespace aria {

enum class L1Result {
    NO_CONFLICT,
    MAYBE_CONFLICT
};

class L1Filter {
public:
    static constexpr uint64_t kNumBlocks = 1 << 12;

    L1Filter()
        : presence_bitmap_(kNumBlocks, false),
          has_domain_(false),
          min_key_(0),
          max_key_(0),
          bucket_width_(1) {}

    void build(const std::vector<uint64_t>& keys, uint64_t min_key,
               uint64_t max_key) {
        LOG(INFO) << "Building L1Filter with " << keys.size() << " keys...";
        presence_bitmap_.assign(kNumBlocks, false);
        has_domain_ = min_key <= max_key;
        min_key_ = min_key;
        max_key_ = max_key;
        bucket_width_ = compute_bucket_width(min_key_, max_key_);

        for (const auto& key_val : keys) {
            uint64_t block_idx = bucket_index_for_key(key_val);
            if (!presence_bitmap_[block_idx]) {
                presence_bitmap_[block_idx] = true;
            }
        }
        LOG(INFO) << "L1Filter build complete.";
    }

    void build_dense(uint64_t min_key, uint64_t max_key) {
        LOG(INFO) << "Building dense L1Filter over domain [" << min_key
                  << ", " << max_key << "]...";
        presence_bitmap_.assign(kNumBlocks, false);
        has_domain_ = min_key <= max_key;
        min_key_ = min_key;
        max_key_ = max_key;
        bucket_width_ = compute_bucket_width(min_key_, max_key_);

        if (!has_domain_) {
            LOG(INFO) << "Dense L1Filter build complete.";
            return;
        }

        std::fill(presence_bitmap_.begin(), presence_bitmap_.end(), true);
        LOG(INFO) << "Dense L1Filter build complete.";
    }

    L1Result query(uint64_t start, uint64_t end) const {
        if (!has_domain_ || start > end) {
            return L1Result::NO_CONFLICT;
        }

        if (end < min_key_ || start > max_key_) {
            return L1Result::NO_CONFLICT;
        }

        const uint64_t clamped_start = std::max(start, min_key_);
        const uint64_t clamped_end = std::min(end, max_key_);

        if (clamped_start > clamped_end) {
            return L1Result::NO_CONFLICT;
        }

        uint64_t start_block = bucket_index_for_key(clamped_start);
        uint64_t end_block = bucket_index_for_key(clamped_end);

        for (uint64_t i = start_block; i <= end_block; ++i) {
            if (presence_bitmap_[i]) {
                return L1Result::MAYBE_CONFLICT;
            }
        }

        return L1Result::NO_CONFLICT;
    }

private:
    static uint64_t compute_bucket_width(uint64_t min_key, uint64_t max_key) {
        if (min_key > max_key) {
            return 1;
        }
        const uint64_t span = max_key - min_key;
        return span / kNumBlocks + 1;
    }

    uint64_t bucket_index_for_key(uint64_t key) const {
        DCHECK(has_domain_);

        if (key <= min_key_) {
            return 0;
        }
        if (key >= max_key_) {
            return kNumBlocks - 1;
        }

        const uint64_t offset = key - min_key_;
        uint64_t block_idx = offset / bucket_width_;
        if (block_idx >= kNumBlocks) {
            block_idx = kNumBlocks - 1;
        }
        return block_idx;
    }

    std::vector<bool> presence_bitmap_;
    bool has_domain_;
    uint64_t min_key_;
    uint64_t max_key_;
    uint64_t bucket_width_;
};

} // namespace aria
