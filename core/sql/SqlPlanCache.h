#pragma once

#include "core/sql/SqlPlanner.h"

#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace aria::sql {

class SqlPlanCache {
public:
  static SqlPlanCache &instance() {
    static SqlPlanCache cache;
    return cache;
  }

  bool lookup(const std::string &key, std::size_t capacity_hint,
              SqlPlan &plan_out) {
    auto &shard = shard_for(key, capacity_hint);
    std::lock_guard<std::mutex> guard(shard.mutex);
    const auto it = shard.entries.find(key);
    if (it == shard.entries.end()) {
      return false;
    }
    shard.lru.splice(shard.lru.begin(), shard.lru, it->second);
    plan_out = it->second->second;
    return true;
  }

  void store(const std::string &key, const SqlPlan &plan,
             std::size_t capacity_hint) {
    auto &shard = shard_for(key, capacity_hint);
    std::lock_guard<std::mutex> guard(shard.mutex);
    const std::size_t capacity = normalized_capacity(capacity_hint);
    auto it = shard.entries.find(key);
    if (it != shard.entries.end()) {
      it->second->second = plan;
      shard.lru.splice(shard.lru.begin(), shard.lru, it->second);
      return;
    }

    shard.lru.emplace_front(key, plan);
    shard.entries[key] = shard.lru.begin();

    while (shard.entries.size() > capacity) {
      auto last = std::prev(shard.lru.end());
      shard.entries.erase(last->first);
      shard.lru.pop_back();
    }
  }

  void clear() {
    for (auto &shard : shards) {
      std::lock_guard<std::mutex> guard(shard.mutex);
      shard.entries.clear();
      shard.lru.clear();
    }
  }

private:
  struct Shard {
    std::mutex mutex;
    std::list<std::pair<std::string, SqlPlan>> lru;
    std::unordered_map<std::string,
                       std::list<std::pair<std::string, SqlPlan>>::iterator>
        entries;
  };

  SqlPlanCache() : shards(16) {}

  static std::size_t normalized_capacity(std::size_t capacity_hint) {
    return capacity_hint == 0 ? 256 : capacity_hint;
  }

  Shard &shard_for(const std::string &key, std::size_t capacity_hint) {
    const std::size_t shard_count =
        capacity_hint == 0 ? shards.size() : std::min(shards.size(), capacity_hint);
    return shards[std::hash<std::string>{}(key) % shard_count];
  }

  std::vector<Shard> shards;
};

} // namespace aria::sql
