// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

namespace dingofs {
namespace connector {

// Thread-safe LRU cache for tracking which keys exist in BlockCache.
// Avoids repeated IsCached() calls to the underlying storage.
class ExistsCache {
 public:
  explicit ExistsCache(size_t capacity) : capacity_(capacity) {}

  bool Lookup(const std::string& key) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    if (it == map_.end()) return false;
    list_.splice(list_.begin(), list_, it->second);
    return true;
  }

  void Insert(const std::string& key) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      list_.splice(list_.begin(), list_, it->second);
      return;
    }
    if (map_.size() >= capacity_) {
      map_.erase(list_.back());
      list_.pop_back();
    }
    list_.push_front(key);
    map_[key] = list_.begin();
  }

  void Remove(const std::string& key) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      list_.erase(it->second);
      map_.erase(it);
    }
  }

 private:
  size_t capacity_;
  mutable std::mutex mu_;
  mutable std::list<std::string> list_;
  std::unordered_map<std::string, std::list<std::string>::iterator> map_;
};

}  // namespace connector
}  // namespace dingofs
