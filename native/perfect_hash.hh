#pragma once

#include <cstdint>
#include <set>
#include <stdexcept>
#include <vector>

namespace {

inline uint64_t next_pow2(uint64_t x) {
    return x == 1 ? 1 : 1 << (64 - __builtin_clzl(x - 1));
}

}  // namespace

template <typename T>
class PerfectHashMap {
   public:
    PerfectHashMap(std::vector<T*> items) {
        hash_map_size = next_pow2(items.size() * items.size());
        hash_map_size_mask = hash_map_size - 1;

        std::set<uintptr_t> values;
        for (T* item : items) {
            if (item == nullptr) {
                throw std::runtime_error("Cannot contain null pointers");
            }
            uintptr_t value = reinterpret_cast<uintptr_t>(item);
            values.insert(value % modulus);
        }

        if (values.size() != items.size()) {
            throw std::runtime_error(
                "Very serious bug in perfect hash function, bad input?");
        }

        int num_tries = 0;

        while (true) {
            multiplier = rand() % modulus;
            hash_map.clear();
            hash_map.resize(hash_map_size);

            bool is_bad = false;

            for (size_t i = 0; i < items.size(); i++) {
                T* item = items[i];

                size_t index = apply_hash(item);
                if (hash_map[index].first != nullptr) {
                    // Found a duplicate
                    is_bad = true;
                    break;
                } else {
                    hash_map[index].first = item;
                    hash_map[index].second = i;
                }
            }

            if (!is_bad) {
                break;
            }

            if (num_tries > 1000) {
                throw std::runtime_error(
                    "Serious bug in perfect hash map, tried 1000 times");
            }

            num_tries++;
        }
    }

    ssize_t get_index(T* item) const {
        size_t index = apply_hash(item);
        auto entry = hash_map[index];
        if (entry.first == item) [[likely]] {
            return entry.second;
        } else {
            return -1;
        }
    }

    std::vector<T*> get_values() const {
        std::vector<T*> result;
        for (const auto& entry : hash_map) {
            if (entry.first != nullptr) {
                result.push_back(entry.first);
            }
        }
        return result;
    }

   private:
    static constexpr size_t modulus = (((size_t)(1)) << 31) - 1;
    size_t hash_map_size;
    size_t hash_map_size_mask;

    size_t apply_hash(T* item) const {
        uintptr_t value = reinterpret_cast<uintptr_t>(item);
        return ((value * multiplier) % modulus) & hash_map_size_mask;
    }

    std::vector<std::pair<T*, size_t>> hash_map;
    size_t multiplier;
    size_t size_mask;
};
