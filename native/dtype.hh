#pragma once

#include <cstdint>

enum class DataType : uint64_t {
    STRING = 0,
    TIMESTAMP = 1,
    FLOAT = 2,
    INT64 = 3,
};