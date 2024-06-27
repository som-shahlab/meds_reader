#pragma once

#include <cstdint>

enum class DataType : uint64_t {
    STRING = 0,
    TIMESTAMP = 1,

    // BOOL = 2,

    // FLOAT16 = 3,
    FLOAT32 = 4,
    FLOAT64 = 5,

    INT8 = 6,
    INT16 = 7,
    INT32 = 8,
    INT64 = 9,

    UINT8 = 10,
    UINT16 = 11,
    UINT32 = 12,
    UINT64 = 13,
};