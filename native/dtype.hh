#pragma once

#include <cstdint>

enum class DataType : uint64_t {
    STRING = 0,
    LARGE_STRING = 1,

    TIMESTAMP = 2,

    // BOOL = 2,

    // FLOAT16 = 3,
    FLOAT32 = 3,
    FLOAT64 = 4,

    INT8 = 5,
    INT16 = 6,
    INT32 = 7,
    INT64 = 8,

    UINT8 = 9,
    UINT16 = 10,
    UINT32 = 11,
    UINT64 = 12,
};
