// Defines the internal DataType enum used across the native layer.
//
// This header declares the compact on-disk type identifiers used to
// serialize per-property data and map them to Python/Arrow types.
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
