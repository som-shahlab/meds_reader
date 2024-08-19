#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "dtype.hh"
#include "pyutils.hh"

struct PropertyReader {
    virtual size_t get_property_data(int32_t subject_offset, int32_t length,
                                     PyObject** result,
                                     PyObject** to_deallocate) = 0;
    virtual ~PropertyReader() {};
};

std::unique_ptr<PropertyReader> create_property_reader(
    const std::filesystem::path& root_directory,
    const std::string& property_name, DataType property_type);

struct NullMapReader {
    virtual void get_null_map(int32_t subject_offset, int32_t length,
                              uint64_t* result) = 0;
    virtual ~NullMapReader() {};
};

std::unique_ptr<NullMapReader> create_null_map_reader(
    const std::filesystem::path& root_directory, int num_properties);

PyObjectWrapper create_pyarrow_dtype(PyObject* pyarrow, DataType type);
