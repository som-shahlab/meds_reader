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
    virtual std::vector<PyObjectWrapper> get_property_data(
        int32_t patient_offset, int32_t length) = 0;
    virtual ~PropertyReader(){};
};

std::unique_ptr<PropertyReader> create_property_reader(
    const std::filesystem::path& root_directory,
    const std::string& property_name, DataType property_type);

struct NullMapReader {
    virtual std::vector<uint64_t> get_null_map(int32_t patient_offset,
                                               int32_t length) = 0;
    virtual ~NullMapReader(){};
};

std::unique_ptr<NullMapReader> create_null_map_reader(
    const std::filesystem::path& root_directory, int num_properties);

PyObjectWrapper create_pyarrow_dtype(PyObject* pyarrow, DataType type);
