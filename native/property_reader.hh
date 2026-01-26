// Declares property reader interfaces for the native layer.
//
// This header defines the abstract interfaces for reading property data
// and null maps plus factory helpers used by SubjectDatabase.
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
    // Reads property data for a subject into Python objects.
    virtual size_t get_property_data(int32_t subject_offset, int32_t length,
                                     PyObject** result,
                                     PyObject** to_deallocate) = 0;
    // Ensures derived readers clean up correctly.
    virtual ~PropertyReader() {};
};

// Builds a property reader for a given property name and type.
std::unique_ptr<PropertyReader> create_property_reader(
    const std::filesystem::path& root_directory,
    const std::string& property_name, DataType property_type);

struct NullMapReader {
    // Reads a null bitmap for a subject into the provided buffer.
    virtual void get_null_map(int32_t subject_offset, int32_t length,
                              uint64_t* result) = 0;
    // Ensures derived readers clean up correctly.
    virtual ~NullMapReader() {};
};

// Builds a null-map reader based on the number of properties.
std::unique_ptr<NullMapReader> create_null_map_reader(
    const std::filesystem::path& root_directory, int num_properties);

// Constructs a pyarrow dtype object for a given internal type.
PyObjectWrapper create_pyarrow_dtype(PyObject* pyarrow, DataType type);
