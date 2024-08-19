#include "property_reader.hh"

#define PY_SSIZE_T_CLEAN
#define ZSTD_STATIC_LINKING_ONLY
#include <Python.h>
#include <datetime.h>
#include <fcntl.h>
#include <structmember.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <bitset>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <vector>

#include "absl/time/civil_time.h"
#include "absl/types/span.h"
#include "dtype.hh"
#include "mmap_file.hh"
#include "pyutils.hh"
#include "streamvbyte.h"
#include "zstd.h"

namespace {

bool datetime_initialized = false;

auto context_deleter = [](ZSTD_DCtx* context) { ZSTD_freeDCtx(context); };

class ZstdRowReader {
   public:
    ZstdRowReader(const std::string& path, ZSTD_DCtx* ctx)
        : fname(path),
          fstream(path, std::ifstream::in | std::ifstream::binary),
          context(ctx),
          current_offset(0),
          uncompressed_size(0) {}

    std::optional<std::tuple<std::string_view, int64_t>> get_next() {
        if (current_offset == uncompressed_size) {
            bool could_load_more = try_to_load_more_data();

            if (!could_load_more) {
                return {};
            }

            assert(current_offset < uncompressed_size);
        }

        assert(compressed_buffer.size() >= sizeof(size_t));

        int64_t count = *reinterpret_cast<const int64_t*>(
            uncompressed_buffer.data() + current_offset);
        current_offset += sizeof(count);

        size_t size = *reinterpret_cast<const size_t*>(
            uncompressed_buffer.data() + current_offset);
        current_offset += sizeof(size);

        std::string_view data(uncompressed_buffer.data() + current_offset,
                              size);
        current_offset += size;

        assert(data.size() >= 0);
        assert(data.data() != nullptr);

        return std::make_tuple(data, count);
    }

   private:
    bool try_to_load_more_data() {
        if (fstream.eof()) {
            return false;
        }

        size_t size;
        fstream.read(reinterpret_cast<char*>(&size), sizeof(size));

        if (fstream.eof()) {
            return false;
        }

        if (compressed_buffer.size() < size) {
            compressed_buffer.resize(size * 2);
        }

        fstream.read(compressed_buffer.data(), size);

        uncompressed_size =
            ZSTD_getFrameContentSize(compressed_buffer.data(), size);

        if (uncompressed_size == ZSTD_CONTENTSIZE_ERROR ||
            uncompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
            throw std::runtime_error(
                "Could not get the size of the zstd compressed stream?");
        }

        if (uncompressed_buffer.size() < uncompressed_size) {
            uncompressed_buffer.resize(uncompressed_size * 2);
        }

        size_t read_size = ZSTD_decompressDCtx(
            context, uncompressed_buffer.data(), uncompressed_size,
            compressed_buffer.data(), size);

        if (ZSTD_isError(read_size) || read_size != uncompressed_size) {
            throw std::runtime_error("Could not decompress zstd data?");
        }

        current_offset = 0;
        return true;
    }

    const std::string fname;
    std::ifstream fstream;

    ZSTD_DCtx* context;

    std::vector<char> compressed_buffer;
    std::vector<char> uncompressed_buffer;
    size_t current_offset;
    size_t uncompressed_size;
};

struct StringPropertyReader : PropertyReader {
    StringPropertyReader(const std::filesystem::path& property_path)
        : zdict_file(property_path / "zdict"),
          data_file(property_path / "data"),
          context(ZSTD_createDCtx(), context_deleter) {
        {
            ZstdRowReader reader(property_path / "dictionary", context.get());

            while (true) {
                auto next = reader.get_next();
                if (!next) {
                    break;
                }

                auto val = std::get<0>(*next);

                PyObject* value_string =
                    PyUnicode_FromStringAndSize(val.data(), val.size());
                PyUnicode_InternInPlace(&value_string);

                dictionary.emplace_back(value_string);
            }
            dictionary_size = dictionary.size();
        }

        if (zdict_file.bytes().size() != 0) {
            size_t ret = ZSTD_DCtx_loadDictionary_byReference(
                context.get(), (void*)zdict_file.bytes().data(),
                zdict_file.bytes().size());
            if (ZSTD_isError(ret)) {
                throw std::runtime_error("Unable to load dictionary");
            }
        }
    }

    MmapFile zdict_file;
    MmapFile data_file;

    std::unique_ptr<ZSTD_DCtx, decltype(context_deleter)> context;

    std::vector<PyObjectWrapper> dictionary;
    size_t dictionary_size;

    std::vector<char> decompressed;
    std::vector<uint32_t> values;

    size_t get_property_data(int32_t subject_offset, int32_t length,
                             PyObject** result, PyObject** allocated) {
        size_t num_allocated = 0;

        uint64_t offset = data_file.data<uint64_t>()[subject_offset];
        uint64_t num_bytes =
            data_file.data<uint64_t>()[subject_offset + 1] - offset;

        uint32_t* length_pointer =
            (uint32_t*)(data_file.bytes().data() + offset);
        uint32_t decompressed_size = *length_pointer;
        if (decompressed.size() < (decompressed_size + STREAMVBYTE_PADDING)) {
            decompressed.resize(2 * (decompressed_size + STREAMVBYTE_PADDING));
        }

        size_t ret = ZSTD_decompressDCtx(
            context.get(), decompressed.data(), decompressed.size(),
            data_file.bytes().data() + offset + sizeof(uint32_t),
            num_bytes - sizeof(uint32_t));

        if (ZSTD_isError(ret)) {
            throw std::runtime_error("Unable to decompress");
        }

        if (ret != decompressed_size) {
            throw std::runtime_error("Decompressed the wrong amount of data " +
                                     std::to_string(ret) + " " +
                                     std::to_string(decompressed_size));
        }

        auto num_null_bytes =
            (length + sizeof(uint64_t) * 8 - 1) / (sizeof(uint64_t) * 8);
        absl::Span<const uint64_t> null_bytes{
            (const uint64_t*)decompressed.data(), num_null_bytes};

        length_pointer = (uint32_t*)(decompressed.data() +
                                     num_null_bytes * sizeof(uint64_t));

        size_t value_size = *length_pointer;

        if (values.size() < value_size) {
            values.resize(value_size * 2);
        }

        size_t num_read = streamvbyte_decode(
            (const uint8_t*)decompressed.data() + sizeof(uint32_t) +
                num_null_bytes * sizeof(uint64_t),
            values.data(), value_size);

        const char* start_of_per_subject = decompressed.data() + num_read +
                                           sizeof(uint32_t) +
                                           num_null_bytes * sizeof(uint64_t);

        uint32_t num_per_subject_values = values[0];

        if (dictionary.size() < dictionary_size + num_per_subject_values) {
            dictionary.resize(dictionary_size + num_per_subject_values * 2);
        }

        size_t value_index = 1;

        for (uint32_t i = 0; i < num_per_subject_values; i++) {
            std::string_view view(start_of_per_subject, values[value_index]);
            PyObject* value_string =
                PyUnicode_FromStringAndSize(view.data(), view.size());
            if (value_string == nullptr) {
                throw std::runtime_error("Should never happen");
            }

            allocated[num_allocated++] = value_string;

            dictionary[dictionary_size + i] = value_string;
            start_of_per_subject += values[value_index++];
        }

        if (start_of_per_subject - decompressed.data() > decompressed_size) {
            throw std::runtime_error(
                "Decoded too much? " +
                std::to_string(start_of_per_subject - decompressed.data()) +
                " " + std::to_string(decompressed_size));
        }

        size_t result_index = 0;

        for (uint64_t null_byte : null_bytes) {
            size_t current_result = result_index;
            while (null_byte != 0) {
                static_assert(sizeof(uint64_t) == sizeof(unsigned long));
                int num_zeros = __builtin_ctzl(null_byte);
                current_result += num_zeros;

                null_byte >>= 1;
                null_byte >>= num_zeros;

                result[current_result++] =
                    dictionary[values[value_index++]].borrow();
            }

            result_index += sizeof(null_byte) * 8;
        }

        for (size_t i = dictionary_size; i < dictionary.size(); i++) {
            dictionary[i].steal();
        }

        if (value_index > value_size) {
            throw std::runtime_error("Out of bounds error for values " +
                                     std::to_string(value_index) + " " +
                                     std::to_string(value_size) + " " +
                                     std::to_string(num_per_subject_values));
        }

        return num_allocated;
    }
};

struct TimePropertyReader : PropertyReader {
    TimePropertyReader(const std::filesystem::path& property_path)
        : zdict_file(property_path / "zdict"),
          data_file(property_path / "data"),
          context(ZSTD_createDCtx(), context_deleter) {
        if (zdict_file.bytes().size() != 0) {
            size_t ret = ZSTD_DCtx_loadDictionary_byReference(
                context.get(), (void*)zdict_file.bytes().data(),
                zdict_file.bytes().size());
            if (ZSTD_isError(ret)) {
                throw std::runtime_error("Unable to load dictionary");
            }
        }
    }

    MmapFile zdict_file;
    MmapFile data_file;

    std::unique_ptr<ZSTD_DCtx, decltype(context_deleter)> context;

    std::vector<char> decompressed;
    std::vector<uint32_t> values;

    size_t get_property_data(int32_t subject_offset, int32_t length,
                             PyObject** result, PyObject** allocated) {
        size_t num_allocated = 0;
        uint64_t offset = data_file.data<uint64_t>()[subject_offset];
        uint64_t num_bytes =
            data_file.data<uint64_t>()[subject_offset + 1] - offset;

        uint32_t* length_pointer =
            (uint32_t*)(data_file.bytes().data() + offset);
        uint32_t decompressed_size = *length_pointer;
        if (decompressed.size() < (decompressed_size + STREAMVBYTE_PADDING)) {
            decompressed.resize(2 * (decompressed_size + STREAMVBYTE_PADDING));
        }

        size_t ret = ZSTD_decompressDCtx(
            context.get(), decompressed.data(), decompressed.size(),
            data_file.bytes().data() + offset + sizeof(uint32_t),
            num_bytes - sizeof(uint32_t));

        if (ZSTD_isError(ret)) {
            throw std::runtime_error("Unable to decompress");
        }

        if (ret != decompressed_size) {
            throw std::runtime_error("Decompressed the wrong amount of data");
        }

        int64_t* start_timestamp = (int64_t*)(decompressed.data());

        length_pointer = (uint32_t*)(decompressed.data() + sizeof(int64_t));

        size_t num_values = *length_pointer;
        if (values.size() < num_values) {
            values.resize(num_values * 2);
        }

        size_t num_read =
            streamvbyte_decode_0124((const uint8_t*)decompressed.data() +
                                        sizeof(uint32_t) + sizeof(int64_t),
                                    values.data(), num_values);

        if (num_read + sizeof(uint32_t) + sizeof(int64_t) !=
            decompressed_size) {
            throw std::runtime_error("Decoded too much? " +
                                     std::to_string(num_read) + " " +
                                     std::to_string(num_bytes));
        }

        int64_t seconds_per_day = (int64_t)(24 * 60 * 60);
        int64_t microseconds_per_second = (int64_t)(1000 * 1000);

        int64_t start_seconds = *start_timestamp / microseconds_per_second;
        int64_t start_micros = *start_timestamp % microseconds_per_second;

        if (start_micros < 0) {
            start_micros += microseconds_per_second;
            start_seconds -= 1;
        }

        absl::CivilSecond day(1970, 1, 1);
        day += start_seconds;

        int64_t microseconds = start_micros;

        uint32_t num_null = values[0];
        size_t result_index = num_null;

        auto add_times = [&](uint32_t copies) {
            PyObject* dt{PyDateTime_FromDateAndTime(
                day.year(), day.month(), day.day(), day.hour(), day.minute(),
                day.second(), microseconds)};
            if (dt == nullptr) {
                PyErr_Print();
                throw std::runtime_error("Should never happen");
            }
            for (uint32_t i = 1; i < copies; i++) {
                result[result_index++] = dt;
            }
            result[result_index++] = dt;

            allocated[num_allocated++] = dt;
        };

        size_t value_index = 1;
        while (result_index < (size_t)length) {
            uint32_t days_val = values[value_index++];
            uint32_t days_delta = days_val >> 4;
            uint32_t days_count = days_val % (1 << 4);

            day += seconds_per_day * days_delta;

            if (days_count != 0) {
                add_times(days_count);
                continue;
            }

            uint32_t seconds_val = values[value_index++];
            uint32_t seconds_delta = seconds_val >> 4;
            uint32_t seconds_count = seconds_val % (1 << 4);

            day += seconds_delta;

            if (seconds_count != 0) {
                add_times(seconds_count);
                continue;
            }

            uint32_t microseconds_val = values[value_index++];
            uint32_t microseconds_delta = microseconds_val >> 4;
            uint32_t microseconds_count = microseconds_val % (1 << 4);

            microseconds += microseconds_delta;
            if (microseconds >= microseconds_per_second) {
                microseconds -= microseconds_per_second;
                day += 1;
            }

            if (microseconds_count != 0) {
                add_times(microseconds_count);
            } else {
                throw std::runtime_error("Date error thing?");
            }
        }

        if (value_index > values.size()) {
            throw std::runtime_error("Out of bounds error for values");
        }

        return num_allocated;
    }
};

template <typename T, typename G>
struct PrimitivePropertyReader : PropertyReader {
    PrimitivePropertyReader(const std::filesystem::path& property_path,
                            G transform)
        : transform_func(transform),
          zdict_file(property_path / "zdict"),
          data_file(property_path / "data"),
          context(ZSTD_createDCtx(), context_deleter) {
        if (zdict_file.bytes().size() != 0) {
            size_t ret = ZSTD_DCtx_loadDictionary_byReference(
                context.get(), (void*)zdict_file.bytes().data(),
                zdict_file.bytes().size());
            if (ZSTD_isError(ret)) {
                throw std::runtime_error("Unable to load dictionary");
            }
        }
    }

    G transform_func;

    MmapFile zdict_file;
    MmapFile data_file;

    std::unique_ptr<ZSTD_DCtx, decltype(context_deleter)> context;

    std::vector<char> decompressed;

    size_t get_property_data(int32_t subject_offset, int32_t length,
                             PyObject** result, PyObject** allocated) {
        size_t num_allocated = 0;

        uint64_t offset = data_file.data<uint64_t>()[subject_offset];
        uint64_t num_bytes =
            data_file.data<uint64_t>()[subject_offset + 1] - offset;

        uint32_t* length_pointer =
            (uint32_t*)(data_file.bytes().data() + offset);
        uint32_t decompressed_size = *length_pointer;
        if (decompressed.size() < decompressed_size) {
            decompressed.resize(2 * decompressed_size);
        }

        size_t ret = ZSTD_decompressDCtx(
            context.get(), decompressed.data(), decompressed.size(),
            data_file.bytes().data() + offset + sizeof(uint32_t),
            num_bytes - sizeof(uint32_t));

        if (ZSTD_isError(ret)) {
            throw std::runtime_error("Unable to decompress");
        }

        if (ret != decompressed_size) {
            throw std::runtime_error("Decompressed the wrong amount of data");
        }

        auto num_null_bytes =
            (length + sizeof(uint64_t) * 8 - 1) / (sizeof(uint64_t) * 8);
        absl::Span<const uint64_t> null_bytes{
            (const uint64_t*)decompressed.data(), num_null_bytes};

        const T* data =
            (const T*)(decompressed.data() + num_null_bytes * sizeof(uint64_t));

        size_t result_index = 0;

        for (uint64_t null_byte : null_bytes) {
            size_t current_result = result_index;
            while (null_byte != 0) {
                static_assert(sizeof(uint64_t) == sizeof(unsigned long));
                int num_zeros = __builtin_ctzl(null_byte);
                current_result += num_zeros;

                null_byte >>= 1;
                null_byte >>= num_zeros;

                PyObject* obj = transform_func(*data++);

                result[current_result++] = obj;
                allocated[num_allocated++] = obj;
            }

            result_index += sizeof(null_byte) * 8;
        }

        if ((const char*)data > decompressed.data() + decompressed.size()) {
            throw std::runtime_error("Read too much in the primitive reader?");
        }

        if (result_index % 32 > (size_t)length % 32) {
            throw std::runtime_error("Had more results than the length?");
        }

        return num_allocated;
    }
};

template <typename T, typename G>
std::unique_ptr<PropertyReader> make_primitive_reader(
    const std::filesystem::path& property_path, G transform) {
    return std::make_unique<PrimitivePropertyReader<T, G>>(property_path,
                                                           transform);
}

PyObject* create_datetime(int64_t offset) {
    absl::CivilSecond day(1970, 1, 1);

    int64_t microseconds_per_second = (int64_t)(1000 * 1000);

    day += offset / microseconds_per_second;
    int64_t microseconds = offset % microseconds_per_second;

    PyObjectWrapper dt{PyDateTime_FromDateAndTime(
        day.year(), day.month(), day.day(), day.hour(), day.minute(),
        day.second(), microseconds)};
    if (dt.borrow() == nullptr) {
        PyErr_Print();
        throw std::runtime_error("Could not make the date?");
    }
    return dt.steal();
}

std::pair<const char*, const char*> get_pyarrow_arguments(DataType type) {
    switch (type) {
        case DataType::STRING:
            return {"string", nullptr};

        case DataType::LARGE_STRING:
            return {"large_string", nullptr};

        case DataType::TIMESTAMP:
            return {"timestamp", "us"};

        case DataType::FLOAT32:
            return {"float32", nullptr};
        case DataType::FLOAT64:
            return {"float64", nullptr};

        case DataType::INT8:
            return {"int8", nullptr};
        case DataType::INT16:
            return {"int16", nullptr};
        case DataType::INT32:
            return {"int32", nullptr};
        case DataType::INT64:
            return {"int64", nullptr};

        case DataType::UINT8:
            return {"uint8", nullptr};
        case DataType::UINT16:
            return {"uint16", nullptr};
        case DataType::UINT32:
            return {"uint32", nullptr};
        case DataType::UINT64:
            return {"uint64", nullptr};
    }

    throw std::runtime_error("Unsupported property type " +
                             std::to_string(static_cast<uint64_t>(type)));
}

template <typename T>
struct NullMapReaderImpl : NullMapReader {
    NullMapReaderImpl(const std::filesystem::path& root_directory)
        : zdict_file(root_directory / "meds_reader.null_map" / "zdict"),
          data_file(root_directory / "meds_reader.null_map" / "data"),
          context(ZSTD_createDCtx(), context_deleter) {
        if (zdict_file.bytes().size() != 0) {
            size_t ret = ZSTD_DCtx_loadDictionary_byReference(
                context.get(), (void*)zdict_file.bytes().data(),
                zdict_file.bytes().size());
            if (ZSTD_isError(ret)) {
                throw std::runtime_error("Unable to load dictionary");
            }
        }
    }

    MmapFile zdict_file;
    MmapFile data_file;

    std::unique_ptr<ZSTD_DCtx, decltype(context_deleter)> context;

    std::vector<char> decompressed;

    void get_null_map(int32_t subject_offset, int32_t length,
                      uint64_t* result) {
        uint64_t offset = data_file.data<uint64_t>()[subject_offset];
        uint64_t num_bytes =
            data_file.data<uint64_t>()[subject_offset + 1] - offset;

        uint32_t* length_pointer =
            (uint32_t*)(data_file.bytes().data() + offset);
        uint32_t decompressed_size = *length_pointer;
        if (decompressed.size() < decompressed_size) {
            decompressed.resize(2 * decompressed_size);
        }

        size_t ret = ZSTD_decompressDCtx(
            context.get(), decompressed.data(), decompressed.size(),
            data_file.bytes().data() + offset + sizeof(uint32_t),
            num_bytes - sizeof(uint32_t));

        if (ZSTD_isError(ret)) {
            throw std::runtime_error("Unable to decompress " +
                                     std::string(ZSTD_getErrorName(ret)));
        }

        if (ret != decompressed_size) {
            throw std::runtime_error("Decompressed the wrong amount of data");
        }

        const T* data = (const T*)decompressed.data();

        for (int32_t i = 0; i < length; i++) {
            result[i] = data[i];
        }
    }
};

}  // namespace

std::unique_ptr<PropertyReader> create_property_reader(
    const std::filesystem::path& root_directory,
    const std::string& property_name, DataType property_type) {
    std::filesystem::path property_path = root_directory / property_name;

    switch (property_type) {
        case DataType::STRING:
        case DataType::LARGE_STRING:
            return std::make_unique<StringPropertyReader>(property_path);

        case DataType::TIMESTAMP:
            if (!datetime_initialized) {
                PyDateTime_IMPORT;
                datetime_initialized = true;
            }
            if (property_name == "time") {
                return std::make_unique<TimePropertyReader>(property_path);
            } else {
                return make_primitive_reader<int64_t>(property_path,
                                                      create_datetime);
            }

        case DataType::FLOAT32:
            return make_primitive_reader<float>(property_path,
                                                PyFloat_FromDouble);
        case DataType::FLOAT64:
            return make_primitive_reader<double>(property_path,
                                                 PyFloat_FromDouble);

            static_assert(sizeof(int64_t) == sizeof(long));

        case DataType::INT8:
            return make_primitive_reader<int8_t>(property_path,
                                                 PyLong_FromLong);
        case DataType::INT16:
            return make_primitive_reader<int16_t>(property_path,
                                                  PyLong_FromLong);
        case DataType::INT32:
            return make_primitive_reader<int32_t>(property_path,
                                                  PyLong_FromLong);
        case DataType::INT64:
            return make_primitive_reader<int64_t>(property_path,
                                                  PyLong_FromLong);

            static_assert(sizeof(uint64_t) == sizeof(unsigned long));
        case DataType::UINT8:
            return make_primitive_reader<int8_t>(property_path,
                                                 PyLong_FromUnsignedLong);
        case DataType::UINT16:
            return make_primitive_reader<int16_t>(property_path,
                                                  PyLong_FromUnsignedLong);
        case DataType::UINT32:
            return make_primitive_reader<int32_t>(property_path,
                                                  PyLong_FromUnsignedLong);
        case DataType::UINT64:
            return make_primitive_reader<int64_t>(property_path,
                                                  PyLong_FromUnsignedLong);
    }

    throw std::runtime_error(
        "Unsupported property " + property_name + " " +
        std::to_string(static_cast<uint64_t>(property_type)));
}

std::unique_ptr<NullMapReader> create_null_map_reader(
    const std::filesystem::path& root_directory, int num_properties) {
    if (num_properties > 64) {
        throw std::runtime_error("Too many properties");
    } else if (num_properties > 32) {
        return std::make_unique<NullMapReaderImpl<uint64_t>>(root_directory);
    } else if (num_properties > 16) {
        return std::make_unique<NullMapReaderImpl<uint32_t>>(root_directory);
    } else if (num_properties > 8) {
        return std::make_unique<NullMapReaderImpl<uint16_t>>(root_directory);
    } else {
        return std::make_unique<NullMapReaderImpl<uint8_t>>(root_directory);
    }
}

PyObjectWrapper create_pyarrow_dtype(PyObject* pyarrow, DataType type) {
    auto name_and_arg = get_pyarrow_arguments(type);

    PyObjectWrapper py_name{PyUnicode_FromString(name_and_arg.first)};
    if (py_name.borrow() == nullptr) {
        throw std::runtime_error("Could not create string for arrow type?");
    }

    PyObjectWrapper result;

    if (name_and_arg.second == nullptr) {
        result = PyObject_CallMethodNoArgs(pyarrow, py_name.borrow());
    } else {
        PyObjectWrapper py_arg{PyUnicode_FromString(name_and_arg.second)};
        if (py_arg.borrow() == nullptr) {
            throw std::runtime_error(
                "Could not create string for arrow type argument?");
        }

        result = PyObject_CallMethodOneArg(pyarrow, py_name.borrow(),
                                           py_arg.borrow());
    }

    if (result.borrow() == nullptr) {
        PyErr_Print();
        throw std::runtime_error("Could not get attribute for pyarrow?");
    }

    return result;
}
