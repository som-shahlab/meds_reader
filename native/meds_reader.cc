#define PY_SSIZE_T_CLEAN
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
#include <set>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/time/civil_time.h"
#include "absl/types/span.h"
#include "dtype.hh"

#define ZSTD_STATIC_LINKING_ONLY
#include "streamvbyte.h"
#include "zstd.h"

namespace {

template <typename F>
PyObject* wrap(F func) {
    try {
        return func();
    } catch (std::exception& e) {
        return PyErr_Format(PyExc_RuntimeError, "%s", e.what());
    }

    throw std::runtime_error("Unknown error?");
}

struct PyObjectWrapper {
    PyObjectWrapper() : ref(nullptr) {}
    explicit PyObjectWrapper(PyObject* obj) : ref(obj) {}

    PyObjectWrapper(const PyObjectWrapper& other) = delete;

    PyObjectWrapper(PyObjectWrapper&& other) {
        ref = other.ref;
        other.ref = nullptr;
    }

    PyObjectWrapper& operator=(PyObject* obj) {
        Py_XDECREF(ref);
        ref = obj;
        return *this;
    }

    PyObject* borrow() { return ref; }

    PyObject* copy() {
        Py_INCREF(ref);
        return ref;
    }

    PyObject* steal() {
        PyObject* result = ref;
        ref = nullptr;
        return result;
    }

    ~PyObjectWrapper() { Py_XDECREF(ref); }

    PyObject* ref;
};

class MmapFile {
   public:
    MmapFile(std::filesystem::path path) {
        fp = open(path.c_str(), O_RDONLY);
        data_size = std::filesystem::file_size(path);
        data_pointer = mmap(nullptr, data_size, PROT_READ, MAP_PRIVATE, fp, 0);
    }

    std::string_view bytes() const {
        return std::string_view((const char*)data_pointer, data_size);
    }

    template <typename T>
    absl::Span<const T> data() const {
        return absl::Span<const T>((const T*)data_pointer,
                                   data_size / sizeof(T));
    }

    ~MmapFile() {
        munmap(data_pointer, data_size);
        close(fp);
    }

   private:
    int fp;
    size_t data_size;
    void* data_pointer;
};

uint64_t next_pow2(uint64_t x) {
    return x == 1 ? 1 : 1 << (64 - __builtin_clzl(x - 1));
}

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

    std::optional<size_t> get_index(T* item) const {
        size_t index = apply_hash(item);
        auto entry = hash_map[index];
        if (entry.first == item) {
            return {entry.second};
        } else {
            return std::nullopt;
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

auto context_deleter = [](ZSTD_DCtx* context) { ZSTD_freeDCtx(context); };

struct PropertyReader {
    virtual std::vector<PyObjectWrapper> get_property_data(
        int32_t patient_offset, int32_t length) = 0;
    virtual ~PropertyReader(){};
};

std::unique_ptr<PropertyReader> create_property_reader(
    std::filesystem::path root_directory, const std::string& property_name,
    DataType property_type);

struct PatientDatabaseState
    : std::enable_shared_from_this<PatientDatabaseState> {
    PatientDatabaseState(std::string_view dir);

    size_t get_num_properties();
    std::optional<size_t> get_property_index(PyObject* property_name);
    std::vector<PyObjectWrapper> get_property_data(size_t index,
                                                   int32_t patient_offset,
                                                   int32_t length);
    int64_t get_patient_id(int32_t patient_offset) const;
    uint32_t get_patient_length(int32_t patient_offset) const;
    std::optional<int32_t> get_patient_offset(int64_t patient_id);

    PyObjectWrapper get_properties();
    PyObjectWrapper get_metadata();

    const std::filesystem::path root_directory;

    int num_patients;

    std::vector<std::pair<std::string, DataType>> properties;
    std::vector<std::unique_ptr<PropertyReader>> property_accessors;

    std::optional<PerfectHashMap<PyObject>> property_map;

    MmapFile patient_id_file;
    MmapFile length_file;

    std::optional<absl::flat_hash_map<int64_t, int32_t>> patient_offset_map;

    PyObjectWrapper py_properties;
    PyObjectWrapper py_metadata;
};

struct PatientData;

PyObject* create_event(std::shared_ptr<PatientData> patient,
                       int32_t event_offset);

struct PatientData {
    const std::shared_ptr<PatientDatabaseState> patient_database;

    const int32_t patient_offset;
    const int32_t patient_length;
    std::vector<std::optional<std::vector<PyObjectWrapper>>> saved_properties;

    PatientData(std::shared_ptr<PatientDatabaseState> pd, int32_t po)
        : patient_database(pd),
          patient_offset(po),
          patient_length(pd->get_patient_length(po)),
          saved_properties(pd->get_num_properties()) {}

    PyObject* get_property(PyObject* property_name, size_t event_index) {
        // Needs to get the property
        std::optional<size_t> index =
            patient_database->get_property_index(property_name);

        if (!index) {
            // Does not contain
            return PyErr_Format(PyExc_AttributeError,
                                "Could not find key %U in meds_reader.Event",
                                property_name);
        }

        auto& val = saved_properties[*index];

        if (!val) {
            // Force init it
            val.emplace(patient_database->get_property_data(
                *index, patient_offset, patient_length));
        }

        return (*val)[event_index].copy();
    }
};

struct Patient {
    PyObject_HEAD
        /* Type-specific fields go here. */

        alignas(std::shared_ptr<PatientData>) char state_data[sizeof(
            std::shared_ptr<PatientData>)];
    std::shared_ptr<PatientData>* state;

    PyObject* patient_id;
    PyObject* events;
};

void patient_dealloc(PyObject* self) {
    Patient* actual_self = (Patient*)self;
    if (actual_self->state != nullptr) {
        std::destroy_at(actual_self->state);
    }
    Py_XDECREF(actual_self->patient_id);
    Py_XDECREF(actual_self->events);
    Py_TYPE(self)->tp_free(self);
}

PyMemberDef PatientTypeMembers[] = {
    {.name = "patient_id",
     .type = T_OBJECT_EX,
     .offset = offsetof(Patient, patient_id),
     .flags = READONLY},
    {.name = "events",
     .type = T_OBJECT_EX,
     .offset = offsetof(Patient, events),
     .flags = READONLY},
    {nullptr},
};

PyObject* patient_str(PyObject* self) {
    Patient* actual_self = (Patient*)self;

    static_assert(sizeof(int64_t) == sizeof(long));
    int64_t patient_id = PyLong_AsLong(actual_self->patient_id);
    size_t num_events = PyTuple_GET_SIZE(actual_self->events);

    std::string debug_string = absl::StrCat("Patient(patient_id=", patient_id,
                                            ", len(events)=", num_events, ")");

    PyObject* py_string =
        PyUnicode_FromStringAndSize(debug_string.data(), debug_string.size());

    if (py_string == nullptr) {
        return PyErr_Format(
            PyExc_RuntimeError,
            "Could not convert the database path to a string ...");
    }

    return py_string;
}

PyTypeObject PatientType = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name = "meds_reader.Patient",
    .tp_basicsize = sizeof(Patient),
    .tp_itemsize = 0,
    .tp_dealloc = patient_dealloc,
    .tp_str = patient_str,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("Patient"),
    .tp_members = PatientTypeMembers,
    .tp_init = nullptr,
    .tp_new = nullptr,
};

PyObject* create_patient(std::shared_ptr<PatientDatabaseState> patient_database,
                         PyObject* patient_id) {
    std::optional<int32_t> patient_offset =
        patient_database->get_patient_offset(PyLong_AsLongLong(patient_id));

    if (!patient_offset) {
        return PyErr_Format(
            PyExc_KeyError,
            "Could not find patient_id %S in meds_reader.PatientDatabase",
            patient_id);
    }

    PyObjectWrapper self{PatientType.tp_alloc(&PatientType, 0)};

    Patient* actual_self = (Patient*)self.borrow();

    return wrap([&]() {
        actual_self->state = new (actual_self->state_data)
            std::shared_ptr<PatientData>(std::make_shared<PatientData>(
                patient_database, *patient_offset));
        int32_t length = patient_database->get_patient_length(*patient_offset);

        Py_INCREF(patient_id);
        actual_self->patient_id = patient_id;
        actual_self->events = PyTuple_New(length);

        for (int32_t i = 0; i < length; i++) {
            PyTuple_SET_ITEM(actual_self->events, i,
                             create_event(*(actual_self->state), i));
        }

        return self.steal();
    });
}

struct EventData {
    const std::shared_ptr<PatientData> patient_data;
    const int32_t event_index;

    EventData(std::shared_ptr<PatientData> pd, int32_t ei)
        : patient_data(pd), event_index(ei){};

    PyObject* get_property(PyObject* property_name) {
        return patient_data->get_property(property_name, event_index);
    }
};

struct Event {
    PyObject_HEAD
        /* Type-specific fields go here. */

        alignas(EventData) char state_data[sizeof(EventData)];
    EventData* state;
};

void event_dealloc(PyObject* self) {
    Event* actual_self = (Event*)self;
    if (actual_self->state != nullptr) {
        actual_self->state->~EventData();
    }
    Py_TYPE(self)->tp_free(self);
}

PyObject* event_getattro(PyObject* self, PyObject* key) {
    Event* actual_self = (Event*)self;
    PyUnicode_InternInPlace(&key);

    return wrap([&]() { return actual_self->state->get_property(key); });
}

PyObject* event_str(PyObject* self) {
    Event* actual_self = (Event*)self;

    PyObjectWrapper time_str{PyUnicode_FromString("time")};
    PyObjectWrapper code_str{PyUnicode_FromString("code")};

    PyObjectWrapper time_obj{
        (*actual_self->state).get_property(time_str.borrow())};
    PyObjectWrapper time_obj_string{PyObject_Str(time_obj.borrow())};

    PyObjectWrapper code_obj{
        (*actual_self->state).get_property(code_str.borrow())};

    std::string debug_string =
        absl::StrCat("Event(time=", PyUnicode_AsUTF8(time_obj_string.borrow()),
                     ", code=", PyUnicode_AsUTF8(code_obj.borrow()), ", ...)");

    PyObject* py_string =
        PyUnicode_FromStringAndSize(debug_string.data(), debug_string.size());

    if (py_string == nullptr) {
        return PyErr_Format(
            PyExc_RuntimeError,
            "Could not convert the database path to a string ...");
    }

    return py_string;
}

PyTypeObject EventType = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name = "meds_reader.Event",
    .tp_basicsize = sizeof(Event),
    .tp_itemsize = 0,
    .tp_dealloc = event_dealloc,
    .tp_str = event_str,
    .tp_getattro = event_getattro,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("Event"),
    .tp_init = nullptr,
    .tp_new = nullptr,
};

PyObject* create_event(std::shared_ptr<PatientData> patient,
                       int32_t event_index) {
    PyObjectWrapper self{EventType.tp_alloc(&EventType, 0)};

    Event* actual_self = (Event*)self.borrow();

    return wrap([&]() {
        actual_self->state =
            new (actual_self->state_data) EventData(patient, event_index);

        return self.steal();
    });
}

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
    StringPropertyReader(std::filesystem::path property_path)
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

    std::vector<char> decompressed;
    std::vector<uint32_t> values;

    std::vector<PyObjectWrapper> get_property_data(int32_t patient_offset,
                                                   int32_t length) {
        uint32_t offset = data_file.data<uint32_t>()[patient_offset];
        uint32_t num_bytes =
            data_file.data<uint32_t>()[patient_offset + 1] - offset;

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

        length_pointer = (uint32_t*)(decompressed.data());

        size_t value_size = *length_pointer;

        if (values.size() < value_size) {
            values.resize(value_size * 2);
        }

        size_t num_read = streamvbyte_decode(
            (const uint8_t*)decompressed.data() + sizeof(uint32_t),
            values.data(), value_size);

        if (num_read + sizeof(uint32_t) > decompressed_size) {
            throw std::runtime_error("Decoded too much? " +
                                     std::to_string(num_read) + " " +
                                     std::to_string(decompressed_size));
        }

        std::vector<PyObjectWrapper> result;
        result.reserve(length);

        size_t value_index = 0;
        while (result.size() < (size_t)length) {
            std::bitset<sizeof(uint32_t)* 8> bitset = values[value_index++];
            for (size_t j = 0;
                 j < bitset.size() && result.size() < (size_t)length; j++) {
                if (bitset[j]) {
                    uint32_t next_code = values[value_index++];
                    if (next_code % 2 == 0) {
                        result.emplace_back(dictionary[next_code / 2].copy());
                    } else {
                        uint32_t next_code_offset = values[value_index++];
                        std::string_view view(decompressed.data() +
                                                  sizeof(uint32_t) + num_read +
                                                  next_code_offset,
                                              (next_code / 2));
                        PyObject* value_string = PyUnicode_FromStringAndSize(
                            view.data(), view.size());
                        if (value_string == nullptr) {
                            throw std::runtime_error(
                                "Could not create unicode string");
                        }
                        result.emplace_back(value_string);
                    }
                } else {
                    Py_INCREF(Py_None);
                    result.emplace_back(Py_None);
                }
            }
        }

        if (value_index > value_size) {
            throw std::runtime_error("Out of bounds error for values");
        }

        return result;
    }
};

struct TimePropertyReader : PropertyReader {
    TimePropertyReader(std::filesystem::path property_path)
        : data_file(property_path) {}

    MmapFile data_file;

    std::vector<uint32_t> values;

    std::vector<PyObjectWrapper> get_property_data(int32_t patient_offset,
                                                   int32_t length) {
        uint32_t offset = data_file.data<uint32_t>()[patient_offset];
        uint32_t num_bytes =
            data_file.data<uint32_t>()[patient_offset + 1] - offset;

        uint32_t* length_pointer =
            (uint32_t*)(data_file.bytes().data() + offset);

        size_t num_values = *length_pointer;
        if (values.size() < num_values) {
            values.resize(num_values * 2);
        }

        size_t num_read =
            streamvbyte_decode_0124((const uint8_t*)data_file.bytes().data() +
                                        offset + sizeof(uint32_t),
                                    values.data(), num_values);

        if (num_read + sizeof(uint32_t) != num_bytes) {
            throw std::runtime_error("Decoded too much? " +
                                     std::to_string(num_read) + " " +
                                     std::to_string(num_bytes));
        }

        std::vector<PyObjectWrapper> result;
        result.reserve(length);

        absl::CivilSecond day(1970, 1, 1);
        uint64_t microseconds = 0;

        uint64_t seconds_per_day = (uint64_t)(24 * 60 * 60);
        uint64_t microseconds_per_second = (uint64_t)(1000 * 1000);

        auto add_times = [&](uint32_t copies) {
            PyObjectWrapper dt{PyDateTime_FromDateAndTime(
                day.year(), day.month(), day.day(), day.hour(), day.minute(),
                day.second(), microseconds)};
            if (dt.borrow() == nullptr) {
                PyErr_Print();
                throw std::runtime_error("Should never happen");
            }
            for (uint32_t i = 1; i < copies; i++) {
                result.emplace_back(dt.copy());
            }
            result.emplace_back(std::move(dt));
        };

        size_t value_index = 0;
        while (result.size() < (size_t)length) {
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

        return result;
    }
};

std::unique_ptr<PropertyReader> create_property_reader(
    std::filesystem::path root_directory, const std::string& property_name,
    DataType property_type) {
    if (property_type == DataType::STRING) {
        return std::make_unique<StringPropertyReader>(root_directory /
                                                      property_name);
    } else if (property_name == "time") {
        if (property_type != DataType::TIMESTAMP) {
            throw std::runtime_error("Time type must be a timestamp");
        }
        return std::make_unique<TimePropertyReader>(root_directory /
                                                    property_name);
    } else {
        throw std::runtime_error(
            "Unsupported property " + property_name + " " +
            std::to_string(static_cast<uint64_t>(property_type)));
    }
}

PyObjectWrapper create_pyarrow_dtype(PyObject* pyarrow, DataType type) {
    const char* name;
    const char* arg = nullptr;
    switch (type) {
        case DataType::STRING:
            name = "string";
            break;
        case DataType::TIMESTAMP:
            name = "timestamp";
            arg = "us";
            break;
        case DataType::FLOAT:
            name = "float32";
            break;
        case DataType::INT64:
            name = "int64";
            break;
        default:
            throw std::runtime_error(
                "Unhandled type " +
                std::to_string(static_cast<uint64_t>(type)));
    }

    PyObjectWrapper py_name{PyUnicode_FromString(name)};
    if (py_name.borrow() == nullptr) {
        throw std::runtime_error("Could not create string for arrow type?");
    }

    PyObjectWrapper result;

    if (arg == nullptr) {
        result = PyObject_CallMethodNoArgs(pyarrow, py_name.borrow());
    } else {
        PyObjectWrapper py_arg{PyUnicode_FromString(arg)};
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

PatientDatabaseState::PatientDatabaseState(std::string_view dir)
    : root_directory(dir),
      patient_id_file(root_directory / "patient_id"),
      length_file(root_directory / "length") {
    {
        PyObjectWrapper pyarrow{PyImport_ImportModule("pyarrow")};
        if (pyarrow.borrow() == nullptr) {
            throw std::runtime_error("Could not import pyarrow");
        }

        MmapFile property_file(root_directory / "properties");

        std::string_view current = property_file.bytes();

        std::vector<PyObject*> property_names;

        py_properties = PyDict_New();
        py_metadata = nullptr;

        while (current.size() != 0) {
            size_t next_size = *((size_t*)current.data());
            current = current.substr(sizeof(next_size));

            std::string next_name = std::string(current.substr(0, next_size));
            current = current.substr(next_size);

            // TODO: Use uint64_t here to be more compatible ...
            static_assert(sizeof(DataType) == sizeof(uint64_t));
            uint64_t casted_type = *((uint64_t*)current.data());
            current = current.substr(sizeof(casted_type));
            DataType type = static_cast<DataType>(casted_type);

            PyObjectWrapper property_name{PyUnicode_FromStringAndSize(
                next_name.data(), next_name.size())};
            if (property_name.borrow() == nullptr) {
                throw std::runtime_error(
                    "Could not construct a property name string?");
            }

            PyUnicode_InternInPlace(&property_name.ref);
            properties.emplace_back(std::move(next_name), type);

            PyObjectWrapper dtype =
                create_pyarrow_dtype(pyarrow.borrow(), type);

            property_names.emplace_back(property_name.borrow());

            if (PyDict_SetItem(py_properties.borrow(), property_name.steal(),
                               dtype.borrow()) == -1) {
                throw std::runtime_error(
                    "Could not insert properties in map ...");
            }
        }

        property_accessors.resize(properties.size());

        property_map.emplace(property_names);
    }

    num_patients = patient_id_file.data<int64_t>().size();
}

size_t PatientDatabaseState::get_num_properties() { return properties.size(); }

std::optional<size_t> PatientDatabaseState::get_property_index(
    PyObject* property_name) {
    PyUnicode_InternInPlace(&property_name);
    return property_map->get_index(property_name);
}

std::vector<PyObjectWrapper> PatientDatabaseState::get_property_data(
    size_t index, int32_t patient_offset, int32_t length) {
    if (property_accessors[index] == nullptr) {
        property_accessors[index] = create_property_reader(
            root_directory, properties[index].first, properties[index].second);
    }
    return property_accessors[index]->get_property_data(patient_offset, length);
}

int64_t PatientDatabaseState::get_patient_id(int32_t patient_offset) const {
    return patient_id_file.data<int64_t>()[patient_offset];
}

uint32_t PatientDatabaseState::get_patient_length(
    int32_t patient_offset) const {
    return length_file.data<uint32_t>()[patient_offset];
}

std::optional<int32_t> PatientDatabaseState::get_patient_offset(
    int64_t patient_id) {
    if (!patient_offset_map) {
        patient_offset_map.emplace();
        for (int i = 0; i < num_patients; i++) {
            patient_offset_map->insert(std::make_pair(get_patient_id(i), i));
        }
    }

    auto iter = patient_offset_map->find(patient_id);
    if (iter == std::end(*patient_offset_map)) {
        return std::nullopt;
    } else {
        return iter->second;
    }
}

PyObjectWrapper PatientDatabaseState::get_properties() {
    return PyObjectWrapper{py_properties.copy()};
}

PyObjectWrapper PatientDatabaseState::get_metadata() {
    if (py_metadata.borrow() == nullptr) {
        MmapFile metadata_file(root_directory / "metadata.json");
        PyObjectWrapper json{PyImport_ImportModule("json")};
        PyObjectWrapper loads{PyUnicode_FromString("loads")};
        PyObjectWrapper metadata_string{PyUnicode_FromStringAndSize(
            metadata_file.bytes().data(), metadata_file.bytes().size())};
        py_metadata = PyObject_CallMethodOneArg(json.borrow(), loads.borrow(),
                                                metadata_string.borrow());
    }

    return PyObjectWrapper{py_metadata.copy()};
}

struct PatientDatabase {
    PyObject_HEAD
        /* Type-specific fields go here. */

        alignas(std::shared_ptr<PatientDatabaseState>) char state_data[sizeof(
            std::shared_ptr<PatientDatabaseState>)];
    std::shared_ptr<PatientDatabaseState>* state;
};

void patient_database_dealloc(PyObject* self) {
    PatientDatabase* actual_self = (PatientDatabase*)self;

    if (actual_self->state != nullptr) {
        std::destroy_at(actual_self->state);
    }

    Py_TYPE(self)->tp_free(self);
}

int patient_database_init(PyObject* self, PyObject* args, PyObject* kwds) {
    PatientDatabase* actual_self = (PatientDatabase*)self;

    auto result = wrap([&]() {
        if (actual_self->state != nullptr) {
            throw std::runtime_error("Cannot double init a patient database");
        }

        if (PyTuple_GET_SIZE(args) != 1) {
            throw std::runtime_error("Must be passed a string argument");
        }
        PyObject* str = PyTuple_GET_ITEM(args, 0);

        if (!PyUnicode_Check(str)) {
            throw std::runtime_error("Must be passed a *string* argument");
        }

        Py_ssize_t path_size;
        const char* path_str = PyUnicode_AsUTF8AndSize(str, &path_size);

        if (path_str == nullptr) {
            return (PyObject*)nullptr;
        }

        actual_self->state =
            new (actual_self->state_data) std::shared_ptr<PatientDatabaseState>(
                std::make_shared<PatientDatabaseState>(
                    std::string_view(path_str, path_size)));
        return str;
    });

    if (result == nullptr) {
        return -1;
    }

    return 0;
}

Py_ssize_t patient_database_get_length(PyObject* self) {
    PatientDatabase* actual_self = (PatientDatabase*)self;
    Py_ssize_t result = (*actual_self->state)->num_patients;
    return result;
}

PyObject* patient_database_get_item(PyObject* self, PyObject* patient_id) {
    PatientDatabase* actual_self = (PatientDatabase*)self;
    return create_patient(*(actual_self->state), patient_id);
}

extern PyTypeObject PatientDatabaseType;

PyObject* patient_database_reduce(PyObject* self, PyObject* Py_UNUSED(unused)) {
    PatientDatabase* actual_self = (PatientDatabase*)self;

    std::string path = (*(actual_self->state))->root_directory.string();

    PyObject* path_string =
        PyUnicode_FromStringAndSize(path.data(), path.size());

    if (path_string == nullptr) {
        return PyErr_Format(
            PyExc_RuntimeError,
            "Could not convert the database path to a string ...");
    }

    PyObject* args = PyTuple_Pack(1, path_string);

    Py_INCREF(&PatientDatabaseType);
    PyObject* result = PyTuple_Pack(2, &PatientDatabaseType, args);

    return result;
}

PyMethodDef patient_database_methods[] = {
    {"__reduce__", patient_database_reduce, METH_NOARGS,
     "Save the patient database"},
    {nullptr}};

PyMappingMethods PatientDatabaseMappingMethods = {
    .mp_length = patient_database_get_length,
    .mp_subscript = patient_database_get_item,
};

struct PatientDatabaseIterator {
    PyObject_HEAD
        /* Type-specific fields go here. */

        PyObject* patient_database;
    int32_t index;
};

void patient_database_iterator_dealloc(PyObject* self) {
    PatientDatabaseIterator* actual_self = (PatientDatabaseIterator*)self;
    Py_DECREF(actual_self->patient_database);
    Py_TYPE(self)->tp_free(self);
}

PyObject* patient_database_iterator_next(PyObject* self) {
    PatientDatabaseIterator* actual_self = (PatientDatabaseIterator*)self;
    PatientDatabase* patient_database =
        (PatientDatabase*)actual_self->patient_database;

    if (actual_self->index >= (*(patient_database->state))->num_patients) {
        return PyErr_Format(PyExc_StopIteration,
                            "Exceeded the size of the PatientDatabase");
    }

    static_assert(sizeof(int64_t) == sizeof(long long));

    return wrap([&]() {
        int64_t patient_id =
            (*patient_database->state)->get_patient_id(actual_self->index++);
        return PyLong_FromLongLong(patient_id);
    });
}

PyObject* patient_database_str(PyObject* self) {
    PatientDatabase* actual_self = (PatientDatabase*)self;

    std::string path = (*(actual_self->state))->root_directory.string();

    std::string debug_string = absl::StrCat("PatientDatabase(path=", path, ")");

    PyObject* py_string =
        PyUnicode_FromStringAndSize(debug_string.data(), debug_string.size());

    if (py_string == nullptr) {
        return PyErr_Format(
            PyExc_RuntimeError,
            "Could not convert the database path to a string ...");
    }

    return py_string;
}

PyTypeObject PatientDatabaseIteratorType = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        "meds_reader.PatientDatabaseIterator",
    .tp_basicsize = sizeof(PatientDatabaseIterator),
    .tp_itemsize = 0,
    .tp_dealloc = patient_database_iterator_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("PatientDatabaseIterator"),
    .tp_iternext = patient_database_iterator_next,
    .tp_init = nullptr,
    .tp_new = nullptr,
};

PyObject* patient_database_iter(PyObject* self) {
    PyObject* iter =
        PatientDatabaseIteratorType.tp_alloc(&PatientDatabaseIteratorType, 0);
    PatientDatabaseIterator* actual_iter = (PatientDatabaseIterator*)iter;

    Py_INCREF(self);
    actual_iter->patient_database = self;

    actual_iter->index = 0;

    return iter;
}

PyObject* patient_database_get_properties(PyObject* self, void*) {
    PatientDatabase* actual_self = (PatientDatabase*)self;
    return wrap(
        [&]() { return (*actual_self->state)->get_properties().steal(); });
}

PyObject* patient_database_get_metadata(PyObject* self, void*) {
    PatientDatabase* actual_self = (PatientDatabase*)self;

    return wrap(
        [&]() { return (*actual_self->state)->get_metadata().steal(); });
}

PyGetSetDef PatientDatabaseGetSet[] = {
    {.name = "properties", .get = patient_database_get_properties},
    {.name = "metadata", .get = patient_database_get_metadata},
    {nullptr},
};

PyTypeObject PatientDatabaseType = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        "meds_reader.PatientDatabase",
    .tp_basicsize = sizeof(PatientDatabase),
    .tp_itemsize = 0,
    .tp_dealloc = patient_database_dealloc,
    .tp_as_mapping = &PatientDatabaseMappingMethods,
    .tp_str = patient_database_str,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("Patient Database"),
    .tp_iter = patient_database_iter,
    .tp_methods = patient_database_methods,
    .tp_getset = PatientDatabaseGetSet,
    .tp_init = patient_database_init,
    .tp_new = PyType_GenericNew,
};

struct PyModuleDef meds_reader_module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "meds_reader",
};

}  // namespace

PyMODINIT_FUNC PyInit__meds_reader(void) {
    PyDateTime_IMPORT;

    if (PyType_Ready(&PatientDatabaseType) < 0) {
        return NULL;
    }
    if (PyType_Ready(&PatientType) < 0) {
        return NULL;
    }
    if (PyType_Ready(&EventType) < 0) {
        return NULL;
    }

    PyObject* m = PyModule_Create(&meds_reader_module);
    if (m == NULL) {
        return NULL;
    }

    if (PyModule_AddType(m, &PatientDatabaseType) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &PatientDatabaseIteratorType) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &PatientType) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &EventType) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
