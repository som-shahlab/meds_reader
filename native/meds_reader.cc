#include <filesystem>
#include <optional>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <datetime.h>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "dtype.hh"
#include "fast_shared_ptr.hh"
#include "mmap_file.hh"
#include "perfect_hash.hh"
#include "property_reader.hh"
#include "pyutils.hh"

namespace {

struct PatientDatabase : public PyObject,
                         fast_shared_ptr_object<PatientDatabase> {
    static PyTypeObject Type;

    PatientDatabase(std::string_view dir);

    size_t get_num_properties();
    ssize_t get_property_index(PyObject* property_name);
    std::vector<PyObjectWrapper> get_property_data(size_t index,
                                                   int32_t patient_offset,
                                                   int32_t length);
    int64_t get_patient_id(int32_t patient_offset) const;
    uint32_t get_patient_length(int32_t patient_offset) const;
    std::optional<int32_t> get_patient_offset(int64_t patient_id);

    // Python methods
    //-----------------------------------------
    static PyObject* create(PyTypeObject* type, PyObject* args, PyObject* kwds);

    PyObject* get_properties(void*);
    PyObject* get_metadata(void*);
    Py_ssize_t length();
    __attribute__((always_inline)) PyObject* subscript(PyObject* patient_id);
    PyObject* reduce(PyObject* Py_UNUSED(unused));

    PyObject* str();
    PyObject* iter();
    void dealloc() { decref(); }
    //-----------------------------------------

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

PyMappingMethods PatientDatabaseMappingMethods = {
    .mp_length = convert_to_cfunc<&PatientDatabase::length>(),
    .mp_subscript = convert_to_cfunc<&PatientDatabase::subscript>(),
};

PyGetSetDef PatientDatabaseGetSet[] = {
    {.name = "properties",
     .get = convert_to_cfunc<&PatientDatabase::get_properties>()},
    {.name = "metadata",
     .get = convert_to_cfunc<&PatientDatabase::get_metadata>()},
    {nullptr},
};

PyTypeObject PatientDatabase::Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        "meds_reader.PatientDatabase",
    .tp_basicsize = sizeof(PatientDatabase),
    .tp_itemsize = 0,
    .tp_dealloc = convert_to_cfunc<&PatientDatabase::dealloc>(),
    .tp_as_mapping = &PatientDatabaseMappingMethods,
    .tp_str = convert_to_cfunc<&PatientDatabase::str>(),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("Patient Database"),
    .tp_iter = convert_to_cfunc<&PatientDatabase::iter>(),
    .tp_getset = PatientDatabaseGetSet,
    .tp_init = nullptr,
    .tp_alloc = nullptr,
    .tp_new = PatientDatabase::create,
    .tp_free = nullptr,
};

struct Event;

struct Patient : public PyObject, fast_shared_ptr_object<Patient> {
    static PyTypeObject Type;

    static PyObject* create(const fast_shared_ptr<PatientDatabase>& pd,
                            int32_t po, PyObject* patient_id);

    const fast_shared_ptr<PatientDatabase> patient_database;

    const int32_t patient_offset;
    const int32_t patient_length;
    void* allocation;
    std::vector<PyObjectWrapper>* saved_properties;
    Event* events;

    PyObjectWrapper patient_id;
    PyObjectWrapper events_obj;

    // Python methods
    //-----------------------------------------
    PyObject* str();
    void dealloc();
    __attribute__((always_inline)) PyObject* get_patient_id(void*);
    __attribute__((always_inline)) PyObject* get_events(void*);
    //-----------------------------------------

    void operator delete(void* ptr) { free(ptr); }
    ~Patient();

   private:
    friend Event;

    Patient(const fast_shared_ptr<PatientDatabase>& pd, int32_t po,
            PyObject* patient_id);

    PyObject* get_property(PyObject* property_name, Event* event_ptr);
};

PyGetSetDef PatientGetSet[] = {
    {.name = "patient_id", .get = convert_to_cfunc<&Patient::get_patient_id>()},
    {.name = "events", .get = convert_to_cfunc<&Patient::get_events>()},
    {nullptr},
};

PyTypeObject Patient::Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name = "meds_reader.Patient",
    .tp_basicsize = sizeof(Patient),
    .tp_itemsize = 0,
    .tp_dealloc = convert_to_cfunc<&Patient::dealloc>(),
    .tp_str = convert_to_cfunc<&Patient::str>(),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("Patient"),
    .tp_getset = PatientGetSet,
    .tp_init = nullptr,
    .tp_alloc = nullptr,
    .tp_new = nullptr,
    .tp_free = nullptr,
};

struct Event : public PyObject {
    static PyTypeObject Type;

    fast_shared_ptr<Patient> patient;

    Event(const fast_shared_ptr<Patient>& pd);

    // Python methods
    //-----------------------------------------
    void dealloc();
    PyObject* str();
    __attribute__((always_inline)) PyObject* getattro(PyObject* key);
    //-----------------------------------------
};

PyTypeObject Event::Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name = "meds_reader.Event",
    .tp_basicsize = sizeof(Event),
    .tp_itemsize = 0,
    .tp_dealloc = convert_to_cfunc<&Event::dealloc>(),
    .tp_str = convert_to_cfunc<&Event::str>(),
    .tp_getattro = convert_to_cfunc<&Event::getattro>(),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("Event"),
    .tp_init = nullptr,
    .tp_alloc = nullptr,
    .tp_new = nullptr,
    .tp_free = nullptr,
};

size_t get_num_needed_bytes(int32_t num_properties, int32_t patient_length) {
    constexpr size_t property_alignment = alignof(std::vector<PyObjectWrapper>);
    constexpr size_t event_alignment = alignof(Event);

    size_t i = 0;

    i += sizeof(Patient);

    // Now align
    size_t extra_bytes = i % property_alignment;
    if (extra_bytes > 0) {
        i += ((ssize_t)property_alignment - (ssize_t)extra_bytes);
    }

    i += num_properties * sizeof(std::vector<PyObjectWrapper>);

    // Now align
    extra_bytes = i % event_alignment;
    if (extra_bytes > 0) {
        i += ((ssize_t)event_alignment - (ssize_t)extra_bytes);
    }

    i += sizeof(Event) * patient_length;

    return i;
}

size_t get_property_offset() {
    constexpr size_t property_alignment = alignof(std::vector<PyObjectWrapper>);

    size_t i = 0;

    i += sizeof(Patient);

    // Now align
    size_t extra_bytes = i % property_alignment;
    if (extra_bytes > 0) {
        i += ((ssize_t)property_alignment - (ssize_t)extra_bytes);
    }

    return i;
}

size_t get_event_offset(int32_t num_properties) {
    constexpr size_t property_alignment = alignof(std::vector<PyObjectWrapper>);
    constexpr size_t event_alignment = alignof(Event);

    size_t i = 0;

    i += sizeof(Patient);

    // Now align
    size_t extra_bytes = i % property_alignment;
    if (extra_bytes > 0) {
        i += ((ssize_t)property_alignment - (ssize_t)extra_bytes);
    }

    i += num_properties * sizeof(std::vector<PyObjectWrapper>);

    // Now align
    extra_bytes = i % event_alignment;
    if (extra_bytes > 0) {
        i += ((ssize_t)event_alignment - (ssize_t)extra_bytes);
    }

    return i;
}

Patient::Patient(const fast_shared_ptr<PatientDatabase>& pd, int32_t po,
                 PyObject* pid)
    : patient_database(pd),
      patient_offset(po),
      patient_length(pd->get_patient_length(po)),
      patient_id(pid) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);
    Py_INCREF(patient_id.borrow());
    char* allocation = reinterpret_cast<char*>(this);
    saved_properties = reinterpret_cast<std::vector<PyObjectWrapper>*>(
        allocation + get_property_offset());
    events = reinterpret_cast<Event*>(
        allocation + get_event_offset(pd->get_num_properties()));

    for (size_t i = 0; i < pd->get_num_properties(); i++) {
        new (saved_properties + i) std::vector<PyObjectWrapper>();
    }

    events_obj = PyTuple_New(patient_length);

    for (int32_t i = 0; i < patient_length; i++) {
        Event* e = new (events + i) Event(shared_from_this());
        PyTuple_SET_ITEM(events_obj.borrow(), i, static_cast<PyObject*>(e));
    }
}

Patient::~Patient() {
    if (events_obj.ref != nullptr) {
        std::cerr << "Patient dies before the events object dies?" << std::endl;
        std::terminate();
    }

    for (size_t i = 0; i < patient_database->get_num_properties(); i++) {
        saved_properties[i].~vector<PyObjectWrapper>();
    }

    for (int32_t i = 0; i < patient_length; i++) {
        if (events[i].patient.get() != nullptr) {
            std::cerr << "Patient dies before a child event object dies?"
                      << std::endl;
            std::terminate();
        }
        events[i].~Event();
    }
}

PyObject* Patient::get_property(PyObject* property_name, Event* event_ptr) {
    size_t event_index = event_ptr - events;
    // Needs to get the property
    ssize_t index = patient_database->get_property_index(property_name);

    if (index == -1) [[unlikely]] {
        // Does not contain
        return PyErr_Format(PyExc_AttributeError,
                            "Could not find key %U in meds_reader.Event",
                            property_name);
    }

    auto& val = saved_properties[index];

    if (val.size() == 0) {
        // Force init it
        val = patient_database->get_property_data(index, patient_offset,
                                                  patient_length);
    }

    if (val[event_index].borrow() == nullptr) {
        Py_RETURN_NONE;
    }

    return val[event_index].copy();
};

PyObject* Patient::create(const fast_shared_ptr<PatientDatabase>& pd,
                          int32_t patient_offset, PyObject* patient_id) {
    int32_t length = pd->get_patient_length(patient_offset);

    constexpr size_t property_alignment = alignof(std::vector<PyObjectWrapper>);
    constexpr size_t event_alignment = alignof(Event);
    constexpr size_t patient_alignment = alignof(Patient);

    size_t common_alignment = std::lcm(
        property_alignment, std::lcm(event_alignment, patient_alignment));

    if (common_alignment > alignof(max_align_t)) {
        throw std::runtime_error("This should never happen");
    }

    void* data =
        calloc(1, get_num_needed_bytes(pd->get_num_properties(), length));
    Patient* casted_data = new (data) Patient(pd, patient_offset, patient_id);
    return static_cast<PyObject*>(casted_data);
}

inline PyObject* Patient::get_patient_id(void*) { return patient_id.copy(); }

inline PyObject* Patient::get_events(void*) { return events_obj.copy(); }

void Patient::dealloc() {
    patient_id = std::move(PyObjectWrapper());
    events_obj = PyObjectWrapper();
    decref();
}

PyObject* Patient::str() {
    static_assert(sizeof(int64_t) == sizeof(long));
    int64_t patient_id_val = PyLong_AsLong(patient_id.borrow());
    size_t num_events = PyTuple_GET_SIZE(events_obj.borrow());

    std::string debug_string =
        absl::StrCat("Patient(patient_id=", patient_id_val,
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

Event::Event(const fast_shared_ptr<Patient>& pd) : patient(pd) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);
}

void Event::dealloc() { patient.reset(); }

inline PyObject* Event::getattro(PyObject* key) {
    Py_INCREF(key);
    PyObjectWrapper key_wrapper(key);
    PyUnicode_InternInPlace(&key_wrapper.ref);

    return patient->get_property(key_wrapper.borrow(), this);
}

PyObject* Event::str() {
    PyObjectWrapper time_str{PyUnicode_FromString("time")};
    PyObjectWrapper code_str{PyUnicode_FromString("code")};

    PyObjectWrapper time_obj{getattro(time_str.borrow())};
    PyObjectWrapper time_obj_string{PyObject_Str(time_obj.borrow())};

    PyObjectWrapper code_obj{getattro(code_str.borrow())};

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

PatientDatabase::PatientDatabase(std::string_view dir)
    : root_directory(dir),
      patient_id_file(root_directory / "patient_id"),
      length_file(root_directory / "length") {
    {
        PyObject_Init(static_cast<PyObject*>(this), &Type);
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

            if (PyDict_SetItem(py_properties.borrow(), property_name.borrow(),
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

size_t PatientDatabase::get_num_properties() { return properties.size(); }

ssize_t PatientDatabase::get_property_index(PyObject* property_name) {
    PyUnicode_InternInPlace(&property_name);
    return property_map->get_index(property_name);
}

std::vector<PyObjectWrapper> PatientDatabase::get_property_data(
    size_t index, int32_t patient_offset, int32_t length) {
    if (property_accessors[index] == nullptr) {
        property_accessors[index] = create_property_reader(
            root_directory, properties[index].first, properties[index].second);
    }
    return property_accessors[index]->get_property_data(patient_offset, length);
}

int64_t PatientDatabase::get_patient_id(int32_t patient_offset) const {
    return patient_id_file.data<int64_t>()[patient_offset];
}

uint32_t PatientDatabase::get_patient_length(int32_t patient_offset) const {
    return length_file.data<uint32_t>()[patient_offset];
}

std::optional<int32_t> PatientDatabase::get_patient_offset(int64_t patient_id) {
    if (!patient_offset_map) {
        patient_offset_map.emplace();
        for (int i = 0; i < num_patients; i++) {
            if (patient_offset_map->find(get_patient_id(i)) !=
                std::end(*patient_offset_map)) {
                throw std::runtime_error("Cannot make this work! " +
                                         std::to_string(i) + " " +
                                         std::to_string(get_patient_id(i)));
            }
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

PyObject* PatientDatabase::get_properties(void*) {
    return py_properties.copy();
}

PyObject* PatientDatabase::get_metadata(void*) {
    if (py_metadata.borrow() == nullptr) {
        MmapFile metadata_file(root_directory / "metadata.json");
        PyObjectWrapper json{PyImport_ImportModule("json")};
        PyObjectWrapper loads{PyUnicode_FromString("loads")};
        PyObjectWrapper metadata_string{PyUnicode_FromStringAndSize(
            metadata_file.bytes().data(), metadata_file.bytes().size())};
        py_metadata = PyObject_CallMethodOneArg(json.borrow(), loads.borrow(),
                                                metadata_string.borrow());
    }

    return py_metadata.copy();
}

PyObject* PatientDatabase::create(PyTypeObject* type, PyObject* args,
                                  PyObject* kwds) {
    if (type != &Type) {
        throw std::runtime_error("Creating wrong type?");
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
        return nullptr;
    }

    PatientDatabase* database =
        new PatientDatabase(std::string_view(path_str, path_size));

    return static_cast<PyObject*>(database);
}

Py_ssize_t PatientDatabase::length() { return num_patients; }

inline PyObject* PatientDatabase::subscript(PyObject* patient_id) {
    if (!PyLong_Check(patient_id)) {
        return PyErr_Format(
            PyExc_TypeError,
            "PatientDatabase only supports integer patient_ids");
    }

    static_assert(sizeof(long) == sizeof(int64_t));
    int64_t int_patient_id = PyLong_AsLong(patient_id);

    if (PyErr_Occurred()) {
        return nullptr;
    }

    std::optional<int32_t> patient_offset = get_patient_offset(int_patient_id);

    if (!patient_offset) {
        return PyErr_Format(PyExc_KeyError,
                            "PatientDatabase did not have patient %d",
                            int_patient_id);
    }

    return Patient::create(shared_from_this(), *patient_offset, patient_id);
}

PyObject* PatientDatabase::str() {
    std::string path = root_directory.string();

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
        static_cast<PatientDatabase*>(actual_self->patient_database);

    if (actual_self->index >= patient_database->num_patients) {
        return PyErr_Format(PyExc_StopIteration,
                            "Exceeded the size of the PatientDatabase");
    }

    static_assert(sizeof(int64_t) == sizeof(long long));
    // std::cout<<"Before thing " << patient_database << std::endl;

    int64_t patient_id = patient_database->get_patient_id(actual_self->index++);
    // std::cout<<"About to do the thing" << std::endl;
    PyObject* result = PyLong_FromLongLong(patient_id);

    // std::cout<<"Did the thing " << std::endl;
    return result;
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

PyObject* PatientDatabase::iter() {
    PyObject* iter =
        PatientDatabaseIteratorType.tp_alloc(&PatientDatabaseIteratorType, 0);
    PatientDatabaseIterator* actual_iter = (PatientDatabaseIterator*)iter;

    Py_INCREF(this);
    actual_iter->patient_database = static_cast<PyObject*>(this);

    actual_iter->index = 0;

    return iter;
}

struct PyModuleDef meds_reader_module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "meds_reader",
};

}  // namespace

PyMODINIT_FUNC PyInit__meds_reader(void) {
    PyDateTime_IMPORT;

    if (PyType_Ready(&PatientDatabase::Type) < 0) {
        return NULL;
    }
    if (PyType_Ready(&Patient::Type) < 0) {
        return NULL;
    }
    if (PyType_Ready(&Event::Type) < 0) {
        return NULL;
    }

    PyObject* m = PyModule_Create(&meds_reader_module);
    if (m == NULL) {
        return NULL;
    }

    if (PyModule_AddType(m, &PatientDatabase::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &PatientDatabaseIteratorType) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &Patient::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &Event::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
