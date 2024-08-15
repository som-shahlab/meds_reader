#include <filesystem>
#include <optional>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <datetime.h>

#include <fstream>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "binary_version.hh"
#include "dtype.hh"
#include "fast_shared_ptr.hh"
#include "mmap_file.hh"
#include "perfect_hash.hh"
#include "property_reader.hh"
#include "pyutils.hh"

namespace {

struct Patient;
struct PatientDatabase;


struct PatientDatabaseIterator : public PyObject {
    static PyTypeObject Type;

    PatientDatabaseIterator();

    void init(PatientDatabase* database);

    // Python methods
    //-----------------------------------------
    void dealloc();
    PyObject* next();
    //-----------------------------------------

    bool in_use;
    PatientDatabase* patient_database;
    int32_t index;
};

PyTypeObject PatientDatabaseIterator::Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        "meds_reader.PatientDatabaseIterator",
    .tp_basicsize = sizeof(PatientDatabaseIterator),
    .tp_itemsize = 0,
    .tp_dealloc = convert_to_cfunc<&PatientDatabaseIterator::dealloc>(),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("PatientDatabaseIterator"),
    .tp_iternext = convert_to_cfunc<&PatientDatabaseIterator::next>(),
    .tp_init = nullptr,
    .tp_new = nullptr,
};

struct PatientDatabase : public PyObject,
                         fast_shared_ptr_object<PatientDatabase> {
    static PyTypeObject Type;

    PatientDatabase(std::string_view dir);
    ~PatientDatabase();

    size_t get_num_properties();
    PyObject* get_property_name(size_t property_index);
    ssize_t get_property_index(PyObject* property_name);
    void get_property_data(size_t index,
                                                   int32_t patient_offset,
                                                   int32_t length, PyObject** result);
    void get_null_map(int32_t patient_offset, int32_t length, uint64_t* result);
    int64_t get_patient_id(int32_t patient_offset) const;
    uint32_t get_patient_length(int32_t patient_offset) const;
    std::optional<int32_t> get_patient_offset(int64_t patient_id);

    // Python methods
    //-----------------------------------------
    static PyObject* create(PyTypeObject* type, PyObject* args, PyObject* kwds);

    PyObject* get_properties(void*);
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
    std::unique_ptr<NullMapReader> null_map_reader;

    std::vector<PyObjectWrapper> property_names;
    std::optional<PerfectHashMap<PyObject>> property_map;

    MmapFile patient_id_file;
    MmapFile length_file;

    std::optional<absl::flat_hash_map<int64_t, int32_t>> patient_offset_map;

    PyObjectWrapper py_properties;

    absl::InlinedVector<Patient*, 4> patients;
    absl::InlinedVector<PatientDatabaseIterator, 4> patient_database_iterators;
};

PyMappingMethods PatientDatabaseMappingMethods = {
    .mp_length = convert_to_cfunc<&PatientDatabase::length>(),
    .mp_subscript = convert_to_cfunc<&PatientDatabase::subscript>(),
};

PyGetSetDef PatientDatabaseGetSet[] = {
    {.name = "properties",
     .get = convert_to_cfunc<&PatientDatabase::get_properties>()},
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
    .tp_new = convert_to_cfunc<&PatientDatabase::create>(),
    .tp_free = nullptr,
};

struct PatientEvents;
struct Event;
struct EventPropertyIterator;

struct PatientEventsIterator : public PyObject {
    static PyTypeObject Type;

    PatientEventsIterator();

    void init(PatientEvents* events);

    // Python methods
    //-----------------------------------------
    void dealloc();
    __attribute__((always_inline)) PyObject* next();
    PyObject* iter();
    //-----------------------------------------

    Event* raw_events;
    PatientEvents* events;
    int event_index;
    int32_t patient_length;
    bool in_use;
};


PyTypeObject PatientEventsIterator::Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        "meds_reader.PatientEventsIterator",
    .tp_basicsize = sizeof(PatientEventsIterator),
    .tp_itemsize = 0,
    .tp_dealloc = convert_to_cfunc<&PatientEventsIterator::dealloc>(),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("PatientEventsIterator"),
    .tp_iter = convert_to_cfunc<&PatientEventsIterator::iter>(),
    .tp_iternext = convert_to_cfunc<&PatientEventsIterator::next>(),
    .tp_init = nullptr,
    .tp_new = nullptr,
};

struct PatientEvents : public PyObject {
    static PyTypeObject Type;

    void init(Patient* patient, Event* events, int32_t patient_length);

    // Python methods
    //-----------------------------------------
    PyObject* str();
    void dealloc();
    __attribute__((always_inline)) PyObject* subscript(PyObject*);
    __attribute__((always_inline)) Py_ssize_t length();
    __attribute__((always_inline)) PyObject* iter();
    //-----------------------------------------

    Patient* patient;
    int patient_length;
    Event* events;
    PyObject* length_obj;
    absl::InlinedVector<PatientEventsIterator, 4> iterators;
};

PyMappingMethods PatientEventsMappingMethods = {
    .mp_length = convert_to_cfunc<&PatientEvents::length>(),
    .mp_subscript = convert_to_cfunc<&PatientEvents::subscript>(),
};

PyTypeObject PatientEvents::Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name = "meds_reader.PatientEvents",
    .tp_basicsize = sizeof(PatientEvents),
    .tp_itemsize = 0,
    .tp_dealloc = convert_to_cfunc<&PatientEvents::dealloc>(),
    .tp_as_mapping = &PatientEventsMappingMethods,
    .tp_str = convert_to_cfunc<&PatientEvents::str>(),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("PatientEvents"),
    .tp_iter = convert_to_cfunc<&PatientEvents::iter>(),
    .tp_init = nullptr,
    .tp_alloc = nullptr,
    .tp_new = nullptr,
    .tp_free = nullptr,
};


struct EventPropertyIterator : public PyObject {
    static PyTypeObject Type;

    bool in_use;
    Patient* patient;
    Event* event;
    uint64_t current_index;

    EventPropertyIterator();
    void init(Patient* patient, Event* e);

    // Python methods
    //-----------------------------------------
    void dealloc();
    __attribute__((always_inline)) PyObject* next();
    PyObject* iter();
    //-----------------------------------------
};

PyTypeObject EventPropertyIterator::Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        "meds_reader.EventPropertyIterator",
    .tp_basicsize = sizeof(EventPropertyIterator),
    .tp_itemsize = 0,
    .tp_dealloc = convert_to_cfunc<&EventPropertyIterator::dealloc>(),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("EventPropertyIterator"),
    .tp_iter = convert_to_cfunc<&EventPropertyIterator::iter>(),
    .tp_iternext = convert_to_cfunc<&EventPropertyIterator::next>(),
    .tp_init = nullptr,
    .tp_alloc = nullptr,
    .tp_new = nullptr,
    .tp_free = nullptr,
};

struct Patient : public PyObject, fast_shared_ptr_object<Patient> {
    static PyTypeObject Type;

    static Patient* create(PatientDatabase* pd, size_t capacity);

    void init(int32_t patient_offset, int32_t patient_length, PyObject* patient_id);

    PatientDatabase* patient_database;
    const size_t capacity;

    int32_t patient_offset;
    int32_t patient_length;
    PyObject* patient_id;

    bool in_use;

    Event* events;
    uint64_t* null_map;
    bool null_map_initialized;
    PyObject** saved_properties;
    std::bitset<64> properties_initialized;

    PatientEvents events_obj;

    absl::InlinedVector<EventPropertyIterator, 4> event_property_iterators;

    // Python methods
    //-----------------------------------------
    PyObject* str();
    void dealloc();
    __attribute__((always_inline)) PyObject* get_patient_id(void*);
    __attribute__((always_inline)) PyObject* get_events(void*);
    //-----------------------------------------

    void delete_self();
    void operator delete(void* ptr) { free(ptr); }
    ~Patient();

   private:
    friend Event;
    friend EventPropertyIterator;

    Patient(PatientDatabase* pd, size_t capacity, char* data);

    PyObject* get_property(PyObject* property_name, Event* event_ptr);
    PyObject* get_property(size_t property_index, Event* event_ptr);
    uint64_t get_null_map(Event* event_ptr);
    PyObject* create_event_property_iterator(Event* event_ptr);
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

    Patient* patient;

    void init(Patient* patient);

    // Python methods
    //-----------------------------------------
    void dealloc();
    PyObject* str();
    __attribute__((always_inline)) PyObject* getattro(PyObject* key);
    PyObject* iter();
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
    .tp_iter = convert_to_cfunc<&Event::iter>(),
    .tp_init = nullptr,
    .tp_alloc = nullptr,
    .tp_new = nullptr,
    .tp_free = nullptr,
};


PatientEventsIterator::PatientEventsIterator() {
    in_use = false;
}

void PatientEventsIterator::init(PatientEvents* e) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);

    events = e;
    Py_INCREF(events);
    raw_events = e->events;
    patient_length = e->patient_length;

    event_index = 0;
}

void PatientEventsIterator::dealloc() {
    in_use = false;
    Py_DECREF(events);
}

inline PyObject* PatientEventsIterator::next() {
    if (event_index == patient_length) {
        return PyErr_Format(PyExc_StopIteration,
                            "Exceeded the number of properties in events");
    } else {
        PyObject* event = raw_events + event_index;
        event_index++;
        Py_INCREF(event);
        return event;
    }
}

PyObject* PatientEventsIterator::iter() {
    Py_INCREF(this);
    return this;
}

void PatientEvents::init(Patient* p, Event* e, int pl) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);

    patient = p;

    patient->incref();

    events = e;
    patient_length = pl;
    length_obj = nullptr;

    for (int i =0 ; i < patient_length;i++) {
        events[i].init(patient);
    }
}

inline PyObject* PatientEvents::iter() {
    size_t desired_index = iterators.size();

    for (size_t i = 0; i < iterators.size(); i++) {
        if (!iterators[i].in_use) {
            desired_index = i;
            break;
        }
    }

    if (desired_index == iterators.size()) {
        iterators.emplace_back();
    }

    iterators[desired_index].init(this);

    return iterators.data() + desired_index;
}

inline Py_ssize_t PatientEvents::length() { return patient_length; }


inline PyObject* PatientEvents::subscript(PyObject* event_index) {
    if ( PyLong_Check(event_index)) {
        Py_ssize_t event_index_integer = PyLong_AsSsize_t(event_index);

        if (PyErr_Occurred()) {
            return nullptr;
        }

        if (event_index_integer < 0) {
            event_index_integer += patient_length;
        }

        if (event_index_integer < 0 || event_index_integer >= patient_length) {
            return PyErr_Format(PyExc_IndexError,
                                "Provided an out of bound index to PatientEvents.__getitem__");
        }

        PyObject* obj = static_cast<PyObject*>(events + event_index_integer);

        Py_INCREF(obj);

        return obj;

    } else if (PySlice_Check(event_index)) {
        Py_ssize_t start, stop, step, slicelength;

        int err = PySlice_GetIndicesEx(event_index, patient_length, &start, &stop, &step, &slicelength);

        if (err == -1) {
            return nullptr;
        }

        PyObject* result = PyTuple_New(slicelength);

        for (Py_ssize_t i = 0; i < slicelength; i++) {
            PyObject* obj = static_cast<PyObject*>(events + start + i * step);

            Py_INCREF(obj);
            PyTuple_SET_ITEM(result, i, obj);
        }

        return result;
    } else {
        return PyErr_Format(PyExc_IndexError,
                            "Provided a unknown index to PatientEvents.__getitem__");
    }
}

void PatientEvents::dealloc() {
    for (int i =0 ; i < patient_length;i++) {
        Py_DECREF(static_cast<PyObject*>(events + i));
    }
    Py_XDECREF(length_obj);

    patient->decref();
}


PyObject* PatientEvents::str() {
    static_assert(sizeof(int64_t) == sizeof(long));
    int64_t patient_id_val = PyLong_AsLong(patient->patient_id);

    std::string debug_string =
        absl::StrCat("Events(patient_id=", patient_id_val,
                     ", len(events)=", patient_length, ")");

    PyObject* py_string =
        PyUnicode_FromStringAndSize(debug_string.data(), debug_string.size());

    if (py_string == nullptr) {
        return PyErr_Format(
            PyExc_RuntimeError,
            "Could not convert the database path to a string ...");
    }

    return py_string;
}

std::tuple<size_t, size_t, size_t, size_t> align_and_size_patient(int32_t num_properties, int32_t capacity) {
    constexpr size_t event_alignment = alignof(Event);
    constexpr size_t null_map_alignment = alignof(uint64_t);
    constexpr size_t property_alignment = alignof(PyObject*);

    size_t i = 0;

    i += sizeof(Patient);

    // Now align
    size_t extra_bytes = i % event_alignment;
    if (extra_bytes > 0) {
        i += ((ssize_t)event_alignment - (ssize_t)extra_bytes);
    }

    size_t event_offset = i;

    i += sizeof(Event) * capacity;

    // Now align
    extra_bytes = i % null_map_alignment;
    if (extra_bytes > 0) {
        i += ((ssize_t)null_map_alignment - (ssize_t)extra_bytes);
    }

    size_t null_map_offset = i;

    i += capacity * sizeof(uint64_t);


    // Now align
    extra_bytes = i % property_alignment;
    if (extra_bytes > 0) {
        i += ((ssize_t)property_alignment - (ssize_t)extra_bytes);
    }

    size_t property_offset = i;

    i += num_properties * capacity * sizeof(PyObject*);


    size_t total_size = i;


    return {event_offset, null_map_offset, property_offset, total_size};
}

Patient::Patient(PatientDatabase* pd, size_t c, char* data)
    : patient_database(pd), capacity(c), in_use(false) {

    auto info = align_and_size_patient(pd->get_num_properties(), capacity);

    static_assert(std::is_trivial<Event>::value);
    events = reinterpret_cast<Event*>(data + std::get<0>(info));

    static_assert(std::is_trivial<uint64_t>::value);
    null_map = reinterpret_cast<uint64_t*>(data + std::get<1>(info));

    static_assert(std::is_trivial<PyObject*>::value);
    saved_properties = reinterpret_cast<PyObject**>(data + std::get<2>(info));
}

Patient::~Patient() {}

PyObject* Patient::get_property(PyObject* property_name, Event* event_ptr) {
    // Needs to get the property
    ssize_t index = patient_database->get_property_index(property_name);

    if (index == -1) [[unlikely]] {
        // Does not contain
        return PyErr_Format(PyExc_AttributeError,
                            "Could not find key %U in meds_reader.Event",
                            property_name);
    }

    return get_property(index, event_ptr);
};

PyObject* Patient::get_property(size_t index, Event* event_ptr) {
    size_t event_index = event_ptr - events;

    if (properties_initialized.test(index) == 0) {
        patient_database->get_property_data(index, patient_offset, patient_length, saved_properties + capacity * index);
        properties_initialized.set(index);
    }

    PyObject* res = saved_properties[capacity * index + event_index];

    if (res == nullptr) {
        Py_RETURN_NONE;
    } else {
        Py_INCREF(res);
        return res;
    }
};

uint64_t Patient::get_null_map(Event* event_ptr) {
    size_t event_index = event_ptr - events;

    if (!null_map_initialized) {
        patient_database->get_null_map(patient_offset, patient_length, null_map);
        null_map_initialized = true;
    }

    return null_map[event_index];
}

Patient* Patient::create(PatientDatabase* pd, size_t capacity) {

    constexpr size_t property_alignment = alignof(PyObject*);
    constexpr size_t event_alignment = alignof(Event);
    constexpr size_t patient_alignment = alignof(Patient);
    constexpr size_t null_map_alignment = alignof(uint64_t);

    size_t common_alignment = std::lcm(null_map_alignment, std::lcm(
        property_alignment, std::lcm(event_alignment, patient_alignment)));

    if (common_alignment > alignof(max_align_t)) {
        throw std::runtime_error("This should never happen");
    }

    auto info = align_and_size_patient(pd->get_num_properties(), capacity);

    void* data =
        calloc(1, std::get<3>(info));
    Patient* casted_data = new (data) Patient(pd, capacity, (char*)data);
    return casted_data;
}


void Patient::init(int32_t po, int32_t pl, PyObject* pid_object) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);

    counter = 1;

    patient_database->incref();

    patient_offset = po;
    patient_length = pl;
    patient_id = pid_object;
    Py_INCREF(patient_id);

    in_use = true;
    properties_initialized.reset();
    null_map_initialized = false;

    events_obj.init(this, events, patient_length);
}

inline PyObject* Patient::get_patient_id(void*) {
    Py_INCREF(patient_id);
    return patient_id;
}

inline PyObject* Patient::get_events(void*) {
    PyObject* result = static_cast<PyObject*>(&events_obj);
    Py_INCREF(result);
    return result;
}

void Patient::dealloc() {
    if (!in_use) {
        throw std::runtime_error("How can a patient not in use get deallocated?");
    }
    Py_DECREF(patient_id);
    Py_DECREF(static_cast<PyObject*>(&events_obj));

    for (size_t p_index = 0; p_index < patient_database->get_num_properties(); p_index++) {
        if (!properties_initialized.test(p_index)) {
            continue;
        }

        for (int32_t i = 0; i < patient_length; i++) {
            PyObject*& obj = saved_properties[p_index * capacity + i];
            if (obj != nullptr) {
                Py_DECREF(obj);
                obj = nullptr;
            }
        }
    }

    decref();
}

void Patient::delete_self() {
    in_use = false;
    patient_database->decref();
}

PyObject* Patient::str() {
    static_assert(sizeof(int64_t) == sizeof(long));
    int64_t patient_id_val = PyLong_AsLong(patient_id);

    std::string debug_string =
        absl::StrCat("Patient(patient_id=", patient_id_val,
                     ", len(events)=", patient_length, ")");

    PyObject* py_string =
        PyUnicode_FromStringAndSize(debug_string.data(), debug_string.size());

    if (py_string == nullptr) {
        return PyErr_Format(
            PyExc_RuntimeError,
            "Could not convert the database path to a string ...");
    }

    return py_string;
}

PyObject* Patient::create_event_property_iterator(Event* event) {
    size_t desired_index = event_property_iterators.size();

    for (size_t i = 0; i < event_property_iterators.size(); i++) {
        if (!event_property_iterators[i].in_use) {
            desired_index = i;
        }
    }

    if (desired_index == event_property_iterators.size()) {
        event_property_iterators.emplace_back();
    }

    event_property_iterators[desired_index].init(this, event);

    return static_cast<PyObject*>(event_property_iterators.data() + desired_index);
}

void Event::init(Patient* p) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);
    patient = p;
    patient->incref();
}

void Event::dealloc() {
    patient->decref();
}

inline PyObject* Event::getattro(PyObject* key) {
    Py_INCREF(key);
    PyObjectWrapper key_wrapper(key);
    PyUnicode_InternInPlace(&key_wrapper.ref);

    return patient->get_property(key_wrapper.borrow(), this);
}

PyObject* Event::iter() { return patient->create_event_property_iterator(this); }

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

EventPropertyIterator::EventPropertyIterator() {
    in_use = false;
}


void EventPropertyIterator::init(Patient* pd, Event* e) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);
    patient = pd;
    event = e;

    in_use = true;

    patient->incref();

    current_index = patient->get_null_map(e);
}

void EventPropertyIterator::dealloc() { in_use = false; patient->decref(); }

inline PyObject* EventPropertyIterator::next() {
    if (current_index == 0) {
        return PyErr_Format(PyExc_StopIteration,
                            "Exceeded the number of properties in events");
    } else {
        static_assert(sizeof(uint64_t) == sizeof(unsigned long));
        int num_zeros = __builtin_ctzl(current_index);
        uint64_t mask = 1;
        mask <<= (uint64_t)num_zeros;
        current_index &= ~mask;

        PyObject* property_name =
            patient->patient_database->get_property_name(num_zeros);

        PyObject* property = patient->get_property(num_zeros, event);

        PyObject* result = PyTuple_New(2);
        PyTuple_SET_ITEM(result, 0, property_name);
        PyTuple_SET_ITEM(result, 1, property);

        return result;
    }
}

PyObject* EventPropertyIterator::iter() {
    Py_INCREF(this);
    return this;
}

PatientDatabase::PatientDatabase(std::string_view dir)
    : root_directory(dir),
      patient_id_file(root_directory / "patient_id"),
      length_file(root_directory / "meds_reader.length") {
    {
        PyObject_Init(static_cast<PyObject*>(this), &Type);
        PyObjectWrapper pyarrow{PyImport_ImportModule("pyarrow")};
        if (pyarrow.borrow() == nullptr) {
            throw std::runtime_error("Could not import pyarrow");
        }

        {
            std::ifstream version_file(root_directory / "meds_reader.version");
            int version;
            version_file >> version;

            if (version != CURRENT_BINARY_VERSION) {
                throw std::runtime_error(
                    "The file you are trying to read has a binary version of " +
                    std::to_string(version) +
                    " while this version of meds_reader only supports binary "
                    "version " +
                    std::to_string(CURRENT_BINARY_VERSION));
            }
        }

        MmapFile property_file(root_directory / "meds_reader.properties");

        std::string_view current = property_file.bytes();

        std::vector<PyObject*> property_name_pointers;

        py_properties = PyDict_New();

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

            if (PyDict_SetItem(py_properties.borrow(), property_name.borrow(),
                               dtype.borrow()) == -1) {
                throw std::runtime_error(
                    "Could not insert properties in map ...");
            }

            property_name_pointers.emplace_back(property_name.borrow());
            property_names.emplace_back(std::move(property_name));
        }

        property_accessors.resize(properties.size());

        property_map.emplace(property_name_pointers);
    }

    num_patients = patient_id_file.data<int64_t>().size();
}

PatientDatabase::~PatientDatabase() {
    for (Patient* patient : patients) {
        if (patient->in_use) {
            std::cerr << "Cannot delete database while still in use" << std::endl;
            abort();
        }
        delete patient;
    }
}

size_t PatientDatabase::get_num_properties() { return properties.size(); }

PyObject* PatientDatabase::get_property_name(size_t property_name_index) {
    return property_names[property_name_index].copy();
}

ssize_t PatientDatabase::get_property_index(PyObject* property_name) {
    PyUnicode_InternInPlace(&property_name);
    return property_map->get_index(property_name);
}

void PatientDatabase::get_property_data(
    size_t index, int32_t patient_offset, int32_t length, PyObject** result) {
    if (property_accessors[index] == nullptr) {
        property_accessors[index] = create_property_reader(
            root_directory, properties[index].first, properties[index].second);
    }
    property_accessors[index]->get_property_data(patient_offset, length, result);
}

void PatientDatabase::get_null_map(int32_t patient_offset,
                                                    int32_t length, uint64_t* result) {
    if (!null_map_reader) {
        null_map_reader =
            create_null_map_reader(root_directory, properties.size());
    }
    null_map_reader->get_null_map(patient_offset, length, result);
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

    uint32_t patient_length = get_patient_length(*patient_offset);

    size_t index_to_use = patients.size();

    for (size_t i = 0; i < patients.size(); i++) {
        if (!patients[i]->in_use) {
            index_to_use = i;
            break;
        }
    }

    if (index_to_use == patients.size()) {
        patients.push_back(Patient::create(this, next_pow2(patient_length)));
    }

    if (patients[index_to_use]->capacity < patient_length) {
        delete patients[index_to_use];
        patients[index_to_use] = Patient::create(this, next_pow2(patient_length));
    }

    patients[index_to_use]->init(*patient_offset, patient_length, patient_id);

    return patients[index_to_use];
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

PatientDatabaseIterator::PatientDatabaseIterator() {
    in_use = false;
}

void PatientDatabaseIterator::init(PatientDatabase* database) {
        PyObject_Init(static_cast<PyObject*>(this), &Type);

        patient_database = database;
        patient_database->incref();
        index = 0;
        in_use = true;
}

    void PatientDatabaseIterator::dealloc() {
        in_use = false;
        patient_database->decref();
    }

PyObject* PatientDatabaseIterator::next() {
     if (index >= patient_database->num_patients) {
            return PyErr_Format(PyExc_StopIteration,
                            "Exceeded the size of the PatientDatabase");
        }

        static_assert(sizeof(int64_t) == sizeof(long long));

        int64_t patient_id = patient_database->get_patient_id(index++);

        PyObject* result = PyLong_FromLongLong(patient_id);

        return result;
}

PyObject* PatientDatabase::iter() {
    size_t desired_index = patient_database_iterators.size();

    for (size_t i = 0; i < patient_database_iterators.size(); i++) {
        if (!patient_database_iterators[i].in_use) {
            desired_index = i;
        }
    }

    if (desired_index == patient_database_iterators.size()) {
        patient_database_iterators.emplace_back();
    }

    patient_database_iterators[desired_index].init(this);

    return patient_database_iterators.data() + desired_index;
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

    if (PyModule_AddType(m, &PatientDatabaseIterator::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &Patient::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &PatientEvents::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &PatientEventsIterator::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &Event::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &EventPropertyIterator::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
