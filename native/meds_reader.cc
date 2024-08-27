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

struct Subject;
struct SubjectDatabase;

struct SubjectDatabaseIterator : public PyObject {
    static PyTypeObject Type;

    SubjectDatabaseIterator();

    void init(SubjectDatabase* database);

    // Python methods
    //-----------------------------------------
    void dealloc();
    PyObject* next();
    //-----------------------------------------

    bool in_use;
    SubjectDatabase* subject_database;
    int32_t index;
};

PyTypeObject SubjectDatabaseIterator::Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        "meds_reader.SubjectDatabaseIterator",
    .tp_basicsize = sizeof(SubjectDatabaseIterator),
    .tp_itemsize = 0,
    .tp_dealloc = convert_to_cfunc<&SubjectDatabaseIterator::dealloc>(),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("SubjectDatabaseIterator"),
    .tp_iternext = convert_to_cfunc<&SubjectDatabaseIterator::next>(),
    .tp_init = nullptr,
    .tp_new = nullptr,
};

struct SubjectDatabase : public PyObject,
                         fast_shared_ptr_object<SubjectDatabase> {
    static PyTypeObject Type;

    SubjectDatabase(std::string_view dir);
    ~SubjectDatabase();

    size_t get_num_properties();
    PyObject* get_property_name(size_t property_index);
    ssize_t get_property_index(PyObject* property_name);
    size_t get_property_data(size_t index, int32_t subject_offset,
                             int32_t length, PyObject** result,
                             PyObject** allocated);
    void get_null_map(int32_t subject_offset, int32_t length, uint64_t* result);
    int64_t get_subject_id(int32_t subject_offset) const;
    uint32_t get_subject_length(int32_t subject_offset) const;
    std::optional<int32_t> get_subject_offset(int64_t subject_id);

    // Python methods
    //-----------------------------------------
    static PyObject* create(PyTypeObject* type, PyObject* args, PyObject* kwds);

    PyObject* get_properties(void*);
    Py_ssize_t length();
    __attribute__((always_inline)) PyObject* subscript(PyObject* subject_id);
    PyObject* reduce(PyObject* Py_UNUSED(unused));

    PyObject* str();
    PyObject* iter();
    void dealloc() { decref(); }
    //-----------------------------------------

    const std::filesystem::path root_directory;

    int num_subjects;

    std::vector<std::pair<std::string, DataType>> properties;
    std::vector<std::unique_ptr<PropertyReader>> property_accessors;
    std::unique_ptr<NullMapReader> null_map_reader;

    std::vector<PyObjectWrapper> property_names;
    std::optional<PerfectHashMap<PyObject>> property_map;

    MmapFile subject_id_file;
    MmapFile length_file;

    std::optional<absl::flat_hash_map<int64_t, int32_t>> subject_offset_map;

    PyObjectWrapper py_properties;

    absl::InlinedVector<Subject*, 4> subjects;
    absl::InlinedVector<SubjectDatabaseIterator, 4> subject_database_iterators;
};

PyMappingMethods SubjectDatabaseMappingMethods = {
    .mp_length = convert_to_cfunc<&SubjectDatabase::length>(),
    .mp_subscript = convert_to_cfunc<&SubjectDatabase::subscript>(),
};

PyGetSetDef SubjectDatabaseGetSet[] = {
    {.name = "properties",
     .get = convert_to_cfunc<&SubjectDatabase::get_properties>()},
    {nullptr},
};

PyTypeObject SubjectDatabase::Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        "meds_reader.SubjectDatabase",
    .tp_basicsize = sizeof(SubjectDatabase),
    .tp_itemsize = 0,
    .tp_dealloc = convert_to_cfunc<&SubjectDatabase::dealloc>(),
    .tp_as_mapping = &SubjectDatabaseMappingMethods,
    .tp_str = convert_to_cfunc<&SubjectDatabase::str>(),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("Subject Database"),
    .tp_iter = convert_to_cfunc<&SubjectDatabase::iter>(),
    .tp_getset = SubjectDatabaseGetSet,
    .tp_init = nullptr,
    .tp_alloc = nullptr,
    .tp_new = convert_to_cfunc<&SubjectDatabase::create>(),
    .tp_free = nullptr,
};

struct SubjectEvents;
struct Event;
struct EventPropertyIterator;

struct SubjectEventsIterator : public PyObject {
    static PyTypeObject Type;

    SubjectEventsIterator();

    void init(SubjectEvents* events);

    // Python methods
    //-----------------------------------------
    void dealloc();
    __attribute__((always_inline)) PyObject* next();
    PyObject* iter();
    //-----------------------------------------

    Event* raw_events;
    SubjectEvents* events;
    int event_index;
    int32_t subject_length;
    bool in_use;
};

PyTypeObject SubjectEventsIterator::Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        "meds_reader.SubjectEventsIterator",
    .tp_basicsize = sizeof(SubjectEventsIterator),
    .tp_itemsize = 0,
    .tp_dealloc = convert_to_cfunc<&SubjectEventsIterator::dealloc>(),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("SubjectEventsIterator"),
    .tp_iter = convert_to_cfunc<&SubjectEventsIterator::iter>(),
    .tp_iternext = convert_to_cfunc<&SubjectEventsIterator::next>(),
    .tp_init = nullptr,
    .tp_new = nullptr,
};

struct SubjectEvents : public PyObject {
    static PyTypeObject Type;

    void init(Subject* subject, Event* events, int32_t subject_length);

    // Python methods
    //-----------------------------------------
    PyObject* str();
    void dealloc();
    __attribute__((always_inline)) PyObject* subscript(PyObject*);
    __attribute__((always_inline)) Py_ssize_t length();
    __attribute__((always_inline)) PyObject* iter();
    //-----------------------------------------

    Subject* subject;
    int subject_length;
    Event* events;
    PyObject* length_obj;
    absl::InlinedVector<SubjectEventsIterator, 4> iterators;
};

PyMappingMethods SubjectEventsMappingMethods = {
    .mp_length = convert_to_cfunc<&SubjectEvents::length>(),
    .mp_subscript = convert_to_cfunc<&SubjectEvents::subscript>(),
};

PyTypeObject SubjectEvents::Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        "meds_reader.SubjectEvents",
    .tp_basicsize = sizeof(SubjectEvents),
    .tp_itemsize = 0,
    .tp_dealloc = convert_to_cfunc<&SubjectEvents::dealloc>(),
    .tp_as_mapping = &SubjectEventsMappingMethods,
    .tp_str = convert_to_cfunc<&SubjectEvents::str>(),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("SubjectEvents"),
    .tp_iter = convert_to_cfunc<&SubjectEvents::iter>(),
    .tp_init = nullptr,
    .tp_alloc = nullptr,
    .tp_new = nullptr,
    .tp_free = nullptr,
};

struct EventPropertyIterator : public PyObject {
    static PyTypeObject Type;

    bool in_use;
    Subject* subject;
    Event* event;
    uint64_t current_index;

    EventPropertyIterator();
    void init(Subject* subject, Event* e);

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

struct Subject : public PyObject, fast_shared_ptr_object<Subject> {
    static PyTypeObject Type;

    static Subject* create(SubjectDatabase* pd, size_t capacity);

    void init(int32_t subject_offset, int32_t subject_length,
              PyObject* subject_id);

    SubjectDatabase* subject_database;
    const size_t capacity;

    int32_t subject_offset;
    int32_t subject_length;
    PyObject* subject_id;

    bool in_use;

    Event* events;
    uint64_t* null_map;
    bool null_map_initialized;
    PyObject** saved_properties;
    std::bitset<64> properties_initialized;

    size_t num_allocated;

    SubjectEvents events_obj;

    absl::InlinedVector<EventPropertyIterator, 4> event_property_iterators;

    // Python methods
    //-----------------------------------------
    PyObject* str();
    void dealloc();
    __attribute__((always_inline)) PyObject* get_subject_id(void*);
    __attribute__((always_inline)) PyObject* get_events(void*);
    //-----------------------------------------

    void delete_self();
    void operator delete(void* ptr) { free(ptr); }
    ~Subject();

   private:
    friend Event;
    friend EventPropertyIterator;

    Subject(SubjectDatabase* pd, size_t capacity, char* data);

    __attribute__((always_inline)) PyObject* get_property(
        PyObject* property_name, Event* event_ptr);
    __attribute__((always_inline)) PyObject* get_property(size_t property_index,
                                                          Event* event_ptr);
    uint64_t get_null_map(Event* event_ptr);
    PyObject* create_event_property_iterator(Event* event_ptr);
};

PyGetSetDef SubjectGetSet[] = {
    {.name = "subject_id", .get = convert_to_cfunc<&Subject::get_subject_id>()},
    {.name = "events", .get = convert_to_cfunc<&Subject::get_events>()},
    {nullptr},
};

PyTypeObject Subject::Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name = "meds_reader.Subject",
    .tp_basicsize = sizeof(Subject),
    .tp_itemsize = 0,
    .tp_dealloc = convert_to_cfunc<&Subject::dealloc>(),
    .tp_str = convert_to_cfunc<&Subject::str>(),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("Subject"),
    .tp_getset = SubjectGetSet,
    .tp_init = nullptr,
    .tp_alloc = nullptr,
    .tp_new = nullptr,
    .tp_free = nullptr,
};

struct Event : public PyObject {
    static PyTypeObject Type;

    Subject* subject;

    void init(Subject* subject);

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

SubjectEventsIterator::SubjectEventsIterator() { in_use = false; }

void SubjectEventsIterator::init(SubjectEvents* e) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);

    events = e;
    Py_INCREF(events);
    raw_events = e->events;
    subject_length = e->subject_length;

    event_index = 0;
}

void SubjectEventsIterator::dealloc() {
    in_use = false;
    Py_DECREF(events);
}

inline PyObject* SubjectEventsIterator::next() {
    if (event_index == subject_length) {
        return PyErr_Format(PyExc_StopIteration,
                            "Exceeded the number of properties in events");
    } else {
        PyObject* event = raw_events + event_index;
        event_index++;
        Py_INCREF(event);
        return event;
    }
}

PyObject* SubjectEventsIterator::iter() {
    Py_INCREF(this);
    return this;
}

void SubjectEvents::init(Subject* p, Event* e, int pl) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);

    subject = p;

    subject->incref();

    events = e;
    subject_length = pl;
    length_obj = nullptr;

    for (int i = 0; i < subject_length; i++) {
        events[i].init(subject);
    }
}

inline PyObject* SubjectEvents::iter() {
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

inline Py_ssize_t SubjectEvents::length() { return subject_length; }

inline PyObject* SubjectEvents::subscript(PyObject* event_index) {
    if (PyLong_Check(event_index)) {
        Py_ssize_t event_index_integer = PyLong_AsSsize_t(event_index);

        if (PyErr_Occurred()) {
            return nullptr;
        }

        if (event_index_integer < 0) {
            event_index_integer += subject_length;
        }

        if (event_index_integer < 0 || event_index_integer >= subject_length) {
            return PyErr_Format(
                PyExc_IndexError,
                "Provided an out of bound index to SubjectEvents.__getitem__");
        }

        PyObject* obj = static_cast<PyObject*>(events + event_index_integer);

        Py_INCREF(obj);

        return obj;

    } else if (PySlice_Check(event_index)) {
        Py_ssize_t start, stop, step, slicelength;

        int err = PySlice_GetIndicesEx(event_index, subject_length, &start,
                                       &stop, &step, &slicelength);

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
        return PyErr_Format(
            PyExc_IndexError,
            "Provided a unknown index to SubjectEvents.__getitem__");
    }
}

void SubjectEvents::dealloc() {
    for (int i = 0; i < subject_length; i++) {
        Py_DECREF(static_cast<PyObject*>(events + i));
    }
    Py_XDECREF(length_obj);

    subject->decref();
}

PyObject* SubjectEvents::str() {
    static_assert(sizeof(int64_t) == sizeof(long));
    int64_t subject_id_val = PyLong_AsLong(subject->subject_id);

    std::string debug_string =
        absl::StrCat("Events(subject_id=", subject_id_val,
                     ", len(events)=", subject_length, ")");

    PyObject* py_string =
        PyUnicode_FromStringAndSize(debug_string.data(), debug_string.size());

    if (py_string == nullptr) {
        return PyErr_Format(
            PyExc_RuntimeError,
            "Could not convert the database path to a string ...");
    }

    return py_string;
}

std::tuple<size_t, size_t, size_t, size_t> align_and_size_subject(
    int32_t num_properties, int32_t capacity) {
    constexpr size_t event_alignment = alignof(Event);
    constexpr size_t null_map_alignment = alignof(uint64_t);
    constexpr size_t property_alignment = alignof(PyObject*);

    size_t i = 0;

    i += sizeof(Subject);

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

    i += num_properties * capacity * sizeof(PyObject*) * 2;

    size_t total_size = i;

    return {event_offset, null_map_offset, property_offset, total_size};
}

Subject::Subject(SubjectDatabase* pd, size_t c, char* data)
    : subject_database(pd), capacity(c), in_use(false) {
    auto info = align_and_size_subject(pd->get_num_properties(), capacity);

    static_assert(std::is_trivial<Event>::value);
    events = reinterpret_cast<Event*>(data + std::get<0>(info));

    static_assert(std::is_trivial<uint64_t>::value);
    null_map = reinterpret_cast<uint64_t*>(data + std::get<1>(info));

    static_assert(std::is_trivial<PyObject*>::value);
    saved_properties = reinterpret_cast<PyObject**>(data + std::get<2>(info));
}

Subject::~Subject() {}

inline PyObject* Subject::get_property(PyObject* property_name,
                                       Event* event_ptr) {
    // Needs to get the property
    ssize_t index = subject_database->get_property_index(property_name);

    if (index == -1) [[unlikely]] {
        // Does not contain
        return PyErr_Format(PyExc_AttributeError,
                            "Could not find key %U in meds_reader.Event",
                            property_name);
    }

    return get_property(index, event_ptr);
};

inline PyObject* Subject::get_property(size_t index, Event* event_ptr) {
    size_t event_index = event_ptr - events;

    if (properties_initialized.test(index) == 0) {
        num_allocated += subject_database->get_property_data(
            index, subject_offset, subject_length,
            saved_properties + subject_length * index,
            saved_properties +
                subject_length * subject_database->get_num_properties() +
                num_allocated);
        properties_initialized.set(index);
    }

    PyObject* res = saved_properties[subject_length * index + event_index];

    if (res == nullptr) {
        Py_RETURN_NONE;
    } else {
        Py_INCREF(res);
        return res;
    }
};

uint64_t Subject::get_null_map(Event* event_ptr) {
    size_t event_index = event_ptr - events;

    if (!null_map_initialized) {
        subject_database->get_null_map(subject_offset, subject_length,
                                       null_map);
        null_map_initialized = true;
    }

    return null_map[event_index];
}

Subject* Subject::create(SubjectDatabase* pd, size_t capacity) {
    constexpr size_t property_alignment = alignof(PyObject*);
    constexpr size_t event_alignment = alignof(Event);
    constexpr size_t subject_alignment = alignof(Subject);
    constexpr size_t null_map_alignment = alignof(uint64_t);

    size_t common_alignment =
        std::lcm(null_map_alignment,
                 std::lcm(property_alignment,
                          std::lcm(event_alignment, subject_alignment)));

    if (common_alignment > alignof(max_align_t)) {
        throw std::runtime_error("This should never happen");
    }

    auto info = align_and_size_subject(pd->get_num_properties(), capacity);

    void* data = calloc(1, std::get<3>(info));
    Subject* casted_data = new (data) Subject(pd, capacity, (char*)data);
    return casted_data;
}

void Subject::init(int32_t po, int32_t pl, PyObject* pid_object) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);

    counter = 1;

    subject_database->incref();

    subject_offset = po;
    subject_length = pl;
    subject_id = pid_object;
    Py_INCREF(subject_id);

    in_use = true;
    properties_initialized.reset();
    null_map_initialized = false;

    num_allocated = 0;

    events_obj.init(this, events, subject_length);
}

inline PyObject* Subject::get_subject_id(void*) {
    Py_INCREF(subject_id);
    return subject_id;
}

inline PyObject* Subject::get_events(void*) {
    PyObject* result = static_cast<PyObject*>(&events_obj);
    Py_INCREF(result);
    return result;
}

void Subject::dealloc() {
    if (!in_use) {
        throw std::runtime_error(
            "How can a subject not in use get deallocated?");
    }
    Py_DECREF(subject_id);
    Py_DECREF(static_cast<PyObject*>(&events_obj));

    decref();
}

void Subject::delete_self() {
    for (size_t p_index = 0; p_index < subject_database->get_num_properties();
         p_index++) {
        if (!properties_initialized.test(p_index)) {
            continue;
        }
        memset(saved_properties + p_index * subject_length, 0,
               subject_length * sizeof(PyObject*));
    }

    PyObject** to_remove =
        saved_properties +
        subject_database->get_num_properties() * subject_length;
    for (size_t i = 0; i < num_allocated; i++) {
        Py_DECREF(to_remove[i]);
    }

    memset(to_remove, 0, num_allocated * sizeof(PyObject*));

    in_use = false;
    subject_database->decref();
}

PyObject* Subject::str() {
    static_assert(sizeof(int64_t) == sizeof(long));
    int64_t subject_id_val = PyLong_AsLong(subject_id);

    std::string debug_string =
        absl::StrCat("Subject(subject_id=", subject_id_val,
                     ", len(events)=", subject_length, ")");

    PyObject* py_string =
        PyUnicode_FromStringAndSize(debug_string.data(), debug_string.size());

    if (py_string == nullptr) {
        return PyErr_Format(
            PyExc_RuntimeError,
            "Could not convert the database path to a string ...");
    }

    return py_string;
}

PyObject* Subject::create_event_property_iterator(Event* event) {
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

    return static_cast<PyObject*>(event_property_iterators.data() +
                                  desired_index);
}

void Event::init(Subject* p) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);
    subject = p;
    subject->incref();
}

void Event::dealloc() { subject->decref(); }

inline PyObject* Event::getattro(PyObject* key) {
    Py_INCREF(key);
    PyObjectWrapper key_wrapper(key);
    PyUnicode_InternInPlace(&key_wrapper.ref);

    return subject->get_property(key_wrapper.borrow(), this);
}

PyObject* Event::iter() {
    return subject->create_event_property_iterator(this);
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

EventPropertyIterator::EventPropertyIterator() { in_use = false; }

void EventPropertyIterator::init(Subject* pd, Event* e) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);
    subject = pd;
    event = e;

    in_use = true;

    subject->incref();

    current_index = subject->get_null_map(e);
}

void EventPropertyIterator::dealloc() {
    in_use = false;
    subject->decref();
}

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
            subject->subject_database->get_property_name(num_zeros);

        PyObject* property = subject->get_property(num_zeros, event);

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

SubjectDatabase::SubjectDatabase(std::string_view dir)
    : root_directory(dir),
      subject_id_file(root_directory / "subject_id"),
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

    num_subjects = subject_id_file.data<int64_t>().size();
}

SubjectDatabase::~SubjectDatabase() {
    for (Subject* subject : subjects) {
        if (subject->in_use) {
            std::cerr << "Cannot delete database while still in use"
                      << std::endl;
            abort();
        }
        delete subject;
    }
}

size_t SubjectDatabase::get_num_properties() { return properties.size(); }

PyObject* SubjectDatabase::get_property_name(size_t property_name_index) {
    return property_names[property_name_index].copy();
}

ssize_t SubjectDatabase::get_property_index(PyObject* property_name) {
    PyUnicode_InternInPlace(&property_name);
    return property_map->get_index(property_name);
}

size_t SubjectDatabase::get_property_data(size_t index, int32_t subject_offset,
                                          int32_t length, PyObject** result,
                                          PyObject** allocated) {
    if (property_accessors[index] == nullptr) {
        property_accessors[index] = create_property_reader(
            root_directory, properties[index].first, properties[index].second);
    }
    return property_accessors[index]->get_property_data(subject_offset, length,
                                                        result, allocated);
}

void SubjectDatabase::get_null_map(int32_t subject_offset, int32_t length,
                                   uint64_t* result) {
    if (!null_map_reader) {
        null_map_reader =
            create_null_map_reader(root_directory, properties.size());
    }
    null_map_reader->get_null_map(subject_offset, length, result);
}

int64_t SubjectDatabase::get_subject_id(int32_t subject_offset) const {
    return subject_id_file.data<int64_t>()[subject_offset];
}

uint32_t SubjectDatabase::get_subject_length(int32_t subject_offset) const {
    return length_file.data<uint32_t>()[subject_offset];
}

std::optional<int32_t> SubjectDatabase::get_subject_offset(int64_t subject_id) {
    if (!subject_offset_map) {
        subject_offset_map.emplace();
        for (int i = 0; i < num_subjects; i++) {
            if (subject_offset_map->find(get_subject_id(i)) !=
                std::end(*subject_offset_map)) {
                throw std::runtime_error("Cannot make this work! " +
                                         std::to_string(i) + " " +
                                         std::to_string(get_subject_id(i)));
            }
            subject_offset_map->insert(std::make_pair(get_subject_id(i), i));
        }
    }

    auto iter = subject_offset_map->find(subject_id);
    if (iter == std::end(*subject_offset_map)) {
        return std::nullopt;
    } else {
        return iter->second;
    }
}

PyObject* SubjectDatabase::get_properties(void*) {
    return py_properties.copy();
}

PyObject* SubjectDatabase::create(PyTypeObject* type, PyObject* args,
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

    SubjectDatabase* database =
        new SubjectDatabase(std::string_view(path_str, path_size));

    return static_cast<PyObject*>(database);
}

Py_ssize_t SubjectDatabase::length() { return num_subjects; }

inline PyObject* SubjectDatabase::subscript(PyObject* subject_id) {
    if (!PyLong_Check(subject_id)) {
        return PyErr_Format(
            PyExc_TypeError,
            "SubjectDatabase only supports integer subject_ids");
    }

    static_assert(sizeof(long) == sizeof(int64_t));
    int64_t int_subject_id = PyLong_AsLong(subject_id);

    if (PyErr_Occurred()) {
        return nullptr;
    }

    std::optional<int32_t> subject_offset = get_subject_offset(int_subject_id);

    if (!subject_offset) {
        return PyErr_Format(PyExc_KeyError,
                            "SubjectDatabase did not have subject %d",
                            int_subject_id);
    }

    uint32_t subject_length = get_subject_length(*subject_offset);

    size_t index_to_use = subjects.size();

    for (size_t i = 0; i < subjects.size(); i++) {
        if (!subjects[i]->in_use) {
            index_to_use = i;
            break;
        }
    }

    if (index_to_use == subjects.size()) {
        subjects.push_back(Subject::create(this, next_pow2(subject_length)));
    }

    if (subjects[index_to_use]->capacity < subject_length) {
        delete subjects[index_to_use];
        subjects[index_to_use] =
            Subject::create(this, next_pow2(subject_length));
    }

    subjects[index_to_use]->init(*subject_offset, subject_length, subject_id);

    return subjects[index_to_use];
}

PyObject* SubjectDatabase::str() {
    std::string path = root_directory.string();

    std::string debug_string = absl::StrCat("SubjectDatabase(path=", path, ")");

    PyObject* py_string =
        PyUnicode_FromStringAndSize(debug_string.data(), debug_string.size());

    if (py_string == nullptr) {
        return PyErr_Format(
            PyExc_RuntimeError,
            "Could not convert the database path to a string ...");
    }

    return py_string;
}

SubjectDatabaseIterator::SubjectDatabaseIterator() { in_use = false; }

void SubjectDatabaseIterator::init(SubjectDatabase* database) {
    PyObject_Init(static_cast<PyObject*>(this), &Type);

    subject_database = database;
    subject_database->incref();
    index = 0;
    in_use = true;
}

void SubjectDatabaseIterator::dealloc() {
    in_use = false;
    subject_database->decref();
}

PyObject* SubjectDatabaseIterator::next() {
    if (index >= subject_database->num_subjects) {
        return PyErr_Format(PyExc_StopIteration,
                            "Exceeded the size of the SubjectDatabase");
    }

    static_assert(sizeof(int64_t) == sizeof(long long));

    int64_t subject_id = subject_database->get_subject_id(index++);

    PyObject* result = PyLong_FromLongLong(subject_id);

    return result;
}

PyObject* SubjectDatabase::iter() {
    size_t desired_index = subject_database_iterators.size();

    for (size_t i = 0; i < subject_database_iterators.size(); i++) {
        if (!subject_database_iterators[i].in_use) {
            desired_index = i;
        }
    }

    if (desired_index == subject_database_iterators.size()) {
        subject_database_iterators.emplace_back();
    }

    subject_database_iterators[desired_index].init(this);

    return subject_database_iterators.data() + desired_index;
}

struct PyModuleDef meds_reader_module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "meds_reader",
};

}  // namespace

PyMODINIT_FUNC PyInit__meds_reader(void) {
    PyDateTime_IMPORT;

    if (PyType_Ready(&SubjectDatabase::Type) < 0) {
        return NULL;
    }
    if (PyType_Ready(&Subject::Type) < 0) {
        return NULL;
    }
    if (PyType_Ready(&Event::Type) < 0) {
        return NULL;
    }

    PyObject* m = PyModule_Create(&meds_reader_module);
    if (m == NULL) {
        return NULL;
    }

    if (PyModule_AddType(m, &SubjectDatabase::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &SubjectDatabaseIterator::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &Subject::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &SubjectEvents::Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyModule_AddType(m, &SubjectEventsIterator::Type) < 0) {
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
