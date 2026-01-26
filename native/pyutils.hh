// Utility helpers for bridging C++ with the Python C API.
//
// This header provides wrappers that translate C++ exceptions into
// Python errors, and RAII helpers for PyObject reference management.
#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <exception>
#include <functional>
#include <stdexcept>

#ifdef _MSC_VER
#define FORCE_INLINE __forceinline
#else
#define FORCE_INLINE inline __attribute__((always_inline))
#endif

// Returns a null pointer to signal a Python error.
inline PyObject* return_error(PyObject**) { return nullptr; }

// Returns -1 to signal a Python error.
inline Py_ssize_t return_error(Py_ssize_t*) { return -1; }

template <typename T, typename R, typename... Args>
// Invokes a member function and translates C++ exceptions to Python errors.
FORCE_INLINE R convert(R (T::*mf)(Args...),
                                                PyObject* obj, Args&&... args) {
#ifndef NDEBUG
    if (obj->ob_type != &T::Type) [[unlikely]] {
        throw std::runtime_error("Invalid type when calling function?");
    }
#endif
    try {
        T* sure = static_cast<T*>(obj);
        return std::invoke(mf, sure, std::forward<Args>(args)...);
    } catch (std::exception& e) {
        PyErr_Format(PyExc_RuntimeError, "%s", e.what());
        return return_error(static_cast<R*>(nullptr));
    }
}

template <typename R, typename... Args>
// Invokes a free function and translates C++ exceptions to Python errors.
FORCE_INLINE R convert(R (*mf)(Args...),
                                                Args&&... args) {
    try {
        return std::invoke(mf, std::forward<Args>(args)...);
    } catch (std::exception& e) {
        PyErr_Format(PyExc_RuntimeError, "%s", e.what());
        return return_error(static_cast<R*>(nullptr));
    }
}

template <typename T, typename... Args>
// Invokes a member function returning void with error translation.
FORCE_INLINE void convert_void(void (T::*mf)(Args...),
                                                        PyObject* obj,
                                                        Args&&... args) {
    if (obj->ob_type != &T::Type) {
        throw std::runtime_error("Invalid type when calling function?");
    }
    try {
        T* sure = static_cast<T*>(obj);
        std::invoke(mf, sure, std::forward<Args>(args)...);
    } catch (std::exception& e) {
        PyErr_Format(PyExc_RuntimeError, "%s", e.what());
    }
}

template <auto actual_mf, typename T, typename R, typename... Args>
// Wraps a void-returning member function for Python C API calls.
decltype(auto) helper(R (T::*mf)(Args...),
                      std::enable_if_t<std::is_void<R>::value>*) {
    return [](PyObject* arg, Args... args) -> void {
        convert_void(actual_mf, arg, std::forward<Args>(args)...);
    };
}

template <auto actual_mf, typename T, typename R, typename... Args>
// Wraps a value-returning member function for Python C API calls.
decltype(auto) helper(R (T::*mf)(Args...),
                      std::enable_if_t<!std::is_void<R>::value>*) {
    return [](PyObject* arg, Args... args) -> R {
        return convert(actual_mf, arg, std::forward<Args>(args)...);
    };
}

template <auto actual_mf, typename R, typename... Args>
// Wraps a value-returning free function for Python C API calls.
decltype(auto) helper(R (*mf)(Args...),
                      std::enable_if_t<!std::is_void<R>::value>*) {
    return [](Args... args) -> R {
        return convert(actual_mf, std::forward<Args>(args)...);
    };
}

template <auto mf>
// Converts a function pointer into a Python-compatible trampoline.
decltype(auto) convert_to_cfunc() {
    return helper<mf>(mf, nullptr);
}

struct PyObjectWrapper {
    // Initializes with a null Python reference.
    PyObjectWrapper() : ref(nullptr) {}
    // Takes ownership of a Python reference.
    explicit PyObjectWrapper(PyObject* obj) : ref(obj) {}

    PyObjectWrapper(const PyObjectWrapper& other) = delete;

    // Moves ownership of the Python reference.
    PyObjectWrapper(PyObjectWrapper&& other) {
        ref = other.ref;
        other.ref = nullptr;
    }

    // Replaces the owned reference with a new object.
    PyObjectWrapper& operator=(PyObject* obj) {
        Py_XDECREF(ref);
        ref = obj;
        return *this;
    }

    // Moves ownership from another wrapper.
    PyObjectWrapper& operator=(PyObjectWrapper&& other) noexcept {
        std::swap(ref, other.ref);
        return *this;
    }

    // Returns the borrowed reference without modifying refcounts.
    PyObject* borrow() { return ref; }

    // Returns a new owned reference to the object.
    PyObject* copy() {
        Py_INCREF(ref);
        return ref;
    }

    // Releases ownership and returns the raw pointer.
    PyObject* steal() {
        PyObject* result = ref;
        ref = nullptr;
        return result;
    }

    // Decrements the owned reference on destruction.
    ~PyObjectWrapper() { Py_XDECREF(ref); }

    PyObject* ref;
};
