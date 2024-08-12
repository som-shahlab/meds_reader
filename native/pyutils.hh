#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <exception>
#include <functional>
#include <stdexcept>

inline PyObject* return_error(PyObject**) { return nullptr; }

inline long return_error(long*) { return -1; }

template <typename T, typename R, typename... Args>
__attribute__((always_inline)) inline R convert(R (T::*mf)(Args...),
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
__attribute__((always_inline)) inline R convert(R (*mf)(Args...),
                                                Args&&... args) {
    try {
        return std::invoke(mf, std::forward<Args>(args)...);
    } catch (std::exception& e) {
        PyErr_Format(PyExc_RuntimeError, "%s", e.what());
        return return_error(static_cast<R*>(nullptr));
    }
}

template <typename T, typename... Args>
__attribute__((always_inline)) inline void convert_void(void (T::*mf)(Args...),
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
decltype(auto) helper(R (T::*mf)(Args...),
                      std::enable_if_t<std::is_void<R>::value>*) {
    return [](PyObject* arg, Args... args) -> void {
        convert_void(actual_mf, arg, std::forward<Args>(args)...);
    };
}

template <auto actual_mf, typename T, typename R, typename... Args>
decltype(auto) helper(R (T::*mf)(Args...),
                      std::enable_if_t<!std::is_void<R>::value>*) {
    return [](PyObject* arg, Args... args) -> R {
        return convert(actual_mf, arg, std::forward<Args>(args)...);
    };
}

template <auto actual_mf, typename R, typename... Args>
decltype(auto) helper(R (*mf)(Args...),
                      std::enable_if_t<!std::is_void<R>::value>*) {
    return [](Args... args) -> R {
        return convert(actual_mf, std::forward<Args>(args)...);
    };
}

template <auto mf>
decltype(auto) convert_to_cfunc() {
    return helper<mf>(mf, nullptr);
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

    PyObjectWrapper& operator=(PyObjectWrapper&& other) noexcept {
        std::swap(ref, other.ref);
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
