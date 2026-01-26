// Lightweight ref-counted pointer utilities.
//
// This header defines a small refcounted pointer type optimized for the
// meds_reader native objects. It provides shared ownership without the
// overhead of std::shared_ptr and integrates with custom delete paths.
#pragma once

#include <cstddef>
#include <iostream>
#include <utility>

template <typename T>
class fast_shared_ptr;

template <typename T>
class fast_shared_ptr_object {
   public:
    // Initializes the refcount to 1 for self-managed ownership.
    fast_shared_ptr_object() : counter(1) {}

    // Returns a fast_shared_ptr that shares ownership of this object.
    fast_shared_ptr<T> shared_from_this();

    // Decrements the refcount and deletes the object when it reaches zero.
    void decref() {
        counter--;
        if (counter < 0) {
            throw std::runtime_error("Counter is less than zero");
        }

        if (counter == 0) {
            static_cast<T*>(this)->delete_self();
        }
    }

    // Increments the refcount.
    void incref() { counter++; }

   protected:
    friend class fast_shared_ptr<T>;

    // Deletes the object with the correct derived type.
    void delete_self() { delete static_cast<T*>(this); }

    size_t counter;

   private:
};

template <typename T>
class fast_shared_ptr {
   public:
    // Constructs an empty smart pointer.
    fast_shared_ptr() { obj = nullptr; }

    // Takes ownership of a raw pointer and increments the refcount.
    explicit fast_shared_ptr(fast_shared_ptr_object<T>* o) : obj(o) {
        obj->counter++;
    }

    // Copies ownership from another fast_shared_ptr.
    fast_shared_ptr(const fast_shared_ptr<T>& other) {
        obj = other.obj;
        obj->counter++;
    }

    // Moves ownership from another fast_shared_ptr.
    fast_shared_ptr(fast_shared_ptr<T>&& other) {
        obj = other.obj;
        other.obj = nullptr;
    }

    // Copies ownership from another fast_shared_ptr.
    fast_shared_ptr<T>& operator=(const fast_shared_ptr<T>& other) {
        return *this = fast_shared_ptr<T>(other);
    }

    // Moves ownership from another fast_shared_ptr.
    fast_shared_ptr<T>& operator=(fast_shared_ptr<T>&& other) noexcept {
        std::swap(obj, other.obj);
        return *this;
    }

    // Returns the managed object pointer.
    T* get() const { return static_cast<T*>(obj); }

    // Provides pointer-like access to the managed object.
    T* operator->() const { return get(); }

    // Provides reference-like access to the managed object.
    T& operator*() const { return *get(); }

    // Releases a reference on destruction.
    ~fast_shared_ptr() {
        if (obj != nullptr) {
            obj->decref();
        }
    }

    // Resets to an empty pointer.
    void reset() { *this = fast_shared_ptr<T>(); }

   private:
    fast_shared_ptr_object<T>* obj;
};

template <class T, class... Args>
// Constructs a fast_shared_ptr with forwarded arguments.
fast_shared_ptr<T> make_fast_shared_ptr(Args&&... args) {
    T* obj = new T(std::forward<Args>(args)...);
    auto result = fast_shared_ptr<T>(obj);
    obj->counter--;
    return result;
}

template <typename T>
// Returns a fast_shared_ptr that shares ownership of this object.
fast_shared_ptr<T> fast_shared_ptr_object<T>::shared_from_this() {
    return fast_shared_ptr<T>(this);
}
