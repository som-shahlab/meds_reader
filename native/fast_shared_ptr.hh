#pragma once

#include <cstddef>
#include <iostream>
#include <utility>

template <typename T>
class fast_shared_ptr;

template <typename T>
class fast_shared_ptr_object {
   public:
    fast_shared_ptr_object() : counter(1) {}

    fast_shared_ptr<T> shared_from_this();

    void decref() {
        counter--;
        if (counter < 0) {
            throw std::runtime_error("Counter is less than zero");
        }

        if (counter == 0) {
            static_cast<T*>(this)->delete_self();
        }
    }

    void incref() { counter++; }

   protected:
    friend class fast_shared_ptr<T>;

    void delete_self() { delete static_cast<T*>(this); }

    size_t counter;

   private:
};

template <typename T>
class fast_shared_ptr {
   public:
    fast_shared_ptr() { obj = nullptr; }

    explicit fast_shared_ptr(fast_shared_ptr_object<T>* o) : obj(o) {
        obj->counter++;
    }

    fast_shared_ptr(const fast_shared_ptr<T>& other) {
        obj = other.obj;
        obj->counter++;
    }

    fast_shared_ptr(fast_shared_ptr<T>&& other) {
        obj = other.obj;
        other.obj = nullptr;
    }

    fast_shared_ptr<T>& operator=(const fast_shared_ptr<T>& other) {
        return *this = fast_shared_ptr<T>(other);
    }

    fast_shared_ptr<T>& operator=(fast_shared_ptr<T>&& other) noexcept {
        std::swap(obj, other.obj);
        return *this;
    }

    T* get() const { return static_cast<T*>(obj); }

    T* operator->() const { return get(); }

    T& operator*() const { return *get(); }

    ~fast_shared_ptr() {
        if (obj != nullptr) {
            obj->decref();
        }
    }

    void reset() { *this = fast_shared_ptr<T>(); }

   private:
    fast_shared_ptr_object<T>* obj;
};

template <class T, class... Args>
fast_shared_ptr<T> make_fast_shared_ptr(Args&&... args) {
    T* obj = new T(std::forward<Args>(args)...);
    auto result = fast_shared_ptr<T>(obj);
    obj->counter--;
    return result;
}

template <typename T>
fast_shared_ptr<T> fast_shared_ptr_object<T>::shared_from_this() {
    return fast_shared_ptr<T>(this);
}
