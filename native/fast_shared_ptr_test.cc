// Tests fast_shared_ptr behavior.
//
// This file exercises construction, copying, and destruction to verify
// refcount updates and lifetime semantics of fast_shared_ptr.
#include "fast_shared_ptr.hh"

#include <iostream>

class Whatever : public fast_shared_ptr_object<Whatever> {
   public:
    // Initializes the test object with a value.
    Whatever(int a) : arg(a) {
        std::cout << "Constructing whatever " << arg << std::endl;
    }

    // Logs destruction for lifetime checks.
    ~Whatever() { std::cout << "Deconstructing whatever " << arg << std::endl; }

    int arg;
};

// Exercises fast_shared_ptr lifetime behavior.
int main() {
    fast_shared_ptr<Whatever> helper;

    std::cout << "Hellow world" << std::endl;
    {
        fast_shared_ptr<Whatever> obj = make_fast_shared_ptr<Whatever>(42);

        std::cout << "Created " << obj->arg << std::endl;
        helper = obj->shared_from_this();
        std::cout << "Copied " << helper->arg << std::endl;
    }
    std::cout << "Ending" << std::endl;
}
