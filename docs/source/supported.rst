Supported Platforms/Data Types
===============================

As a C++ extension, meds_reader cannot support every possible platform and data type.

Platforms
----------

The following platforms are currently supported:
  - Linux x86_64
  - MacOS x86_64
  - MacOS ARM

We plan on adding support for Windows x86_64 in the future.


Data Types
-----------

MEDS is a very flexible format in that Events can have arbitrary custom properties of arbitrary type.

meds_reader only supports a finite subset of additional types.

.. role:: python(code)
   :language: python
   :class: highlight

The following types are currently supported:
   - :python:`pyarrow.string()`

We plan on adding support for all numeric types.