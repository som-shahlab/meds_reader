Supported Platforms/Data Types
===============================

Supported Platforms
-------------------

The following platforms are currently supported:
  - Linux x86_64
  - MacOS x86_64
  - MacOS ARM

We plan on adding support for Windows x86_64 in the future.


Data Types
-----------

MEDS is a very flexible format in that Events can have custom properties of any pyarrow type.

However, meds_reader currently only supports some pyarrow types.


.. role:: python(code)
   :language: python
   :class: highlight

The following types are currently supported:
   - :python:`pyarrow.string()`
   - :python:`pyarrow.large_string()`
   - :python:`pyarrow.timestamp('us')`
   - :python:`pyarrow.float32()`
   - :python:`pyarrow.float64()`
   - :python:`pyarrow.*int*()`


If you need support for additional types, please create an issue on GitHub.
