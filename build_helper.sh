#!/bin/sh

cd native
bazel build -c opt _meds_reader.so meds_reader_convert
cd ..

rm -f src/meds_reader/_meds_reader* src/meds_reader/meds_reader_convert*
cp native/bazel-bin/_meds_reader.so src/meds_reader/_meds_reader.so
cp native/bazel-bin/meds_reader_convert src/meds_reader/meds_reader_convert
