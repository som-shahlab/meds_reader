load(
    "@rules_foreign_cc//foreign_cc:defs.bzl", "cmake", "configure_make", "make"
)

load("@pybind11_bazel//:build_defs.bzl", "pybind_extension", "pybind_library")

cc_binary(
    name="simple_test",
    srcs=["simple_test.cc"],
)

cc_library(
    name="create_database",
    srcs=["create_database.cc", "pdqsort.h"],
    hdrs=["create_database.hh"],
    deps=[
        ":dtype",
        ":arrow",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:flat_hash_map",
        "@concurrentqueue",
        ":zstd",
        ":streamvbyte",
        ":binary_version",
        ":mmap_file",
    ],
)

cc_library(
    name="binary_version",
    hdrs=["binary_version.hh"],
)

cc_library(
    name="dtype",
    hdrs=["dtype.hh"],
)

cc_library(
    name="fast_shared_ptr",
    hdrs=["fast_shared_ptr.hh"],
)

cc_library(
    name="perfect_hash",
    hdrs=["perfect_hash.hh"],
)

pybind_library(
    name="pyutils",
    hdrs=["pyutils.hh"],
)

pybind_library(
    name="mmap_file",
    hdrs=["mmap_file.hh"],
    deps=[
        "@com_google_absl//absl/types:span",
    ]
)

cc_library(
    name="property_reader",
    hdrs=["property_reader.hh"],
    srcs=["property_reader.cc"],
    deps=[":pyutils", ":dtype", ":zstd", ":streamvbyte",
        "@com_google_absl//absl/time", "@com_google_absl//absl/types:span", ":mmap_file"]
)

cc_binary(
    name="fast_shared_ptr_test",
    srcs=["fast_shared_ptr_test.cc"],
    deps=[
        ":fast_shared_ptr",
    ],
)

cc_binary(
    name="meds_reader_convert",
    srcs=["meds_reader_convert.cc"],
    deps=[
        ":create_database",
        "@CLI11//:cli11",
    ],
)

pybind_extension(
    name="_meds_reader",
    srcs=[
        "meds_reader.cc",
    ],
    linkopts = select({
        "@platforms//os:osx": ["-undefined", "dynamic_lookup"],
        "//conditions:default": [],
    }),
    # copts = ["-fno-inline"],
    deps=[
        "@com_google_absl//absl/container:flat_hash_map",
        ":pyutils",
        ":perfect_hash",
        ":dtype",
        ":property_reader",
        ":fast_shared_ptr",
        ":binary_version",
    ],
)

cmake(
    name = "arrow",
    cache_entries = {
        "CMAKE_INSTALL_LIBDIR": "lib",
        "CMAKE_TOOLCHAIN_FILE": "",
        "CMAKE_C_FLAGS": "-fPIC -I/usr/include -fvisibility=hidden",
        "CMAKE_CXX_FLAGS": "-fPIC -I/usr/include -fvisibility=hidden",
        "EP_COMMON_CMAKE_ARGS": "-DWITH_OPENSSL=OFF",
        "ARROW_BUILD_SHARED": "OFF",
        "ARROW_BUILD_STATIC": "ON",
        "ARROW_BUILD_TESTS": "OFF",
        "ARROW_PARQUET": "ON",
        "ARROW_JEMALLOC": "OFF",
        "ARROW_IPC": "OFF",
        "ARROW_DEPENDENCY_SOURCE": "BUNDLED",
        "ARROW_WITH_SNAPPY": "ON",
        "ARROW_WITH_ZSTD": "ON",
        "CMAKE_OSX_DEPLOYMENT_TARGET": "12",
    },
    tags = ["requires-network"],
    generate_args = ["-GNinja", "-DCMAKE_RANLIB=/usr/bin/ranlib"],
    working_directory="cpp",
    lib_source = "@arrow//:all",
    out_static_libs = ["libparquet.a", "libarrow.a", "libarrow_bundled_dependencies.a"],
    linkopts = ["-pthread"],
)

cmake(
    name = "zstd",
    cache_entries = {
        "CMAKE_C_FLAGS": "-fPIC -fvisibility=hidden",
        "CMAKE_CXX_FLAGS": "-fPIC -fvisibility=hidden",
        "CMAKE_INSTALL_LIBDIR": "lib",
	    "CMAKE_OSX_DEPLOYMENT_TARGET": "12",

        "ZSTDLIB_VISIBLE": "hidden",
        "ZSTDERRORLIB_VISIBLE": "hidden",
        "ZDICTLIB_VISIBLE": "hidden",
        "ZSTDLIB_STATIC_API": "hidden",
        "ZDICTLIB_STATIC_API": "hidden",
    },
    working_directory="build/cmake",
    lib_source = "@zstd//:all",
    out_static_libs = ["libzstd.a"],
    generate_args = ["-GNinja"],
)

cmake(
    name = "streamvbyte",
    cache_entries = {
        "CMAKE_C_FLAGS": "-fPIC -fvisibility=hidden",
        "CMAKE_CXX_FLAGS": "-fPIC -fvisibility=hidden",
        "CMAKE_INSTALL_LIBDIR": "lib",
        "CMAKE_OSX_DEPLOYMENT_TARGET": "12",
    },
    lib_source = "@streamvbyte//:all",
    out_static_libs = ["libstreamvbyte.a"],
    generate_args = ["-GNinja"],
)
