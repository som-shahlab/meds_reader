cc_library(
    name = "streamvbyte",
    srcs = [
        "src/streamvbyte_encode.c",
        "src/streamvbyte_decode.c",
        "src/streamvbyte_zigzag.c",
        "src/streamvbytedelta_encode.c",
        "src/streamvbytedelta_decode.c",
        "src/streamvbyte_0124_encode.c",
        "src/streamvbyte_0124_decode.c",
    ],
    hdrs = glob(["include/*.h", "src/*"]),
    includes = ["include"],
    visibility = ["//visibility:public"],
    local_defines = select({
        "@bazel_tools//src/conditions:windows": ["__restrict__=__restrict"],
        "//conditions:default": [],
    }),
)