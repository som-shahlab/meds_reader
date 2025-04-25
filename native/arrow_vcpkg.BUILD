package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "arrow_vcpkg",
    srcs = glob(["mpir_x64-windows-static/lib/**/mpir.lib"]),
    hdrs = glob(["mpir_x64-windows-static/include/**/*.h"]),
    includes = glob(["mpir_x64-windows-static/include/"]),
    visibility = ["//visibility:public"],
    linkstatic = 1
)