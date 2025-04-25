def _impl(repository_ctx):
  repository_ctx.execute(["git", "clone", "https://github.com/microsoft/vcpkg.git"])
  repository_ctx.execute(["./vcpkg/bootstrap-vcpkg.bat"])
  repository_ctx.execute(["./vcpkg/vcpkg.exe", "install", "arrow:x64-windows-static-md"], timeout=6000)

  repository_ctx.file("BUILD", """
cc_library(
  name="arrow",
    hdrs = glob(["vcpkg/packages/arrow_x64-windows-static-md/include/**/*.h"]),
    includes =  ["vcpkg/packages/arrow_x64-windows-static-md/include"],
    srcs = glob([
      "vcpkg/packages/arrow_x64-windows-static-md/lib/*.lib",
      "vcpkg/packages/openssl_x64-windows-static-md/lib/*.lib",
      "vcpkg/packages/thrift_x64-windows-static-md/lib/*.lib",
      "vcpkg/packages/lz4_x64-windows-static-md/lib/*.lib",
      "vcpkg/packages/snappy_x64-windows-static-md/lib/*.lib",
      "vcpkg/packages/brotli_x64-windows-static-md/lib/*.lib",
    ]),
    defines = ['ARROW_STATIC', 'PARQUET_STATIC'],
    visibility = ["//visibility:public"],
    linkstatic = 1,
    linkopts = ["-DEFAULTLIB:shell32.lib", "-DEFAULTLIB:ole32.lib"]
)

""")

load_arrow_vcpkg = repository_rule(
    implementation=_impl)