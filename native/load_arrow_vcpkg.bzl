# Clones vcpkg and installs Arrow for Windows builds.
def _execute_or_fail(repository_ctx, arguments, timeout = 600, attempts = 1):
  result = None
  for _ in range(attempts):
    result = repository_ctx.execute(arguments, timeout = timeout)
    if result.return_code == 0:
      return

  fail("Command failed after {} attempt(s): {}\nstdout:\n{}\nstderr:\n{}".format(
      attempts,
      " ".join(arguments),
      result.stdout,
      result.stderr,
  ))

def _impl(repository_ctx):
  _execute_or_fail(repository_ctx, [
      "git",
      "clone",
      "--branch",
      "2026.05.25",
      "--depth",
      "1",
      "https://github.com/microsoft/vcpkg.git",
  ])
  _execute_or_fail(repository_ctx, ["./vcpkg/bootstrap-vcpkg.bat"], attempts = 2)
  _execute_or_fail(
      repository_ctx,
      ["./vcpkg/vcpkg.exe", "install", "arrow:x64-windows-static-md"],
      timeout = 6000,
      attempts = 3,
  )

  repository_ctx.file("BUILD", """
load("@rules_cc//cc:defs.bzl", "cc_library")

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
