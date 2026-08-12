from __future__ import annotations

import subprocess
import sys
from collections import deque


def escape_workflow_command(value: str) -> str:
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


process = subprocess.Popen(
    [sys.executable, "-m", "cibuildwheel", "--output-dir", "wheelhouse"],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
)

assert process.stdout is not None
recent_output: deque[str] = deque(maxlen=100)
for line in process.stdout:
    print(line, end="", flush=True)
    recent_output.append(line)

return_code = process.wait()
if return_code:
    # GitHub truncates annotation messages at 4 KiB. Keep the end of the
    # output, where build tools and tracebacks report the actual failure.
    details = escape_workflow_command("".join(recent_output)[-3900:])
    print(f"::error title=cibuildwheel failed::{details}")
    raise SystemExit(return_code)
