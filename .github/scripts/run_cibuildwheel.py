from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import sys
import time
import urllib.request
from collections import deque


def escape_workflow_command(value: str) -> str:
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def prefetch_windows_virtualenv() -> None:
    if sys.platform != "win32":
        return

    # Keep these values aligned with the cibuildwheel version pinned in the
    # workflow. cibuildwheel otherwise downloads this file only once, making a
    # transient GitHub error fail the entire Windows matrix.
    version = "20.35.2"
    cache_path = (
        pathlib.Path(os.environ["LOCALAPPDATA"]) / "pypa" / "cibuildwheel" / "Cache" / f"virtualenv-{version}.pyz"
    )
    if cache_path.exists():
        return

    # PyPA documents this CDN endpoint for the zipapp. It avoids depending on
    # GitHub release assets merely to start cibuildwheel.
    url = "https://bootstrap.pypa.io/virtualenv.pyz"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = cache_path.with_suffix(".tmp")

    for attempt in range(1, 7):
        try:
            print(f"Prefetching {url} (attempt {attempt}/6)", flush=True)
            with urllib.request.urlopen(url, timeout=60) as response, temporary_path.open("wb") as output:
                shutil.copyfileobj(response, output)
            temporary_path.replace(cache_path)
            return
        except Exception:
            temporary_path.unlink(missing_ok=True)
            if attempt == 6:
                raise
            time.sleep(2 ** (attempt - 1))


prefetch_windows_virtualenv()

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
