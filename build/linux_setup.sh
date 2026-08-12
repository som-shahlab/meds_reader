set -euo pipefail

curl \
    --fail \
    --location \
    --retry 8 \
    --retry-all-errors \
    --retry-delay 2 \
    --connect-timeout 30 \
    --output /tmp/meds-reader-bazelisk \
    https://github.com/bazelbuild/bazelisk/releases/download/v1.27.0/bazelisk-linux-amd64
install -m 0755 /tmp/meds-reader-bazelisk /usr/local/bin/bazel
