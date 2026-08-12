set -euo pipefail

download_succeeded=0
for attempt in {1..8}; do
    if curl \
        --fail \
        --location \
        --connect-timeout 30 \
        --max-time 120 \
        --output /tmp/meds-reader-bazelisk \
        https://github.com/bazelbuild/bazelisk/releases/download/v1.27.0/bazelisk-linux-amd64; then
        download_succeeded=1
        break
    fi

    if [ "$attempt" -lt 8 ]; then
        sleep "$((attempt * 2))"
    fi
done

test "$download_succeeded" -eq 1
install -m 0755 /tmp/meds-reader-bazelisk /usr/local/bin/bazel
