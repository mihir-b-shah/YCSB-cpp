
set -e

rm -f /tmp/sharkdb/*
PERF record ./build/$1.run
