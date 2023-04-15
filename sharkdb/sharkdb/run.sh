# rm -f /tmp/sharkdb/* && strace ./build/$1.run
rm -f /tmp/sharkdb/* && /usr/bin/time -v ./build/$1.run
# rm -f /tmp/sharkdb/* && gdb ./build/$1.run
