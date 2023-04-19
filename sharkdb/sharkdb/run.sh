# rm -f /tmp/sharkdb/* && strace --stack-traces ./build/$1.run
# rm -f /tmp/sharkdb/* && strace -c ./build/$1.run
rm -f /tmp/sharkdb/* && /usr/bin/time -v ./build/$1.run
# rm -f /tmp/sharkdb/* && ./build/$1.run
# rm -f /tmp/sharkdb/* && gdb ./build/$1.run
