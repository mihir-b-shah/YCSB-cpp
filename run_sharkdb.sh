
set -e

cd sharkdb/sharkdb && make -j 4 && cd -
make -j 4 
export LD_LIBRARY_PATH='$LD_LIBRARY_PATH:./sharkdb/sharkdb/liburing/lib'
rm -f /tmp/sharkdb/*
./ycsb -load -run -db sharkdb -P workloads/workload$1 -s
