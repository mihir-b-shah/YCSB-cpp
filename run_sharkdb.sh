
set -e

cd sharkdb/sharkdb && make -j 4 && cd -
make -j 4 
export LD_LIBRARY_PATH='$LD_LIBRARY_PATH:./sharkdb/sharkdb/liburing/lib'
rm -f /tmp/sharkdb/*
./ycsb -run -db sharkdb -P workloads/workload$1 -p threadcount=4 -p operationcount=1000000 -s
