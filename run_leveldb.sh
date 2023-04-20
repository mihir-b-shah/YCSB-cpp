export LD_LIBRARY_PATH='$LD_LIBRARY_PATH:./sharkdb/sharkdb/liburing/lib'
/usr/bin/time -v ./ycsb -load -run -db leveldb -P workloads/workload$1 -P leveldb/leveldb.properties -p threadcount=256 -p operationcount=1000000 -s
