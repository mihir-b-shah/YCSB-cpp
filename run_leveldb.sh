export LD_LIBRARY_PATH='$LD_LIBRARY_PATH:./sharkdb/sharkdb/liburing/lib'
/usr/bin/time -v taskset --cpu-list 0-3 ./ycsb -load -run -db leveldb -P workloads/workload$1 -P leveldb/leveldb.properties -p threadcount=800 -p operationcount=1000000 -s
