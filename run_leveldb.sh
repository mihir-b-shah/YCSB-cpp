export LD_LIBRARY_PATH='$LD_LIBRARY_PATH:./sharkdb/sharkdb/liburing/lib'
./ycsb -load -run -db leveldb -P workloads/workload$1 -P leveldb/leveldb.properties -s
