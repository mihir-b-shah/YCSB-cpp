FLAGS=-g -O2 -Wall -Werror #-fsanitize=address -fsanitize=undefined #-DNDEBUG
O_FILES=$(patsubst src/%.cc,build/%.o,$(wildcard src/*.cc))
TESTS=$(patsubst tests/%.cc,build/%.run,$(wildcard tests/*.cc))
BENCHS=$(patsubst bench/%.cc,bench/%.run,$(wildcard bench/*.cc))
TGT=build/libsharkdb.so
TGT_LFLAGS=-lsharkdb
HDR=sharkdb.h
SRC_HDRS=$(wildcard src/*.h)
URING_FLAGS=-L./liburing/lib -Wl,-rpath=./liburing/lib -luring

all: $(TESTS) $(BENCHS) $(TGT)

bench/%.run: bench/%.cc
	g++ -I./liburing/include -o $@ $(FLAGS) $< $(URING_FLAGS)

build/%.run: tests/%.cc $(HDR) $(TGT)
	g++ -I. -fPIC -o $@ $(FLAGS) $< -L./build/ -Wl,-rpath=./build/ -ltbb -lpthread -pthread $(TGT_LFLAGS) $(URING_FLAGS)

$(TGT): $(O_FILES)
	g++ $(FLAGS) -shared $(O_FILES) -fPIC -ltbb -latomic -pthread -lpthread $(URING_FLAGS) -o $@

build/%.o: src/%.cc $(HDR) $(SRC_HDRS)
	g++ -I. -I./liburing/include -c $(FLAGS) $(URING_FLAGS) -latomic -ltbb -lpthread -pthread -fPIC -o $@ $<

clean:
	rm $(TGT) $(TESTS) $(O_FILES) $(BENCHS)
