FLAGS=-g -O2 #-fsanitize=address -fsanitize=undefined #-DNDEBUG
O_FILES=$(patsubst src/%.cc,build/%.o,$(wildcard src/*.cc))
TESTS=$(patsubst tests/%.cc,build/%.run,$(wildcard tests/*.cc))
BENCHS=$(patsubst bench/%.cc,bench/%.run,$(wildcard bench/*.cc))
TGT=build/libsharkdb.so
TGT_LFLAGS=-lsharkdb
HDR=sharkdb.h
SRC_HDRS=$(wildcard src/*.h)

all: $(TESTS) $(BENCHS) $(TGT)

bench/%.run: bench/%.cc
	g++ -I./liburing/include -o $@ $(FLAGS) $< -L./liburing/lib -Wl,-rpath=./liburing/lib -luring

build/%.run: tests/%.cc $(HDR) $(TGT)
	g++ -I. -fPIC -o $@ $(FLAGS) $< -L./build/ -Wl,-rpath=./build/ -ltbb -lpthread -pthread $(TGT_LFLAGS)

$(TGT): $(O_FILES)
	g++ $(FLAGS) -shared $(O_FILES) -fPIC -ltbb -latomic -pthread -lpthread -o $@

build/%.o: src/%.cc $(HDR) $(SRC_HDRS)
	g++ -I. -I./liburing/include -c $(FLAGS) -L./liburing/lib -Wl,-rpath=./liburing/lib -luring -latomic -ltbb -lpthread -pthread -fPIC -o $@ $<

clean:
	rm $(TGT) $(TESTS) $(O_FILES) $(BENCHS)
