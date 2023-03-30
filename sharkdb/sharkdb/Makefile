FLAGS=-g -O2
O_FILES=$(patsubst src/%.cc,build/%.o,$(wildcard src/*.cc))
TESTS=$(patsubst tests/%.cc,build/%.run,$(wildcard tests/*.cc))
TGT=build/libsharkdb.so
TGT_LFLAGS=-lsharkdb
HDR=sharkdb.h

all: $(TGT) $(TESTS)

$(TGT): $(O_FILES)
	g++ $(FLAGS) -shared $(O_FILES) -o $@

build/%.o: src/%.cc $(HDR)
	g++ -I. -I./liburing/include -c $(FLAGS) -L./liburing/lib -Wl,-rpath=./liburing/lib -luring -o $@ $<

build/%.run: tests/%.cc $(HDR)
	g++ -I. -o $@ $(FLAGS) $< -L./build/ -Wl,-rpath=./build/ $(TGT_LFLAGS)

clean:
	rm $(TGT) $(TESTS) $(O_FILES)
