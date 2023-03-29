FLAGS=-g -O2
O_FILES=$(patsubst src/%.c,build/%.o,$(wildcard src/*.c))
TESTS=$(patsubst tests/%.c,build/%.run,$(wildcard tests/*.c))
TGT=build/libsharkdb.so
TGT_LFLAGS=-lsharkdb
HDR=sharkdb.h

all: $(TGT) $(TESTS)

$(TGT): $(O_FILES)
	gcc $(FLAGS) -shared $(O_FILES) -o $@

build/%.o: src/%.c $(HDR)
	gcc -I. -c $(FLAGS) -o $@ $<

build/%.run: tests/%.c $(HDR)
	gcc -I. -o $@ $(FLAGS) $< -L./build/ -Wl,-rpath=./build/ $(TGT_LFLAGS)

clean:
	rm $(TGT) $(TESTS) $(O_FILES)
