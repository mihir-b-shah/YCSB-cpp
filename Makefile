#
#  Makefile
#  YCSB-cpp
#
#  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
#  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
#  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
#


#---------------------build config-------------------------

# Database bindings
BIND_SHARKDB ?= 1
BIND_LEVELDB ?= 1

# Extra options
DEBUG_BUILD ?= 0
EXTRA_CXXFLAGS ?= -I./leveldb/leveldb/include -I./sharkdb/sharkdb
EXTRA_LDFLAGS ?= -L./leveldb/leveldb/build -L./sharkdb/sharkdb/build -Wl,-rpath=./sharkdb/sharkdb/build -lsnappy -L./sharkdb/sharkdb/liburing/lib -Wl,-rpath=./sharkdb/sharkdb/liburing/lib -luring

#----------------------------------------------------------

ifeq ($(DEBUG_BUILD), 1)
	CXXFLAGS += -g
else
	CXXFLAGS += -O2
	CPPFLAGS += -DNDEBUG
endif

ifeq ($(BIND_SHARKDB), 1)
	LDFLAGS += -lsharkdb
	SOURCES += $(wildcard sharkdb/*.cc)
endif

ifeq ($(BIND_LEVELDB), 1)
	LDFLAGS += -lleveldb
	SOURCES += $(wildcard leveldb/*.cc)
endif

CXXFLAGS += -std=c++17 -Wall -pthread $(EXTRA_CXXFLAGS) -I./
LDFLAGS += $(EXTRA_LDFLAGS) -lpthread
SOURCES += $(wildcard core/*.cc)
OBJECTS += $(SOURCES:.cc=.o)
DEPS += $(SOURCES:.cc=.d)
EXEC = ycsb

all: $(EXEC)

$(EXEC): $(OBJECTS)
	@$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@
	@echo "  LD      " $@

.cc.o:
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
	@echo "  CC      " $@

%.d: %.cc
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MM -MT '$(<:.cc=.o)' -o $@ $<

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

clean:
	find . -name "*.[od]" -delete
	$(RM) $(EXEC)

.PHONY: clean
