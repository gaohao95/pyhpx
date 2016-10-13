CC=mpicc
CFLAGS=$(shell pkg-config --cflags hpx) -O2 -std=gnu99
CFLAGS_DEBUG=$(shell pkg-config --cflags hpx) -O0 -g -std=gnu99
LINK=-lm $(shell pkg-config --libs hpx)

all: marshalled
marshalled: marshalled_test.c
	$(CC) marshalled_test.c $(CFLAGS) $(LINK) -o marshalled_test
debug:
	$(CC) marshalled_test.c $(CFLAGS_DEBUG) $(LINK) -o marshalled_test
clean:
	rm marshalled_test
