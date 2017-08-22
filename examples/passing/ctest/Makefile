CC=mpicc
CFLAGS=$(shell pkg-config --cflags hpx) -O2 -std=gnu99
LINK=-lm $(shell pkg-config --libs hpx)

all: contiguous
contiguous: contiguous.c
	$(CC) contiguous.c $(CFLAGS) $(LINK) -o contiguous
clean:
	rm -f contiguous
