CC=mpicc
CFLAGS=$(shell pkg-config --cflags hpx) -O2 -std=gnu99
CFLAGS_DEBUG=$(shell pkg-config --cflags hpx) -O0 -g -std=gnu99
LINK=-lm $(shell pkg-config --libs hpx)

all: kmeans
kmeans: kmeans.c
	$(CC) kmeans.c $(CFLAGS) $(LINK) -o kmeans
debug:
	$(CC) kmeans.c $(CFLAGS_DEBUG) $(LINK) -o kmeans
clean:
	rm kmeans
