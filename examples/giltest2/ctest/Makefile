all: ctest

ctest: ctest.o calculate.o
	gcc ctest.o calculate.o `pkg-config --libs hpx` -o ctest

ctest.o: ctest.c
	gcc -c ctest.c `pkg-config --cflags hpx` -o ctest.o

calculate.o: calculate.c
	gcc -o0 -c calculate.c -o calculate.o

clean:
	rm -f calculate.o ctest.o ctest
