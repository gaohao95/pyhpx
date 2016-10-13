mpirun -np 2 --map-by node:PE=8 --hostfile hosts -x LD_LIBRARY_PATH ./dotproduct --hpx-log-level=default --hpx-log-at=0 --hpx-network=isir
