import hpx
import math
import numpy as np
from sklearn.neighbors import NearestNeighbors
import logging
import sys

DATA_SIZE = 10000000
DATA_RANGE = 100000.0
DATA_PER_NODE = 1000000
K = 100 # must < DATA_PER_NODE
MAX_ITERATION = 10
NUM_NODE = math.ceil(DATA_SIZE/DATA_PER_NODE)
DIM = 2

@hpx.create_action(pinned=True)
def generate_data(array, count):
    array[:count, :] = np.random.rand(count, 2)*DATA_RANGE
    return hpx.SUCCESS

@hpx.create_id_action(np.dtype(np.int))
def initialize_count(array):
    array[:] = 0

@hpx.create_op_action(np.dtype(np.int))
def sum_count(lhs_array, rhs_array):
    lhs_array[:] = lhs_array + rhs_array

@hpx.create_id_action(np.dtype(np.float))
def initialize_position(array):
    array[:] = 0

@hpx.create_op_action(np.dtype(np.float))
def sum_position(lhs_array, rhs_array):
    lhs_array[:] = lhs_array + rhs_array

@hpx.create_action(pinned=True)
def calculate_centers(data, size, centers, count_lco, position_lco, and_lco):
    nbrs = NearestNeighbors(n_neighbors=1, algorithm='auto').fit(centers)
    nearest_centers = nbrs.kneighbors(data, return_distance=False)[:, 0]
    count_lco.set(array=np.bincount(nearest_centers, minlength=K),
                  sync='lsync')
    positions = np.zeros((K, DIM))
    for i in range(K):
        positions[i, :] = np.sum(data[nearest_centers == i], axis=0)
    position_lco.set(array=positions, sync='lsync')
    and_lco.set()
    
    return hpx.SUCCESS

def node_size(no):
    if no == NUM_NODE - 1:
        return DATA_SIZE - no*DATA_PER_NODE
    else:
        return DATA_PER_NODE

@hpx.create_action()
def main():
    data = hpx.GlobalMemory.alloc_cyclic(NUM_NODE, (DATA_PER_NODE, DIM), 
                                         np.dtype(np.float))

    generate_data_complete = hpx.And(NUM_NODE)
    for i in range(NUM_NODE):
        generate_data(data[i], node_size(i), rsync_lco=generate_data_complete)
    generate_data_complete.wait()
    
    data_this_block = data[0].try_pin()
    centers = data_this_block[:K]
    data[0].unpin()
    iterations = 0
    while iterations < MAX_ITERATION:
        count_lco = hpx.Reduce(NUM_NODE, (K,), np.dtype(np.int), 
                               initialize_count, sum_count)  
        position_lco = hpx.Reduce(NUM_NODE, (K, DIM), np.dtype(np.float),
                                  initialize_position, sum_position)
        and_lco = hpx.And(NUM_NODE)
        for i in range(NUM_NODE):
            calculate_centers(data[i], node_size(i), centers, 
                              count_lco, position_lco, and_lco)
        counts = count_lco.get()
        positions = position_lco.get()
        centers = positions / counts.reshape((K,1))
        and_lco.wait()
        count_lco.delete_sync()
        position_lco.delete_sync()
        and_lco.delete_sync()
        iterations = iterations + 1

    hpx.exit()

if __name__ == '__main__':
    hpx.init(sys.argv)
    hpx.run(main)
    hpx.finalize()
    
