import hpx
import numpy as np

@hpx.create_id_action(np.dtype(np.int))
def set_zero(array):
    return np.ones(array.shape, dtype=np.int)

@hpx.create_op_action(np.dtype(np.int))
def add(lhs, rhs):
    return lhs + rhs

@hpx.create_action([])
def main():
    reduce_lco = hpx.Reduce(5, (3,4,5), np.dtype(np.int), set_zero, add)
    for i in range(5):
        array = np.ones((3,4,5), dtype=np.int)
        reduce_lco.set(array, sync='lsync')
    return_array = reduce_lco.get()
    print(return_array)
    hpx.exit()

if __name__ == "__main__":
    hpx.init()
    hpx.run(main)
    hpx.finalize()
