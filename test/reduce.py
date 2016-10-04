import hpx
import numpy as np

@hpx.create_id_action(np.dtype(np.int))
def set_zero(array):
    array[:] = 0

@hpx.create_op_action(np.dtype(np.int))
def add(lhs, rhs):
    lhs = lhs + rhs

@hpx.create_action([])
def main():
    reduce_lco = hpx.Reduce(5, (3,4,5), np.dtype(np.int), set_zero, add)
    for i in range(5):
        array = np.ones((3,4,5))
        reduce_lco.set(array)
    hpx.exit()

if __name__ == "__main__":
    hpx.init()
    hpx.run(main)
    hpx.finalize()
