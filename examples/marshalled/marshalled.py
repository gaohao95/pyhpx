import hpx
import sys
import numpy as np

@hpx.create_action(marshalled=True)
def some_marshalled_action(huge_arg):
    return hpx.SUCCESS

@hpx.create_action()
def worker(current_rank, num_ranks):
    target_rank = (current_rank + 1) % num_ranks
    array = np.zeros((128, 1024, 1024))
    some_marshalled_action(hpx.THERE(target_rank), array)
    print("worker {0} sends {1} bytes".format(current_rank, array.nbytes))
    return hpx.SUCCESS

@hpx.create_action()
def main():
    num_ranks = hpx.get_num_ranks()
    for i in range(num_ranks):
        worker(hpx.THERE(i), i, num_ranks)
    hpx.exit()

if __name__ == '__main__':
    hpx.init(sys.argv)
    hpx.run(main)
    hpx.finalize()
