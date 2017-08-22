import hpx
import sys
import numpy as np

@hpx.create_action(marshalled='true')
def some_marshalled_action(huge_arg):
    return hpx.SUCCESS

@hpx.create_action()
def worker(current_rank, num_ranks):
    target_rank = (current_rank + 1) % num_ranks
    array = np.array((128, 1024, 1024))
    some_marshalled_action(hpx.THERE(target_rank), array, sync='rsync')
    print("worker {0} sends {1} bytes".format(current_rank, array.nbytes))
    return hpx.SUCCESS

@hpx.create_action()
def main():
    start = hpx.time_now()
    num_ranks = hpx.get_num_ranks()
    and_lco = hpx.And(num_ranks)
    for i in range(num_ranks):
        worker(hpx.THERE(i), i, num_ranks, rsync_lco=and_lco)
    and_lco.wait()
    print(hpx.time_elapsed_ms(start))
    hpx.exit()

if __name__ == '__main__':
    hpx.init(sys.argv)
    hpx.run(main)
    hpx.finalize()
