import hpx
import sys
import numpy as np

NUM_TRY = 16

@hpx.create_action(marshalled='continuous')
def some_marshalled_action(huge_arg):
    return hpx.SUCCESS

@hpx.create_action()
def worker(current_rank, num_ranks, and_lco):
    target_rank = (current_rank + 1) % num_ranks
    array = np.empty((128//NUM_TRY, 1024, 1024))
    finish_copy = hpx.Future()
    some_marshalled_action(hpx.THERE(target_rank), array, sync='async', lsync_lco=finish_copy, rsync_lco=and_lco)
    finish_copy.wait()
    print("worker {0} sends {1} bytes".format(current_rank, array.nbytes))
    return hpx.SUCCESS

@hpx.create_action()
def main():
    start = hpx.time_now()
    num_ranks = hpx.get_num_ranks()
    and_lco = hpx.And(num_ranks*NUM_TRY)
    for i in range(num_ranks):
        for j in range(NUM_TRY):
            worker(hpx.THERE(i), i, num_ranks, and_lco)
    and_lco.wait()
    print(hpx.time_elapsed_ms(start))
    hpx.exit()

if __name__ == '__main__':
    hpx.init(sys.argv)
    hpx.run(main)
    hpx.finalize()