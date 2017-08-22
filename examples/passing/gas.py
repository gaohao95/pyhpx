import hpx
import sys
import numpy as np

@hpx.create_action()
def copy_from_array(array, cur_rank, num_rank):
    from_locality = (cur_rank + 1) % num_rank
    local_array = array[from_locality].get()
    return hpx.SUCCESS

@hpx.create_action()
def main():
    num_ranks = hpx.get_num_ranks()
    print(num_ranks)
    array = hpx.GlobalMemory.alloc_cyclic(num_ranks, (64, 1024, 1024), dtype=np.dtype(float))

    start = hpx.time_now()
    and_lco = hpx.And(num_ranks)
    for i in range(num_ranks):
        copy_from_array(hpx.THERE(i), array, i, num_ranks, rsync_lco=and_lco)
    and_lco.wait()
    print(hpx.time_elapsed_ms(start))
    hpx.exit()

if __name__ == '__main__':
    hpx.init(sys.argv)
    hpx.run(main)
    hpx.finalize()