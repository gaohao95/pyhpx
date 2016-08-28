import hpx
import numpy as np

def main_handler():
    test_memory = hpx.GlobalMemory.alloc_local_at_sync(3, (4,5), np.dtype(np.int), hpx.HERE())
    
    # test strides
    itemsize = np.dtype(np.int).itemsize
    assert test_memory.strides == (20*itemsize, 5*itemsize, itemsize)

    # test indexing
    assert type(test_memory[2]) is hpx.GlobalAddressBlock
    assert test_memory[2].addr.addr == test_memory.addr.addr + itemsize*20*2


    hpx.exit()

main_action = hpx.Action(main_handler, hpx.ATTR_NONE, b"main", [])

hpx.init()
hpx.run(main_action)
hpx.finalize()
