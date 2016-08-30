import hpx
import numpy as np

def main_handler():
    test_memory = hpx.GlobalMemory.alloc_local_at_sync(3, (4,5), np.dtype(np.int), hpx.HERE())
    
    # test strides and offset initialization
    itemsize = np.dtype(np.int).itemsize
    assert test_memory.shape == (3, 4, 5)
    assert test_memory.strides == (20*itemsize, 5*itemsize, itemsize)
    assert test_memory.offsets == (0, 0, 0)

    # test indexing
    assert type(test_memory[2]) is hpx.GlobalAddressBlock
    assert test_memory[2].addr.addr == test_memory.addr.addr + itemsize*20*2
    assert test_memory[2].shape == (4,5)

    assert type(test_memory[1:]) is hpx.GlobalMemory
    assert test_memory[1:].addr.addr == test_memory.addr.addr
    assert test_memory[1:].shape == (2, 4, 5)
    assert test_memory[1:].strides == (20*itemsize, 5*itemsize, itemsize)
    assert test_memory[1:].offsets == (20*itemsize, 0, 0)

    assert type(test_memory[1:, 2, 1:3]) is hpx.GlobalMemory
    assert test_memory[1:, 2, 1:3].addr.addr == test_memory.addr.addr
    assert test_memory[1:, 2, 1:3].shape == (2, 1, 2)
    assert test_memory[1:, 2, 1:3].strides == (20*itemsize, 5*itemsize, itemsize)
    assert test_memory[1:, 2, 1:3].offsets == (20*itemsize, 10*itemsize, itemsize)

    # test free
    test_memory[1:].free_sync()

    hpx.exit()

main_action = hpx.Action(main_handler, hpx.ATTR_NONE, b"main", [])

hpx.init()
hpx.run(main_action)
hpx.finalize()
