import hpx
import numpy as np

@hpx.create_action()
def main():
    test_memory = hpx.GlobalMemory.alloc_local_at(3, (4,5), np.dtype(np.int), hpx.HERE())
    
    # test strides and offset initialization
    itemsize = np.dtype(np.int).itemsize
    assert test_memory.shape == (3, 4, 5)
    assert test_memory.strides == (20*itemsize, 5*itemsize, itemsize)
    assert test_memory.offsets == (0, 0, 0)

    # test indexing
    assert type(test_memory[2]) is hpx.GlobalAddressBlock
    assert test_memory[2].addr.addr == test_memory.addr.addr + itemsize*20*2
    assert test_memory[2].shape == (4,5)
    assert test_memory[2].strides == (5*itemsize, itemsize)
    assert test_memory[2].offsets == (0, 0)

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

    assert type(test_memory[1:, 1]) is hpx.GlobalMemory
    assert test_memory[1:, 1].addr.addr == test_memory.addr.addr
    assert test_memory[1:, 1].shape == (2, 1, 5)
    assert test_memory[1:, 1].strides == (20*itemsize, 5*itemsize, itemsize)
    assert test_memory[1:, 1].offsets == (20*itemsize, 5*itemsize, 0)

    sub_memory = test_memory[1:, 2:, :3]
    assert type(sub_memory[1]) is hpx.GlobalAddressBlock
    assert sub_memory[1].addr.addr == test_memory.addr.addr + itemsize*20*2
    assert sub_memory[1].shape == (2, 3)
    assert sub_memory[1].strides == (5*itemsize, itemsize)
    assert sub_memory[1].offsets == (10*itemsize, 0) 

    assert type(sub_memory[1:, 1]) is hpx.GlobalMemory
    assert sub_memory[1:, 1].addr.addr == test_memory.addr.addr
    assert sub_memory[1:, 1].shape == (1, 1, 3)
    assert sub_memory[1:, 1].offsets == (40*itemsize, 15*itemsize, 0)

    sub_memory = test_memory[1]
    assert type(sub_memory[1:]) is hpx.GlobalAddressBlock
    assert sub_memory[1:].addr.addr == test_memory.addr.addr + 20*itemsize
    assert sub_memory[1:].shape == (3, 5)
    assert sub_memory[1:].offsets == (5*itemsize, 0)

    # test try_pin and unpin
    sub_block = test_memory[1, :2, 1:]
    array = sub_block.try_pin()
    assert isinstance(array, np.ndarray)
    array[0, 0] = 5 # test_memory[1, 0, 1] = 5
    array[1, 1] = 10 # test_memory[1, 1, 2] = 10
    sub_block.unpin()
    array = test_memory[1].try_pin()
    assert array[0, 1] == 5
    assert array[1, 2] == 10
    test_memory[1].unpin()

    # test get
    array = test_memory[1].get(sync='lsync')
    assert array[0, 1] == 5
    assert array[1, 2] == 10
    # array = sub_block.get(sync='lsync')

    # test set
    from_array = np.array([6,11])
    test_memory[2,0,2:4].set(from_array, sync='rsync') # test_memory[2,0,2:4] = [6,11]
    array = test_memory[2].try_pin()
    assert array[0, 2] == 6
    assert array[0, 3] == 11

    # test free
    test_memory[1:].free_sync()
    
    hpx.exit()

hpx.init()
hpx.run(main)
hpx.finalize()
