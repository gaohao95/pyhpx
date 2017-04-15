import hpx
import numpy as np

@hpx.create_action()
def main():
    test_memory = hpx.GlobalMemory.alloc_local_at(3, (4,5), np.dtype(np.int), hpx.HERE())
    
    # test strides and offset initialization
    itemsize = np.dtype(np.int).itemsize
    assert test_memory.numBlock == (3,)
    assert test_memory.blockShape == (4,5)
    assert test_memory.strides == (20*itemsize, 5*itemsize, itemsize)

    # test indexing
    assert type(test_memory[2]) is hpx.GlobalAddressBlock
    assert test_memory[2].addr.addr == test_memory.addr.addr + itemsize*20*2
    assert test_memory[2].shape == (4,5)
    assert test_memory[2].strides == (5*itemsize, itemsize)

    assert type(test_memory[1:]) is hpx.GlobalMemory
    assert test_memory[1:].addr.addr == test_memory.addr.addr + itemsize*20
    assert test_memory[1:].numBlock == (2,)
    assert test_memory[1:].blockShape == (4, 5)
    assert test_memory[1:].strides == (20*itemsize, 5*itemsize, itemsize)

    assert type(test_memory[1:, 2, 1:3]) is hpx.GlobalMemory
    assert test_memory[1:, 2, 1:3].addr.addr == test_memory.addr.addr + itemsize*20 + itemsize*5*2 + itemsize*1
    assert test_memory[1:, 2, 1:3].numBlock == (2,)
    assert test_memory[1:, 2, 1:3].blockShape == (2,)
    assert test_memory[1:, 2, 1:3].strides == (20*itemsize, itemsize)

    assert type(test_memory[1:, 1]) is hpx.GlobalMemory
    assert test_memory[1:, 1].addr.addr == test_memory.addr.addr + itemsize*20 + itemsize*5
    assert test_memory[1:, 1].numBlock == (2,)
    assert test_memory[1:, 1].blockShape == (5,)
    assert test_memory[1:, 1].strides == (20*itemsize, itemsize)

    sub_memory = test_memory[1:, 2:, :3]
    assert type(sub_memory[1]) is hpx.GlobalAddressBlock
    assert sub_memory[1].addr.addr == test_memory.addr.addr + itemsize*20*2 + itemsize*5*2
    assert sub_memory[1].shape == (2, 3)
    assert sub_memory[1].strides == (5*itemsize, itemsize)

    assert type(sub_memory[1:, 1]) is hpx.GlobalMemory
    assert sub_memory[1:, 1].addr.addr == test_memory.addr.addr + itemsize*20*2 + itemsize*5*3
    assert sub_memory[1:, 1].numBlock == (1,)
    assert sub_memory[1:, 1].blockShape == (3,)

    sub_memory = test_memory[1]
    assert type(sub_memory[1:]) is hpx.GlobalAddressBlock
    assert sub_memory[1:].addr.addr == test_memory.addr.addr + 20*itemsize + 5*itemsize
    assert sub_memory[1:].shape == (3, 5)

    try:
        test_memory[3]
    except RuntimeError:
        pass
    else:
        raise RuntimeError("Runtime error not raised!")

    try:
        sub_memory[4]
    except RuntimeError:
        pass
    else:
        raise RuntimeError("Runtime error not raised!")

    try:
        sub_memory[2:5]
    except RuntimeError:
        pass
    else:
        raise RuntimeError("Runtime error not raised!")

    # test try_pin and unpin
    sub_block = test_memory[1, :2]
    array = sub_block.try_pin()
    assert isinstance(array, np.ndarray)
    array[0, 0] = 5 # test_memory[1, 0, 0] = 5
    array[1, 1] = 10 # test_memory[1, 1, 1] = 10
    sub_block.unpin()
    array = test_memory[1].try_pin()
    assert array[0, 0] == 5
    assert array[1, 1] == 10
    test_memory[1].unpin()

    # test get
    array = test_memory[1].get(sync='sync')
    assert array[0, 0] == 5
    assert array[1, 1] == 10

    # test set
    from_array = np.array([6,11])
    test_memory[2,0,2:4].set(from_array, sync='rsync') # test_memory[2,0,2:4] = [6,11]
    array = test_memory[2].try_pin()
    assert array[0, 2] == 6
    assert array[0, 3] == 11

    # when get and set on not contiguous gas, RuntimeError should be raised
    try:
        array = test_memory[1,:2,1:].get(sync='sync')
    except RuntimeError:
        pass
    else:
        raise RuntimeError("Runtime error not raised!")

    try:
        test_memory[1,:2,1].set(from_array, sync='rsync')
    except RuntimeError:
        pass
    else:
        raise RuntimeError("Runtime error not raised!")

    # test get and set on array with some dimension of size 1
    test_memory_2 = hpx.GlobalMemory.alloc_local_at(2, (2,2,2,2,2), np.dtype(np.int), hpx.HERE())
    from_array = np.array([[1,2],[3,4]])
    test_memory_2[1,1,0,1,:,:].set(from_array, sync='rsync')
    get_array = test_memory_2[1,1,0,:,:,:].get(sync='sync')
    assert np.array_equal(get_array[1], from_array)

    # test free
    test_memory.free_sync()
    
    hpx.exit()

hpx.init()
hpx.run(main)
hpx.finalize()
