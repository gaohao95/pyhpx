import hpx
import numpy as np

@hpx.create_action()
def main_action():
    # test addr arithmatic
    global_memory = hpx.GlobalMemory.alloc_local_at_sync(3, (4,), np.dtype(np.int), hpx.HERE())
    global_addr = global_memory.addr + 2*np.dtype(np.int).itemsize
    global_addr_2 = 2*np.dtype(np.int).itemsize + global_memory.addr
    assert global_addr.addr == global_memory[0, 2].addr.addr + global_memory[0, 2].offsets[0]
    assert global_addr.addr == global_addr_2.addr

    assert global_addr - global_memory.addr == 2*np.dtype(np.int).itemsize
    assert (global_addr - 2*np.dtype(np.int).itemsize).addr == global_memory.addr.addr

    hpx.exit()

hpx.init()
hpx.run(main_action)
hpx.finalize()
