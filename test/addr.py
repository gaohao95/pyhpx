import hpx
import numpy as np
# from build._hpx import ffi, lib

def main_handler():
    # test addr arithmatic
    global_memory = hpx.GlobalMemory.alloc_local_at_sync(3, (3,), 
            np.dtype(np.int), hpx.HERE())
    print(global_memory.addr.addr)
    global_addr = global_memory.addr + np.dtype(np.int).itemsize
    print(global_addr.addr)
    hpx.exit()

main_action = hpx.Action(main_handler, hpx.ATTR_NONE, b"main", []) 

hpx.init()
hpx.run(main_action)
hpx.finalize()
