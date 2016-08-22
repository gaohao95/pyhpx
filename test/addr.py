import hpx
import numpy as np
# from build._hpx import ffi, lib

def main_handler():
    # test addr arithmatic
    global_memory = hpx.GlobalMemory.alloc_local_at_sync(3, (3,), 
            np.dtype(np.int), hpx.HERE())
    hpx.exit()

main_action = hpx.Action(main_handler, hpx.ATTR_NONE, b"main", []) 

hpx.init()
hpx.run(main_action)
hpx.finalize()
