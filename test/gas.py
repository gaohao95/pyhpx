import hpx
import numpy as np

def main_handler():
    test_memory = hpx.GlobalMemory.alloc_local_at_sync(3, (3,), np.dtype(np.int), hpx.HERE())
    test_memory[5]
    hpx.exit()

main_action = hpx.Action(main_handler, hpx.ATTR_NONE, b"main", [])

hpx.init()
hpx.run(main_action)
hpx.finalize()
