import hpx
import numpy as np

@hpx.create_action()
def main():
    future = hpx.Future()
    set_lco(hpx.HERE(), future, 2)
    future.wait()
    rtv = np.arange(6).reshape((2, 3))
    hpx.exit(rtv)

@hpx.create_action()
def set_lco(lco, unused_int):
    lco.set()
    assert unused_int == 2
    return hpx.SUCCESS

if __name__ == '__main__':
    hpx.init()
    rtv = hpx.run(main, shape=(2, 3), dtype=np.dtype(int))
    assert np.array_equal(rtv, np.arange(6).reshape((2, 3)))
    hpx.finalize()
