import hpx
import numpy as np

@hpx.create_action()
def main():
    future = hpx.Future()
    set_lco(hpx.HERE(), future, 2)
    future.wait()

    out_array = np.empty((5,6))
    return_an_array(hpx.HERE(), sync='rsync', out_array=out_array)

    rtv = np.arange(6).reshape((2, 3))
    hpx.exit(rtv)

@hpx.create_action()
def set_lco(lco, unused_int):
    lco.set()
    assert unused_int == 2
    return hpx.SUCCESS

@hpx.create_action()
def return_an_array():
    return hpx.SUCCESS

if __name__ == '__main__':
    hpx.init()
    rtv = hpx.run(main, shape=(2, 3), dtype=np.dtype(int))
    assert np.array_equal(rtv, np.arange(6).reshape((2, 3)))
    hpx.finalize()
