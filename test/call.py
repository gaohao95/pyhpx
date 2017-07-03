import hpx
import numpy as np

from cffi import FFI
ffi = FFI()

@hpx.create_action()
def main():
    future = hpx.Future()
    set_lco(hpx.HERE(), future, 2)
    future.wait()
    future.delete()

    out_array = np.zeros((5,6), dtype=int)
    return_an_array(hpx.HERE(), sync='rsync', out_array=out_array)
    assert np.array_equal(out_array, np.arange(30).reshape((5,6)))

    future = hpx.Future((3,4), dtype=np.dtype(int))
    call_cc(hpx.HERE(), sync='lsync', rsync_lco=future)
    out_array = future.get()
    assert np.array_equal(out_array, np.arange(12).reshape((3,4)))

    rtv = np.arange(6).reshape((2, 3))
    hpx.exit(rtv)

@hpx.create_action()
def set_lco(lco, unused_int):
    lco.set()
    assert unused_int == 2
    return hpx.SUCCESS

@hpx.create_action()
def return_an_array():
    rtv_array = np.arange(30).reshape((5,6))
    hpx.thread_continue('array', rtv_array)
    return hpx.SUCCESS

@hpx.create_action()
def call_cc():
    hpx.call_cc(set_future, hpx.HERE())
    return hpx.SUCCESS

@hpx.create_action()
def set_future():
    rtarray = np.arange(12).reshape((3,4))
    hpx.thread_continue('array', rtarray)
    return hpx.SUCCESS

if __name__ == '__main__':
    hpx.init()
    rtv = hpx.run(main, shape=(2, 3), dtype=np.dtype(int))
    assert np.array_equal(rtv, np.arange(6).reshape((2, 3)))
    hpx.finalize()
