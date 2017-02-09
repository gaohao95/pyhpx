import hpx
import sys
import numpy as np

@hpx.create_action()
def svd(num_mat):
    for i in range(num_mat):
        array = np.random.rand(128,128)
        u, s, v = np.linalg.svd(array)
    return hpx.SUCCESS

@hpx.create_action()
def main(num_worker):
    # total = 232792560 # lcm up to 20
    total = 1260
    for i in range(num_worker):
        svd(hpx.HERE(), total//num_worker)
    hpx.exit()

if __name__ == '__main__':
    hpx.init(sys.argv)
    hpx.run(main, int(sys.argv[1]))
    hpx.finalize()