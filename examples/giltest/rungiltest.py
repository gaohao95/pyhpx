import hpx
import giltest
import sys
import numpy as np

num_action = 16

@hpx.create_action()
def calculate(num_gil):
    for i in range(num_gil):
        giltest.calculate(262144//num_gil) # 262144 is 2^18
    return hpx.SUCCESS

@hpx.create_action()
def main():
    time = np.zeros((18,))
    for j in range(18):
        num_gil = 2**j
        start = hpx.time_now()
        and_lco = hpx.And(num_action)
        for i in range(num_action):
            calculate(hpx.HERE(), num_gil, rsync_lco=and_lco)
        and_lco.wait()
        current_time = hpx.time_elapsed_ms(start)
        print(current_time)
        time[j] = current_time
    
    print(time)
    time.dump("time.bin")
    hpx.exit()

if __name__ == '__main__':
    hpx.init(sys.argv)
    hpx.run(main)
    hpx.finalize()
