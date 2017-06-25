import os
import subprocess
import numpy as np

l = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
time = np.zeros((16,10))

for num_run in range(10):
    for i in range(len(l)):
        num_thread = l[i]
        finished_process = subprocess.run(["./ctest", str(num_thread), "--hpx-threads=" + str(num_thread)], stdout=subprocess.PIPE)
        current_time = float(str(finished_process.stdout, 'utf-8'))
        print(current_time)
        time[i][num_run] = current_time

print(time)
time.dump("time_c.bin")
