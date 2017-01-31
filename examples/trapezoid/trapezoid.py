import hpx
import sys
import numpy as np

low = 1.0
high = 5000.0
total_cells = 10000000

def f(x):
    return 3*x**2+2*x+4

def fint(x):
    return x**3+x**2+4*x # antiderivative of f

@hpx.create_id_action(dtype=np.dtype(float))
def result_init(array):
    array[0] = 0

@hpx.create_op_action(dtype=np.dtype(float))
def result_op(array1, array2):
    array1[0] = array1[0] + array2[0]

@hpx.create_action()
def main_action():
    num_node = hpx.get_num_ranks()
    print("program runs on {0} nodes".format(num_node))
    step_size = (high - low) / total_cells
    cell_per_node = total_cells // num_node
    
    result_lco = hpx.Reduce(num_node, (1,), np.dtype(float), result_init, result_op)
    for i in range(num_node):
        calculate_integral(hpx.THERE(i), low + i*step_size*cell_per_node,
            step_size, cell_per_node, result_lco)
    print(result_lco.get())
    print(fint(high) - fint(low))
    hpx.exit()

@hpx.create_action()
def calculate_integral(start, step_size, cell_per_node, result_lco):
    result = 0
    for i in range(cell_per_node):
        s1 = f(start + i*step_size)
        s2 = f(start + (i+1)*step_size)
        result += (s1 + s2)*step_size/2
    result_lco.set(np.array([result]))
    return hpx.SUCCESS

if __name__ == '__main__':
    hpx.init(sys.argv)
    hpx.run(main_action)
    hpx.finalize()
