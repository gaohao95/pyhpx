import hpx
import giltest
import sys

@hpx.create_action()
def calculate(num):
    giltest.calculate(num)
    return hpx.SUCCESS

@hpx.create_action()
def main(num_action):
    for i in range(num_action):
        calculate(hpx.HERE(), 5040//num_action) # 5040 is lcm(2,3,4,5,6,7,8,9,10,12,14,15,16)
    hpx.exit()

if __name__ == '__main__':
    hpx.init(sys.argv)
    hpx.run(main, int(sys.argv[1]))
    hpx.finalize()
