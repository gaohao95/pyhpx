import hpx

@hpx.create_action()
def main():
    future = hpx.Future()
    set_lco(hpx.HERE(), future, 2)
    future.wait()
    hpx.exit()

@hpx.create_action()
def set_lco(lco, unused_int):
    lco.set()
    assert unused_int == 2
    return hpx.SUCCESS

if __name__ == '__main__':
    hpx.init()
    hpx.run(main)
    hpx.finalize()
