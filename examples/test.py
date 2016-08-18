import hpx

@hpx.Action
def add(a: int, b: int) -> int:
    print(a+b)
    hpx.exit()

add_action = hpx.Action(add, hpx.ATTR_NONE, b"add", [hpx.INT, hpx.INT])
hpx.init()
hpx.run(add_action, 5, 6)

