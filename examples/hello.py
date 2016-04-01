import hpx


# Set up the callback function
def add(a, b, message):
    print(a + b)
    print(message)
    hpx.exit(0)

# Register action
add_action = hpx.register_action(add, hpx.DEFAULT, hpx.ATTR_NONE)

# Initialize HPX runtime and run the action
hpx.init()
hpx.run(add_action, 5, 6, "Hello World!")
hpx.finalize()
