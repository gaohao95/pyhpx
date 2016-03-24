from _hpx import lib, ffi


@ffi.callback("void(int, int)")
def add_int_func(v1, v2):
    print(v1 + v2)
    lib.hpx_exit(0)


add_int = ffi.new("hpx_action_t *")
action_key = ffi.new("char[]", b"aaa")
lib.hpx_register_action(lib.HPX_DEFAULT, 0, action_key, add_int, 3,
                        add_int_func, lib.HPX_INT_lvalue, lib.HPX_INT_lvalue)
lib.hpx_init(ffi.NULL, ffi.NULL)
lib._hpx_run(add_int, 2, ffi.cast("int", 42), ffi.cast("int", 21))
lib.hpx_finalize()
