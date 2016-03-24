from _hpx import lib, ffi


@ffi.callback("void(void)")
def _hello_action():
    print("Hello World!")
    lib.hpx_exit(0)


_hello = ffi.new("hpx_action_t *")
action_key = ffi.new("char[]", b"aaa")
lib.hpx_register_action(lib.HPX_DEFAULT, 0, action_key, _hello, 1, _hello_action)
lib.hpx_init(ffi.NULL, ffi.NULL)
lib._hpx_run(_hello, 0)
lib.hpx_finalize()

