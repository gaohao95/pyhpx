from build._hpx import ffi, lib

# object_set store the reference to C memory so that it is not garbage collected by Python
object_set = set()


# Initializes the HPX runtime.
# This must be called before other HPX functions.
def init(argv=[]):
    if len(argv) == 0:
        c_argc = ffi.NULL
        c_argv_address = ffi.NULL
    else:
        c_argc = ffi.new("int *", len(argv))
        c_argv = ffi.new("char* []", len(argv))
        c_argv_obj = [] # store the reference to make sure it is not garbage collected
        for i in range(len(argv)):
            c_argv_obj.append(ffi.new("char[]", argv[i].encode('ascii')))
            c_argv[i] = c_argv_obj[i]
        c_argv_address = ffi.new("char ***", c_argv)
    if lib.hpx_init(c_argc, c_argv_address) != lib.HPX_SUCCESS:
        raise RuntimeError("hpx_init failed")


# Exit the HPX runtime.
def exit(code):
    lib.hpx_exit(code)


def run(action_id, *args, **kwargs):
    args_handle = ffi.new_handle(args)
    object_set.add(args_handle)
    args_handle_address = ffi.new("void**", args_handle)
    # object_set.add(args_handle_address) ?? necessary ??
    lib._hpx_run(action_id, 1, args_handle_address)


def finalize():
    lib.hpx_finalize()


# Define action types
# Standard action that is scheduled and has its own stack.
DEFAULT = lib.HPX_DEFAULT
# Tasks are threads that do not block.
TASK = lib.HPX_TASK
# Interrupts are simple actions that have function call semantics.
INTERRUPT = lib.HPX_INTERRUPT
# Functions are simple functions that have uniform ids across localities,
# but can not be called with the set of hpx_call operations
# or as the action or continuation in a parcel.
FUNCTION = lib.HPX_FUNCTION
# Action that runs OpenCL kernels
OPENCL = lib.HPX_OPENCL


# Define action attributes
# Null attribute.
ATTR_NONE = lib.HPX_ATTR_NONE
# Action takes a pointer to marshalled arguments and their size.
MARSHALLED = lib.HPX_MARSHALLED
# Action automatically pins memory.
PINNED = lib.HPX_PINNED
# Action is a libhpx action
INTERNAL = lib.HPX_INTERNAL
# Action is a vectored action
VECTORED = lib.HPX_VECTORED
# Action is a coalesced action
COALESCED = lib.HPX_COALESCED
# Action is a compressed action
COMPRESSED = lib.HPX_COMPRESSED


# Helper function to generate a suitable C function from user-defined Python function
def _generate_hpx_action(user_action):
    def hpx_action(user_args_handle):
        user_args = ffi.from_handle(user_args_handle)
        object_set.remove(user_args_handle)
        return user_action(*user_args)
    return ffi.callback("int(void*)")(hpx_action)


# Register an HPX action
# This must be called prior to hpx_init().
def register_action(action, action_type, action_attribute):
    action_id = ffi.new("hpx_action_t *")
    hpx_action = _generate_hpx_action(action)
    object_set.add(hpx_action)
    lib.hpx_register_action(action_type, action_attribute, b'aaa',
                            action_id, 2, hpx_action, lib.HPX_POINTER_lvalue)
    return action_id
