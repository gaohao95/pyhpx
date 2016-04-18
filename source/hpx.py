from build._hpx import ffi, lib

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


def run(action_id, *args):
    arg = ffi.new("int* ", args[0])
    lib._hpx_run(action_id, 1, arg)


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

# Define argument types
CHAR = lib.HPX_CHAR_lvalue
SHORT = lib.HPX_SHORT_lvalue
USHORT = lib.HPX_USHORT_lvalue
SSHORT = lib.HPX_SSHORT_lvalue
INT = lib.HPX_INT_lvalue
UINT = lib.HPX_UINT_lvalue
SINT = lib.HPX_SINT_lvalue
LONG = lib.HPX_LONG_lvalue
ULONG = lib.HPX_ULONG_lvalue
SLONG = lib.HPX_SLONG_lvalue
VOID = lib.HPX_VOID_lvalue
UINT8 = lib.HPX_UINT8_lvalue
SINT8 = lib.HPX_SINT8_lvalue
UINT16 = lib.HPX_UINT16_lvalue
SINT16 = lib.HPX_SINT16_lvalue
UINT32 = lib.HPX_UINT32_lvalue
SINT32 = lib.HPX_SINT32_lvalue
UINT64 = lib.HPX_UINT64_lvalue
SINT64 = lib.HPX_SINT64_lvalue
FLOAT = lib.HPX_FLOAT_lvalue
DOUBLE = lib.HPX_DOUBLE_lvalue
POINTER = lib.HPX_POINTER_lvalue
LONGDOUBLE = lib.HPX_LONGDOUBLE_lvalue
COMPLEX_FLOAT = lib.HPX_COMPLEX_FLOAT_lvalue
COMPLEX_DOUBLE = lib.HPX_COMPLEX_DOUBLE_lvalue
COMPLEX_LONGDOUBLE = lib.HPX_COMPLEX_LONGDOUBLE_lvalue

# Define a dictionary to map argument types to C definition
_c_def_map = {
    CHAR: "char",
    SHORT: "short",
    USHORT: "unsigned short",
    SSHORT: "signed short",
    INT: "int", 
    UINT: "unsigned int"
}


# Helper function to generate a suitable C function from user-defined Python function
def _generate_hpx_action(user_action, action_arguments):
    action_arguments_cdef = []
    for argument in action_arguments:
        action_arguments_cdef.append(_c_def_map[argument])
    return ffi.callback("int(" + ",".join(action_arguments_cdef) + ")")(user_action)


# Register an HPX action
# This must be called prior to hpx_init().
# @action: a Python function to be registered as a HPX action
# @action_type, @action_attribute see above
# @action_arguments a list of argument types
def register_action(action, action_type, action_attribute, action_arguments):
    action_id = ffi.new("hpx_action_t *")
    hpx_action = _generate_hpx_action(action, action_arguments)
    lib.hpx_register_action(action_type, action_attribute, b'aaa',
                            action_id, len(action_arguments) + 1, hpx_action, *action_arguments)
    return action_id
