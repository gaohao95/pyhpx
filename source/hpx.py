import sys, os
sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_LAZY)

from build._hpx import ffi, lib
import numpy as np

# Define HPX status
ERROR = lib.HPX_ERROR
SUCCESS = lib.HPX_SUCCESS
RESEND = lib.HPX_RESEND
LCO_ERROR = lib.HPX_LCO_ERROR
LCO_CHAN_EMPTY = lib.HPX_LCO_CHAN_EMPTY
LCO_TIMEOUT = lib.HPX_LCO_TIMEOUT
LCO_RESET = lib.HPX_LCO_RESET
ENOMEM = lib.HPX_ENOMEM
USER = lib.HPX_USER

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
ADDR = lib.HPX_ADDR_lvalue


# Define a dictionary to map argument types to C definition
_c_def_map = {
    CHAR: "char",
    SHORT: "short",
    USHORT: "unsigned short",
    SSHORT: "signed short",
    INT: "int", 
    UINT: "unsigned int",
    SINT: "signed int",
    FLOAT: "float",
    DOUBLE: "double",
    POINTER: "void*",
    ADDR: "hpx_addr_t"
}


# Store registered function
_hpx_action_dict = {}


# Helper function to generate a suitable C function from user-defined Python function
def _generate_hpx_action(user_action, action_arguments):
    action_arguments_cdef = []
    for argument in action_arguments:
        action_arguments_cdef.append(_c_def_map[argument])
    return ffi.callback("int(" + ",".join(action_arguments_cdef) + ")")(user_action)


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


def register_action(action, action_type, action_attribute, action_key, action_arguments):
    """Register an HPX action.
    
    Note:
        This must be called prior to hpx_init().

    Args:
        action: A Python function to be registered as a HPX action
        action_type: Type of the action.
        action_attribute: Attributes of the action. 
        action_key: A Python byte object to be specified as key for this action.
        action_arguments: A Python list of argument types.

    Returns:
        The id of this argument.
    """
    action_id = ffi.new("hpx_action_t *")
    hpx_action = _generate_hpx_action(action, action_arguments)
    lib.hpx_register_action(action_type, action_attribute, action_key,
                            action_id, len(action_arguments) + 1, hpx_action, *action_arguments)
    _hpx_action_dict[action_id] = {'function': hpx_action, 
                                   'argument_types': action_arguments}
    return action_id


# Initializes the HPX runtime.
# This must be called before other HPX functions.
# TODO: remove hpx specifig flags in argv
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
    if lib.hpx_init(c_argc, c_argv_address) != SUCCESS:
        raise RuntimeError("hpx_init failed")

# Exit the HPX runtime.
def exit(code):
    lib.hpx_exit(code)

# Helper function for generating C arguments for the action corresponds to action_id
def generate_c_arguments(action_id, *args):
    argument_types = _hpx_action_dict[action_id]['argument_types']
    c_args = []
    for i in range(len(argument_types)):
        if argument_types[i] != POINTER:
            c_type = _c_def_map[argument_types[i]] + ' *'
        else:
            c_type = 'hpx_addr_t *'
        c_args.append(ffi.new(c_type, args[i]))
    return c_args

def run(action_id, *args):
    c_args = generate_c_arguments(action_id, *args)
    lib._hpx_run(action_id, len(c_args), *c_args)    

def finalize():
    lib.hpx_finalize()

def print_help():
    lib.hpx_print_help()

def get_num_ranks():
    return lib.hpx_get_num_ranks()

def thread_current_pid():
    return lib.hpx_thread_current_pid()

def bcast_rsync(action_id, *args):
    c_args = generate_c_arguments(action_id, *args)
    lib._hpx_process_broadcast_rsync(thread_current_pid(), action_id[0], len(c_args), *c_args)

# lower_level api for gas
def gas_alloc_local_at_sync(n, bsize, boundary, loc):
    return lib.hpx_gas_alloc_local_at_sync(n, bsize, boundary, loc)

def locality_address(locality_no):
    return lib.HPX_THERE(locality_no)

NULL = lib.HPX_NULL

# get numpy type for a user-specified C type
def get_numpy_type(type_string):
    if ffi.typeof(type_string) is ffi.typeof("uint64_t"):
        return np.uint64
    return np.dtype((np.void, ffi.sizeof(type_string)))

def gas_memput_rsync(to_addr, from_addr, size):
    from_addr_ptr = ffi.cast("void *", from_addr)
    if lib.hpx_gas_memput_rsync(to_addr, from_addr_ptr, size) != SUCCESS:
        raise RuntimeError("Fail to copy data from a local buffer to a global address")


def gas_try_pin(addr, return_local=True):
    """Performs address translation.

    This will try to perform a global-to-local translation on the global @p
    addr, and return the address in local virtual memory space if the pin is
    successful and @p return_local is true.
    
    If @p addr is not local, or @p addr is local and return_local is true but 
    the pin fails, this will raise an Runtime error.

    Args:
        addr: The address in global memory space to be translated.
        return_local: Whether return the local virtual memory correspondence.

    Returns:
        Local memory which corresponds to the given global memory if successful
        and return_local is true.

    """
    if return_local == True:
        local = ffi.new("void **")
        rtv = lib.hpx_gas_try_pin(addr, local)
        if rtv == False:
            raise RuntimeError("Pinning the global memory fails")
        else:
            return local[0]
    else:
        rtv = lib.hpx_gas_try_pin(addr, ffi.NULL)
        if rtv == False:
            raise RuntimeError("Pinning the global memory fails")


def addr2buffer(addr, size):
    return ffi.buffer(addr, size)


def gas_unpin(addr):
    """Unpin a previously pinned block.

    Args:
        addr: The address of global memory to unpin.
    """
    lib.hpx_gas_unpin(addr)


def lco_future_new(size):
    """Create a future.
    
    Futures are builtin LCOs that represent values returned from asynchronous
    computation.
    Futures are always allocated in the global address space, because their
    addresses are used as the targets of parcels.

    Args:
        size: The size in bytes of the future's value (may be 0)
    Returns:
        The global address of the newly allocated future
    """
    return lib.hpx_lco_future_new(size)


def lco_wait(lco):
    """Perform a wait operation.

    The LCO blocks the caller until an LCO set operation triggers the LCO. Each
    LCO type has its own semantics for the state under which this occurs.
    
    Args:
        lco: The LCO we're processing
    """
    rtv = lib.hpx_lco_wait(lco)
    if rtv != SUCCESS:
        raise RuntimeError("LCO error")


def lco_delete_sync(lco):
    """Delete an LCO synchronously.

    Args:
        lco: The address of the LCO to delete
    """
    lib.hpx_lco_delete_sync(lco)


def call(addr, action_id, result, *args):
    """Locally synchronous call interface.

    This is a locally-synchronous, globally-asynchronous variant of
    the remote-procedure call interface. If @p result is not hpx.NULL,
    hpx_call puts the the resulting value in @p result at some point
    in the future.
    
    Args:
        addr: The address that defines where the action is executed.
        action: The action to perform.
        result: An address of an LCO to trigger with the result.
    """
    c_args = generate_c_arguments(action_id, *args)
    rtv = lib._hpx_call(addr, action_id, result, len(c_args), *c_args)
    if rtv != SUCCESS:
        raise RuntimeError("A problem occurs during the hpx_call invocation")