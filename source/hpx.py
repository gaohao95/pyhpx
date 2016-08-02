import sys, os
sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_LAZY)

from build._hpx import ffi, lib
import numpy as np
from abc import ABCMeta, abstractmethod

# {{{ Define HPX status
ERROR = lib.HPX_ERROR
SUCCESS = lib.HPX_SUCCESS
RESEND = lib.HPX_RESEND
LCO_ERROR = lib.HPX_LCO_ERROR
LCO_CHAN_EMPTY = lib.HPX_LCO_CHAN_EMPTY
LCO_TIMEOUT = lib.HPX_LCO_TIMEOUT
LCO_RESET = lib.HPX_LCO_RESET
ENOMEM = lib.HPX_ENOMEM
USER = lib.HPX_USER
# }}}

# {{{ Define argument types
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
# POINTER = lib.HPX_POINTER_lvalue
LONGDOUBLE = lib.HPX_LONGDOUBLE_lvalue
COMPLEX_FLOAT = lib.HPX_COMPLEX_FLOAT_lvalue
COMPLEX_DOUBLE = lib.HPX_COMPLEX_DOUBLE_lvalue
COMPLEX_LONGDOUBLE = lib.HPX_COMPLEX_LONGDOUBLE_lvalue
# ADDR = lib.HPX_ADDR_lvalue
# }}}za

# {{{ Define a dictionary to map argument types to C definition
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
    # POINTER: "void*",
    # ADDR: "hpx_addr_t"
}
# }}}


# {{{ Action

# {{{ Define action types

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

# }}}


# {{{ Define action attributes

# Null attribute.
ATTR_NONE = lib.HPX_ATTR_NONE
# Action takes a pointer to marshalled arguments and their size.
MARSHALLED = lib.HPX_MARSHALLED
# Action automatically pins memory.
PINNED = lib.HPX_PINNED

# }}}


class BaseAction(metaclass=ABCMeta):

    def __init__(self, python_func, action_type, action_attribute, action_key, action_arguments):
        """Register an HPX action.
        
        Note:
            This must be called prior to hpx_init().

        Args:
            python_func: A Python function to be registered as a HPX action
            action_type: Type of the action.
            action_attribute: Attributes of the action. 
            action_key: A Python byte object to be specified as key for this action.
            action_arguments: A Python list of argument types.
        """
        self._id = ffi.new("hpx_action_t *")
        self._arguments_cdef = []
        for argument in action_arguments:
            self._arguments_cdef.append(_c_def_map[argument])
        self._ffi_func = ffi.callback("int(" + ",".join(self._arguments_cdef) + ")")(python_func)
        lib.hpx_register_action(action_type, action_attribute, action_key,
                                self._id, len(action_arguments) + 1, 
                                self._ffi_func, *action_arguments)
        self._attribute = action_attribute

    # Helper function for generating C arguments for this action
    def _generate_c_arguments(self, *args):
        c_args = []
        for i in range(len(self._arguments_cdef)):
            c_type = self._arguments_cdef[i] + ' *'
            c_args.append(ffi.new(c_type, args[i]))
        return c_args

class Action(BaseAction):
    
    def __init__(self, python_func, action_attribute, action_key, action_arguments):
        return super(Action, self).__init__(python_func, lib.HPX_DEFAULT, 
                                                  action_attribute, action_key, 
                                                  action_arguments)

# }}}

# {{{ Runtime

def init(argv=[]):
    """Initializes the HPX runtime.
    
    This must be called before other HPX functions.  hpx_init() initializes the
    scheduler, network, and locality and should be called before any other HPX
    functions.

    Args:
        argv (list): List of command-line arguments.

    Raises:
        RuntimeError: If the initialization fails.
    """
    # TODO: remove hpx specifig flags in argv

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
        raise RuntimeError("hpx.init failed")


def exit():
    """Exit the HPX runtime.

    This causes the hpx.run() in the main native thread to return the `code`. 
    It is safe to call hpx.run() again after hpx.exit().
  
    This call does not imply that the HPX runtime has shut down. In particular,
    system threads may continue to run and execute HPX high-speed network
    progress or outstanding lightweight threads. Users should ensure that such
    concurrent activity will not create detrimental data races in their
    applications.
  
    Note:
        While this routine does not guarantee to suspend the runtime,
        high-performance implementations are expected to reduce their resource
        consumption as a result of this call. In particular, runtime-spawned
        system threads should be suspended.
    """
    lib.hpx_exit(lib.HPX_SUCCESS)


def run(action, *args):
    """Start an HPX main process.
    
    This collective creates an HPX "main" process, and calls the given `action`
    entry in the context of this process.

    The `entry` action is invoked only on the root locality and represents a
    diffusing computation.
    
    The process does not use termination detection and must be terminated
    through a single explicit call to hpx.exit().

    Args:
        action (hpx.Action): An action to execute.
        *args: The arguments of this action.

    Raise:
        RuntimeError
    """
    c_args = action._generate_c_arguments(*args)
    if lib._hpx_run(action._id, len(c_args), *c_args) != lib.HPX_SUCCESS:
        raise RuntimeError("hpx.run failed")


def finalize():
    """Finalize/cleanup from the HPX runtime.

    This function will remove almost all data structures and allocations, and
    will finalize the underlying network implementation. Note that hpx.run()
    must never be called after hpx.finalize().

    """
    lib.hpx_finalize()


def print_help():
    lib.hpx_print_help()

# }}} 

def get_num_ranks():
    return lib.hpx_get_num_ranks()


def thread_current_pid():
    return lib.hpx_thread_current_pid()


def bcast_rsync(action_id, *args):
    c_args = generate_c_arguments(action_id, *args)
    lib._hpx_process_broadcast_rsync(thread_current_pid(), action_id[0], len(c_args), *c_args)

# {{{ GlobalAddress

class GlobalAddress:

    def __init__(self, addr, bsize=-1):
        """Constructor for GlobalAddr class.

        Args:
            addr (int): The address in global memory space
            bsize (int): The block size used when allocating memory associated 
                with `addr`.
        """
        self._addr = addr
        self._bsize = bsize

    def try_pin(self, return_local=True):
        """Performs address translation.

        This will try to perform a global-to-local translation, and return the 
        address in local virtual memory space if the pin is successful and 
        `return_local` is true.
        
        If the memory of this GlobalAddr object is not local, or it is local
        and `return_local` is true but the pin fails, this will raise an
        Runtime error.

        Args:
            return_local (Optional[bool]): Whether return the local virtual
                memory correspondence.

        Returns:
            An LocalAddr object representing local memory which corresponds to 
            the given global memory if successful and `return_local` is true.
        """
        if return_local == True:
            local = ffi.new("void **")
            rtv = lib.hpx_gas_try_pin(self._addr, local)
            if rtv == False:
                raise RuntimeError("Pinning the global memory fails")
            else:
                return LocalAddr(local[0])
        else:
            rtv = lib.hpx_gas_try_pin(self._addr, ffi.NULL)
            if rtv == False:
                raise RuntimeError("Pinning the global memory fails")

    def unpin(self):
        """Unpin this address.
        """
        lib.hpx_gas_unpin(self._addr)                

def THERE(locality_number):
    """ Get the global address representing some other locality, that is
    suitable for use as a parcel target.

    Args:
        locality_number (int): The number of that locality

    Note:
        GlobalAddress object obtained from hpx.THERE cannot do arithmatic like 
        add, substract, etc.

    Returns:
        An GlobalAddr object representing that locality
    """
    return GlobalAddress(lib.HPX_THERE(locality_number))
   
def NULL():
    """ Get the equivalent of NULL for global memory

    Note:
        GlobalAddress object obtained from hpx.NULL cannot do arithmatic 
        like add, substract, etc.
    
    Returns:
        An GlobalAddress object representing the equivalent of NULL for
        global memory.
    """
    return GlobalAddress(lib.HPX_NULL) 

def HERE():
    """ Get An address representing this locality in general, that is
    suitable for use as a parcel target.

    Note:
        GlobalAddress object obtained from hpx.HERE cannot do arithmatic like
        add, substract, etc.

    Returns:
        An GlobalAddress object representing this locality.
    """
    return GlobalAddress(lib.HPX_HERE)

# }}}

class GlobalAddressBlock:
    def __init__(self, addr, blockShape, dtype):
        """Constructor of AddrBlock class

        Args:
            addr (GlobalAddr): The address in global memory space
            size (int): Total size of this address block, must be a multiple of
                the size of `dtype`.
            dtype (numpy.dtype): The type of each object in address block

        """
        self._addr = addr
        self._blockShape = blockShape
        self._dtype = dtype

    def try_pin(self, return_local=True):
        """ Performs address translation. See `Addr.try_pin` for detail.

        Returns:
            A numpy array representing this address block.
        """
        if return_local == True:
            local = ffi.new("void **")
            rtv = lib.hpx_gas_try_pin(self._addr, local)
            if rtv == False:
                raise RuntimeError("Pinning the global memory fails")
            # else:
                # block
                # return np.frombuffer(ffi.buffer(local[0], ), dtype=)
        else:
            rtv = lib.hpx_gas_try_pin(self._addr, ffi.NULL)
            if rtv == False:
                raise RuntimeError("Pinning the global memory fails")

    def unpin(self):
        """ Unpin this address block.
        """
        self.addr.unpin()


def _calculate_block_size(blockShape, dtype):
    """ Helper function for calculating block size for GAS allocation functions.
    """
    total_size = 1
    for dim in blockShape:
        total_size *= dim
    return total_size * dtype.itemsize

class GlobalMemory:

    def __init__(self, addr, numBlock, blockShape, dtype):
        self._addr = addr
        self._numBlock = numBlock
        self._blockShape = blockShape
        self._dtype = dtype

    @classmethod
    def alloc_cyclic(cls, numBlock, blockShape, dtype, boundary=0):
        """Allocate cyclically distributed global memory.
        
        Args:
            numBlock (int): The number of blocks to allocate.
            blockShape (tuple): The shape of each block.
            dtype (numpy.dtype): The data type of each entry in the block.
            boundary (int): The alignment.

        Returns:
            A GlobalMemory object representing the allocated memory.
        """
        block_size = _calculate_block_size(blockShape, dtype)
        addr = lib.hpx_gas_alloc_cyclic(numBlock, block_size, boundary) 
        return cls(addr, numBlock, blockShape, dtype)

    @classmethod
    def calloc_cyclic(cls, numBlock, blockShape, dtype, boundary=0):
        """Allocate cyclically distributed global zeroed memory.
        """
        block_size = _calculate_block_size(blockShape, dtype)
        addr = lib.hpx_gas_calloc_cyclic(numBlock, block_size, boundary)
        return cls(addr, numBlock, blockShape, dtype)

    @classmethod
    def alloc_local_at_sync(cls, numBlock, blockShape, dtype, loc, boundary=0):
        """Allocate a block of global memory.

        This is a non-collective call to allocate memory in the global
        address space that can be moved. The allocated memory, by default,
        has affinity to the allocating node, however in low memory conditions the
        allocated memory may not be local to the caller. As it allocated in the GAS,
        it is accessible from any locality, and may be relocated by the
        runtime.
        
        Args:
            loc (GlobalAddress): The address which the allocation targets.
        """
        block_size = _calculate_block_size(blockShape, dtype)
        addr = lib.hpx_gas_alloc_local_at_sync(numBlock, block_size, boundary, loc._addr)
        return cls(addr, numBlock, blockShape, dtype)
    
    @classmethod
    def alloc_local_at_async(cls, numBlock, blockShape, dtype, loc, lco, boundary=0):
        """Allocate a block of global memory asynchronously
        """
        block_size = _calculate_block_size(blockShape, dtype)
        addr = lib.hpx_gas_alloc_local_at_async(numBlock, block_size, boundary, 
                                                loc._addr, lco._addr)
        return cls(addr, numBlock, blockShape, dtype)

    def free(self, lco):
        """Free the global allocation associated with this object.
        """
        lib.hpx_gas_free(self._addr, lco._addr)

    def free_sync(self):
        lib.hpx_gas_free_sync(self._addr)

# get numpy type for a user-specified C type
def get_numpy_type(type_string):
    if ffi.typeof(type_string) is ffi.typeof("uint64_t"):
        return np.uint64
    return np.dtype((np.void, ffi.sizeof(type_string)))


def addr2buffer(addr, size):
    return ffi.buffer(addr, size)


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
        addr (GlobalAddr): The address that defines where the action is executed.
        action: The action to perform.
        result: An address of an LCO to trigger with the result.
    """
    c_args = generate_c_arguments(action_id, *args)
    rtv = lib._hpx_call(addr._addr, action_id[0], result, len(c_args), *c_args)
    if rtv != SUCCESS:
        raise RuntimeError("A problem occurs during the hpx_call invocation")
