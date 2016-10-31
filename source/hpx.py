import sys, os
sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_LAZY)

from build._hpx import ffi, lib
import numpy as np
from abc import ABCMeta, abstractmethod
from collections import deque
import pickle

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
class Type:
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
    # COMPLEX_FLOAT = lib.HPX_COMPLEX_FLOAT_lvalue
    # COMPLEX_DOUBLE = lib.HPX_COMPLEX_DOUBLE_lvalue
    # COMPLEX_LONGDOUBLE = lib.HPX_COMPLEX_LONGDOUBLE_lvalue
    ADDR = lib.HPX_ADDR_lvalue
    SIZE_T = lib.HPX_SIZE_T_lvalue
    LCO = lib.HPX_ADDR_lvalue

# }}}

# {{{ Define a dictionary to map argument types to C definition

_c_def_map = {
    Type.CHAR: "char",
    Type.SHORT: "short",
    Type.USHORT: "unsigned short",
    Type.SSHORT: "signed short",
    Type.INT: "int", 
    Type.UINT: "unsigned int",
    Type.SINT: "signed int",
    Type.FLOAT: "float",
    Type.DOUBLE: "double",
    Type.POINTER: "void*",
    Type.SIZE_T: "size_t",
    Type.ADDR: "hpx_addr_t",
    Type.LCO: "hpx_addr_t"
}

# }}}

# {{{ Action

# {{{ Define action attributes

# Null attribute.
ATTR_NONE = lib.HPX_ATTR_NONE
# Action takes a pointer to marshalled arguments and their size.
MARSHALLED = lib.HPX_MARSHALLED
# Action automatically pins memory.
PINNED = lib.HPX_PINNED

# }}}

class BaseAction(metaclass=ABCMeta):

    @abstractmethod
    def __init__(self, python_func, action_type, key, marshalled, pinned, 
                 argument_types):
        """Register an HPX action.
        
        Note:
            This must be called prior to hpx_init().

        Args:
            python_func: A Python function to be registered as a HPX action
            action_type: Type of the action.
            key: A Python byte object to be specified as key for this action.
            argument_types: A Python list of argument types.
        """
        self.id = ffi.new("hpx_action_t *")
        
        # generate default key if not specified
        if key is None:
            key = ((python_func.__module__ + ":" + python_func.__name__)
                  .encode('ascii'))

        self.marshalled = marshalled
        self.pinned = pinned

        if marshalled:
            def callback_func(pointer, size):
                args_bytes = ffi.buffer(pointer, size)[:]
                args = pickle.loads(args_bytes)
                if pinned:
                    argslist = list(args)
                    target = argslist[0]
                    argslist[0] = target.try_pin()
                    args = tuple(argslist)
                rtv = python_func(*args)
                if pinned:
                    target = target.unpin()
                return rtv
            self._ffi_func = ffi.callback("int (void*, size_t)")(callback_func)
            lib.hpx_register_action(action_type, lib.HPX_MARSHALLED, key, 
                                    self.id, 3, self._ffi_func, 
                                    Type.POINTER, Type.SIZE_T)
        else:
            self._arguments_cdef = []
            for argument in argument_types:
                if isinstance(argument, tuple):
                    if argument[0] != Type.LCO:
                        raise TypeError("The first entry in a tuple argument should be Type.LCO")
                    elif argument[1] is not None and not isinstance(argument[1], tuple):
                        raise TypeError("The second entry in a tuple argument should be None or a tuple")
                    else:
                        pass
                        # TODO: implement support LCO type
                else:
                    self._arguments_cdef.append(_c_def_map[argument])
            
            if action_type == lib.HPX_FUNCTION:
                self._ffi_func = ffi.callback("void(" + ",".join(self._arguments_cdef) + ")")(python_func)
            else:
                self._ffi_func = ffi.callback("int(" + ",".join(self._arguments_cdef) + ")")(python_func)
            
            lib.hpx_register_action(action_type, lib.HPX_ATTR_NONE, key,
                                    self.id, len(argument_types) + 1, 
                                    self._ffi_func, *argument_types)

    # Helper function for generating C arguments for this action
    def _generate_c_arguments(self, *args):
        c_args = []
        for i in range(len(self._arguments_cdef)):
            c_type = self._arguments_cdef[i] + ' *'
            if self._arguments_cdef[i] == 'hpx_addr_t':
                c_args.append(ffi.new('hpx_addr_t *', args[i].addr))
            else:
                c_args.append(ffi.new(c_type, args[i]))
        return c_args

    def __call__(self, target_addr, *args, sync='lsync', gate=None, 
                 lsync_lco=None, rsync_lco=None):
        """
        if this action is pinned, target_addr must be a GlobalAddressBlock, 
        otherwise can be either GlobalAddressBlock or GlobalAddress.

        if target_addr is hpx.NULL(), then the action is launched on every
        locality of this process.
        """
        if self.marshalled:
            pointer, size = _parse_marshalled_args(args)
        else:
            c_args = self._generate_c_arguments(*args)

        lsync_addr = _get_lco_addr(lsync_lco)
        rsync_addr = _get_lco_addr(rsync_lco)

        if (isinstance(target_addr, GlobalAddress) and 
            target_addr.addr == lib.HPX_NULL):
            if self.pinned:
                raise RuntimeError("Pinned action is not supported for"
                        "broadcast.")
            if sync == 'lsync':
                if self.marshalled:
                    lib._hpx_process_broadcast_lsync(
                            lib.hpx_thread_current_pid(), self.id[0], 
                            rsync_addr, 2, pointer, size)
                else:
                    lib._hpx_process_broadcast_lsync(
                            lib.hpx_thread_current_pid(), self.id[0],
                            rsync_addr, len(c_args), *c_args)
            elif sync == 'rsync':
                if self.marshalled:
                    lib._hpx_process_broadcast_rsync(
                            lib.hpx_thread_current_pid(), self.id[0],
                            2, pointer, size)
                else:
                    lib._hpx_process_broadcast_rsync(
                            lib.hpx_thread_current_pid(), self.id[0],
                            len(c_args), *c_args)
            elif sync == 'async':
                if self.marshalled:
                    lib._hpx_process_broadcast(lib.hpx_thread_current_pid(),
                        self.id[0], lsync_addr, rsync_addr, 2, pointer, size)
                else:
                    lib._hpx_process_broadcast(lib.hpx_thread_current_pid(),
                        self.id[0], lsync_addr, rsync_addr,
                        len(c_args), *args)
            elif isinstance(sync, str):
                raise NameError("unrecognized string for sync argument")
            else:
                raise TypeError("sync argument should be of type str")
            return

        # get the address of target_addr of type int
        if isinstance(target_addr, GlobalAddressBlock):
            target_addr_int = target_addr.addr.addr
        elif isinstance(target_addr, GlobalAddress):
            target_addr_int = target_addr.addr
        else:
            raise TypeError("target_addr must be either GlobalAddressBlock or"
                            "GlobalAddress")

        if gate is None:
            if sync == 'lsync':
                if self.marshalled:
                    if self.pinned:
                        if not isinstance(target_addr, GlobalAddressBlock):
                            raise TypeError("target_addr is not GlobalAddressBlock object") 
                        expandargs = list(args)
                        expandargs.insert(0, target_addr)
                        args = tuple(expandargs)
                    pointer, size = _parse_marshalled_args(args)
                    lib._hpx_call(target_addr_int, self.id[0], rsync_addr, 2, pointer, size)
                else:
                    lib._hpx_call(target_addr_int, self.id[0], rsync_addr, len(c_args), *args)
            elif sync == 'rsync':
                # How can user set the return value ?????
                # TODO: handle the return value
                lib._hpx_call_sync(target_addr.addr, self._id[0], ffi.NULL, 0, len(c_args), *args)
            elif sync == 'async':
                lib._hpx_call_async(target_addr.addr, self._id[0], lsync_addr, rsync_addr,
                        len(c_args), *args)
            elif isinstance(sync, str):
                raise ValueError("sync argument not recognizable")
            else:
                raise TypeError("sync argument should be of type str")
        elif isinstance(gate, LCO):
            pass
        else:
            raise TypeError("gate should be an instance of LCO")


class Action(BaseAction):
    def __init__(self, python_func, key=None, marshalled=True, pinned=False, 
                 argument_types=None):
        return super(Action, self).__init__(python_func, lib.HPX_DEFAULT, key, 
                                            marshalled, pinned, argument_types)

def create_action(key=None, marshalled=True, pinned=False, 
                  argument_types=None):
    def decorator(python_func):
        return Action(python_func, key, marshalled, pinned, argument_types)
    return decorator


class Function(BaseAction):
    def __init__(self, python_func, key=None, marshalled=True, pinned=False,
                 argument_types=None):
        return super(Function, self).__init__(python_func, lib.HPX_FUNCTION, 
                                              key, marshalled, pinned,
                                              argument_types) 
    
    def __call__(self, *args):
        raise RuntimeError("Funtion action is not callable")

def create_function(key=None, marshalled=True, pinned=False,
                    argument_types=None):
    def decorator(python_func):
        return Function(python_func, key, marshalled, pinned, argument_types)
    return decorator

def _parse_marshalled_args(args):
    args_bytes = bytearray(pickle.dumps(args))
    pointer = ffi.from_buffer(args_bytes)
    size = ffi.cast("size_t", sys.getsizeof(args_bytes))
    return pointer, size

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

def exit(array=None):
    """Exit the HPX runtime.
  
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
    if array is not None:
        size = array.size * array.dtype.itemsize
        lib.hpx_exit(size, array.__array_interface__['data'][0])
    else:
        lib.hpx_exit(0, ffi.NULL)

def run(action, *args, shape=None, dtype=None):
    """Start an HPX main process.
    
    This collective creates an HPX "main" process, and calls the given `action`
    entry in the context of this process.
    
    The process does not use termination detection and must be terminated
    through a single explicit call to hpx.exit().

    Args:
        action (hpx.BaseAction): An action to execute.
        *args: The arguments of this action.
    """
    if action.marshalled:
        args_pointer, size = _parse_marshalled_args(args) 
    else:
        c_args = action._generate_c_arguments(*args)

    if shape is None:
        if action.marshalled:
            status = lib._hpx_run(action.id, ffi.NULL, 2, args_pointer, size) 
        else:
            status = lib._hpx_run(action.id, ffi.NULL, len(c_args), *c_args)
    else:
        rtv = np.zeros(shape, dtype=dtype)
        rtv_pointer = ffi.cast("void*", rtv.__array_interface__['data'][0])
        status = lib._hpx_run(action.id, rtv_pointer, len(c_args), *c_args)

    if status != lib.HPX_SUCCESS:
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
        self.addr = addr
        self.bsize = bsize

    def __add__(self, bytes):
        """Perform global address displacement arithmetic.

        Get the address of `bytes` into memory with address `addr`. As with 
        normal C pointer arithmetic, the `bytes` displacement must result in an
        address associated with the same allocation as `addr`, or 
        one-off-the-end if the allocation was an array.
        """
        return GlobalAddress(lib.hpx_addr_add(self.addr, bytes, self.bsize), 
                             self.bsize)

    def __radd__(self, bytes):
        """See GlobalAddress.__add__ for details.
        """
        return self.__add__(bytes)

    def __sub__(self, other):
        """
        Depending on the type of other, this method either performs global 
        address displacement arithmetic or distance arithmetic.

        If other is of type int, this performs displacement arithmetic. If 
        other is another GlobalAddress object, the distance arithmetic will be
        performed. In addition, self and other must be part of the same 
        allocation.
        """
        if isinstance(other, int):
            return GlobalAddress(lib.hpx_addr_add(self.addr, -other, self.bsize), self.bsize)
        elif isinstance(other, GlobalAddress):
            return lib.hpx_addr_sub(self.addr, other.addr, self.bsize)
        else:
            raise TypeError("Invalid data type")

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
            Local memory which corresponds to the given global memory if 
            successful and `return_local` is true.
        """
        if return_local == True:
            local = ffi.new("void **")
            rtv = lib.hpx_gas_try_pin(self.addr, local)
            if rtv == False:
                raise RuntimeError("Pinning the global memory fails")
            else:
                return local[0]
        else:
            rtv = lib.hpx_gas_try_pin(self._addr, ffi.NULL)
            if rtv == False:
                raise RuntimeError("Pinning the global memory fails")

    def unpin(self):
        """Unpin this address.
        """
        lib.hpx_gas_unpin(self.addr)                

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

# {{{ GlobalAddressBlock

def _currentdim_is_slice(sliceObj, dimLimit, dimStride, dimOffset):
    if sliceObj.start == None:
        start = 0
    else:
        start = sliceObj.start
    if sliceObj.stop == None:
        stop = dimLimit
    else:
        stop = sliceObj.stop
    return stop - start, dimOffset + start * dimStride

class GlobalAddressBlock:
    def __init__(self, addr, shape, dtype, strides, offsets):
        """Constructor of AddrBlock class

        Args:
            addr (GlobalAddr): The starting address of this block is given by 
                self.addr + self.offsets[0]. self.addr will not change for 
                indexing.
            size (int): Total size of this address block, must be a multiple of
                the size of `dtype`.
            dtype (numpy.dtype): The type of each object in address block
        """
        self.addr = addr
        self.shape = shape
        self.dtype = dtype
        self.strides = strides
        self.offsets = offsets
    
    def __getitem__(self, key):
        if type(key) is int or type(key) is slice:
            keyWrap = (key,)
        elif type(key) is tuple:
            keyWrap = key
        else:
            raise TypeError("Invalid key type")
        
        newShape = list(self.shape)
        newOffsets = list(self.offsets)
        for i in range(len(keyWrap)):
            if isinstance(keyWrap[i], int):
                newShape[i] = 1
                newOffsets[i] = self.offsets[i] + keyWrap[i]*self.strides[i]
            elif isinstance(keyWrap[i], slice):
                currentLength, currentOffset = _currentdim_is_slice(
                        keyWrap[i], self.shape[i], self.strides[i], 
                        self.offsets[i])
                newShape[i] = currentLength
                newOffsets[i] = currentOffset
            else:
                raise TypeError("Invalid key type in dimension " + str(i))

        newShape = tuple(newShape)
        newOffsets = tuple(newOffsets)
        
        return GlobalAddressBlock(self.addr, newShape, self.dtype, 
                self.strides, newOffsets)  

    def try_pin(self, return_local=True):
        """ Performs address translation. See `Addr.try_pin` for detail.

        Returns:
            A numpy array representing this address block.
        """
        if return_local == True:
            addrLocal = self.addr.try_pin(True)
            size = self.offsets[0] + self.strides[0] * self.shape[0]
            array = np.frombuffer(ffi.buffer(addrLocal, size), dtype=self.dtype)
            
            # reshape the array
            bigShape = [self.offsets[0] // self.strides[0] + self.shape[0]]
            for i in range(len(self.shape) - 1):
                bigShape.append(self.strides[i] // self.strides[i+1])
            bigShape = tuple(bigShape)
            array = array.reshape(bigShape)

            indexing = []
            for i in range(len(self.shape)):
                start = self.offsets[i] // self.strides[i]
                stop = start + self.shape[i]
                indexing.append(slice(start, stop, None))
            indexing = tuple(indexing)
            
            return array[indexing]
        else:
            self.addr.try_pin(False)

    def unpin(self):
        """ Unpin this address block.
        """
        self.addr.unpin()

# }}}

# {{{ GlobalMemory

def _calculate_block_size(blockShape, dtype):
    """ Helper function for calculating block size for GAS allocation functions.
    """
    total_size = 1
    for dim in blockShape:
        total_size *= dim
    return total_size * dtype.itemsize

class GlobalMemory:

    def __init__(self, addr, numBlock, blockShape, dtype, strides, offsets):
        """
        Args:
            addr (GlobalAddress): A GlobalAddress object representing the 
                beginning of the allocated memory. Indexing should not change 
                this value.
            strides (tuple): Memory increment for each dimension. This should
                not change after the initial allocation.
            offset (tuple): Memory offset for each dimension. 
        """
        self.addr = addr
        self.shape = (numBlock,) + blockShape 
        self.dtype = dtype
        self.strides = strides
        self.offsets = offsets
        
    def _calculate_strides(blockShape, dtype):
        strides = deque([dtype.itemsize])
        for i in range(len(blockShape)-1, -1, -1):
            strides.appendleft(strides[0] * blockShape[i])
        return tuple(strides)

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
        strides = GlobalMemory._calculate_strides(blockShape, dtype)
        return cls(GlobalAddress(addr, block_size), numBlock, blockShape, 
            dtype, strides, (0,)*(len(blockShape) + 1))

    @classmethod
    def calloc_cyclic(cls, numBlock, blockShape, dtype, boundary=0):
        """Allocate cyclically distributed global zeroed memory.
        """
        block_size = _calculate_block_size(blockShape, dtype)
        addr = lib.hpx_gas_calloc_cyclic(numBlock, block_size, boundary)
        strides = GlobalMemory._calculate_strides(blockShape, dtype)
        return cls(GlobalAddress(addr, block_size), numBlock, blockShape, 
            dtype, strides, (0,)*(len(blockShape) + 1))

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
        addr = lib.hpx_gas_alloc_local_at_sync(numBlock, block_size, boundary, loc.addr)
        strides = GlobalMemory._calculate_strides(blockShape, dtype)
        return cls(GlobalAddress(addr, block_size), numBlock, blockShape, 
            dtype, strides, (0,)*(len(blockShape) + 1))

    @classmethod
    def alloc_local_at_async(cls, numBlock, blockShape, dtype, loc, lco, boundary=0):
        """Allocate a block of global memory asynchronously
        """
        block_size = _calculate_block_size(blockShape, dtype)
        addr = lib.hpx_gas_alloc_local_at_async(numBlock, block_size, boundary, 
                                                loc.addr, lco.addr)
        strides = GlobalMemory._calculate_strides(blockShape, dtype)
        return cls(GlobalAddress(addr, block_size), numBlock, blockShape, 
            dtype, strides, (0,)*(len(blockShape) + 1))

    def free(self, lco):
        """Free the global allocation associated with this object.

        Note that even if the current object is created by indexing, and hence 
        only part of the initial allocation, all memory of the initial 
        allocation will be freed.
        """
        lib.hpx_gas_free(self.addr.addr, lco.addr)

    def free_sync(self):
        lib.hpx_gas_free_sync(self.addr.addr)

    def _check_index(self, index):
        if index >= self.numBlock:
            raise IndexError("Invalid index")

    def __getitem__(self, key):
        if type(key) is tuple:
            if len(key) == 0:
                return self
            if type(key[0]) is int:
                return GlobalAddressBlock(
                        self.addr + self.offsets[0] + key[0] * self.strides[0],
                        self.shape[1:], self.dtype, self.strides[1:], 
                        self.offsets[1:])[key[1:]]
            elif type(key[0]) is slice:
                newShape = list(self.shape)
                newOffsets = list(self.offsets)
                for i in range(len(key)):
                    if type(key[i]) is slice:
                        newDimLength, newDimOffset = _currentdim_is_slice(
                                key[i], self.shape[i], self.strides[i], 
                                self.offsets[i])
                        newShape[i] = newDimLength
                        newOffsets[i] = newDimOffset
                    elif type(key[i]) is int:
                        newShape[i] = 1
                        newOffsets[i] = self.offsets[i] + key[i] * self.strides[i]
                    else:
                        raise TypeError("Invalid key type on dimension {0}.".format(i)) 
                newShape = tuple(newShape)
                newOffsets = tuple(newOffsets)
                return GlobalMemory(self.addr, newShape[0], newShape[1:], 
                    self.dtype, self.strides, newOffsets)
            else:
                raise TypeError("Invalid key type")
        elif type(key) is slice:
            newNumBlock, newFirstDimOffset = _currentdim_is_slice(
                    key, self.shape[0], self.strides[0], self.offsets[0])
            newOffsets = list(self.offsets)
            newOffsets[0] = newFirstDimOffset
            newOffsets = tuple(newOffsets)
            return GlobalMemory(self.addr, newNumBlock, self.shape[1:], 
                self.dtype, self.strides, newOffsets)
        elif type(key) is int:
            return GlobalAddressBlock(
                    self.addr + self.offsets[0] + key * self.strides[0],
                    self.shape[1:], self.dtype, self.strides[1:],
                    self.offsets[1:])
        elif key == None:
            return self
        else:
            raise TypeError("Invalid key type")

# }}}

# get numpy type for a user-specified C type
def get_numpy_type(type_string):
    if ffi.typeof(type_string) is ffi.typeof("uint64_t"):
        return np.uint64
    return np.dtype((np.void, ffi.sizeof(type_string)))

# {{{ LCO

def _get_lco_addr(lco_obj):
    """
    Helper function to get the address of an LCO object. If the LCO object is 
    None, return HPX_NULL.
    """
    if lco_obj is None:
        addr = lib.HPX_NULL
    else:
        if not isinstance(lco_obj, LCO):
            raise TypeError("expect type hpx.LCO, got '{0}'".format(type(lco_obj).__name__))
        else:
            addr = lco_obj.addr
    return addr

class LCO(metaclass=ABCMeta):

    @abstractmethod
    def __init__(self, addr, shape=None, dtype=None):
        """
        Args:
            addr (hpx_addr_t): The global address of this LCO.
            shape (int): The shape of stored numpy array of this LCO. If this 
            LCO does not have an associated buffer, shape is set to None. 
        """
        self.addr = addr
        self.shape = shape
        self.dtype = dtype
        if shape is not None:
            self.size = _calculate_block_size(shape, dtype)
        else:
            self.size = 0

    def delete(self, rsync):
        """
        Args:
            rsync (LCO): An LCO to signal remote completion.
        """
        lib.hpx_lco_delete(self.addr, rsync.addr)

    def delete_sync(self):
        lib.hpx_lco_delete_sync(self.addr)
    

    def set(self, array=None, sync='rsync', lsync_lco=None, rsync_lco=None):
        """
        The argument `sync` can be 'rsync', 'lsync', or 'async'. If `sync` is 
        'rsync', this call is fully synchronous, the argument `lsync_lco` and 
        `rsync_lco` is ommited. If `sync` is 'lsync', this call is locally 
        synchronous, that is the method will not return until `array` can be 
        modified. In this case, `rsync_lco` can be optionally set to a LCO 
        object to wait for completion. If `sync` is 'async', this call is 
        asynchronous. In this case, `lsync_lco` can be optionally set to a LCO
        object to wait for local completion, and `rsync_lco` can be optionally
        set to a LCO object to wait for remote completion.
        """
        if array is not None:
            pointer_to_data = ffi.cast("void*", array.__array_interface__['data'][0])
        else:
            pointer_to_data = ffi.NULL
        if sync == 'rsync':
            lib.hpx_lco_set_rsync(self.addr, self.size, pointer_to_data)
        elif sync == 'lsync':
            rsync_addr = _get_lco_addr(rsync_lco)
            lib.hpx_lco_set_lsync(self.addr, self.size, pointer_to_data, rsync_addr)
        elif sync == 'async':
            lsync_addr = _get_lco_addr(lsync_lco)
            rsync_addr = _get_lco_addr(rsync_lco)
            lib.hpx_lco_set(self.addr, self.size, pointer_to_data, lsync_addr, rsync_addr)
        elif isinstance(sync, str):
            raise ValueError("sync value not supported")
        else:
            raise TypeError("sync argument should be a string")

    def wait(self):
        lib.hpx_lco_wait(self.addr)

    def get(self):
        """
        TODO: error handling
        """
        return_array = np.zeros(self.shape, dtype=self.dtype)
        pointer_to_data = ffi.cast("void*", return_array.__array_interface__['data'][0])
        lib.hpx_lco_get(self.addr, self.size, pointer_to_data)
        return return_array

# {{{ And LCO
class And(LCO):

    def __init__(self, num):
        addr = lib.hpx_lco_and_new(num)
        super(And, self).__init__(addr, None, None)

    def set(self, sync=None):
        if sync != None:
            lib.hpx_lco_and_set(self.addr, sync.addr)
        else:
            lib.hpx_lco_and_set(self.addr, lib.HPX_NULL) 

    def set_num(self, num, sync=None):
        if sync != None:
            lib.hpx_lco_and_set_num(self.addr, num, sync.addr)
        else:
            lib.hpx_lco_and_set_num(self.addr, num, lib.HPX_NULL)

# }}}

class Future(LCO): 
    def __init__(self, shape=None, dtype=None):
        """
        If shape is None, this Future LCO does not has the associated buffer.
        """
        if shape is not None:
            size = _calculate_block_size(shape, dtype)
        else:
            size = 0
        addr = lib.hpx_lco_future_new(size)
        super(Future, self).__init__(addr, shape, dtype)

def create_id_action(dtype, shape=None):
    def decorator(python_func):
        @create_function(marshalled=False,
                         argument_types=[Type.POINTER, Type.SIZE_T])
        def callback_action(pointer, size):
            buf = ffi.buffer(pointer, size)
            array = np.frombuffer(buf, dtype=dtype)
            if shape is not None:
                array = array.reshape(shape)
            rtn = python_func(array)
            if rtn is not None:
                array[:] = rtn
        return callback_action
    return decorator

def create_op_action(dtype, shape=None):
    def decorator(python_func):
        @create_function(marshalled=False, 
                argument_types=[Type.POINTER, Type.POINTER, Type.SIZE_T])
        def callback_action(lhs, rhs, size):
            lhs_array = np.frombuffer(ffi.buffer(lhs, size), dtype=dtype)
            rhs_array = np.frombuffer(ffi.buffer(rhs, size), dtype=dtype)
            if shape is not None:
                lhs_array = lhs_array.reshape(shape)
                rhs_array = rhs_array.reshape(shape)
            rtn = python_func(lhs_array, rhs_array)
            if rtn is not None:
                lhs_array[:] = rtn
        return callback_action
    return decorator 

class Reduce(LCO):
    def __init__(self, inputs, shape, dtype, id_action, op_action):
        """
        Args:
            id_action (Function)
            op_action (Function)
        """
        size = _calculate_block_size(shape, dtype)
        addr = lib.hpx_lco_reduce_new(inputs, size, id_action.id[0], op_action.id[0])
        super(Reduce, self).__init__(addr, shape, dtype) 
# }}}

