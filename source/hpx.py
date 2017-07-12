import sys, os
sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_LAZY)

from build._hpx import ffi, lib
import numpy as np
from abc import ABCMeta, abstractmethod
from collections import deque
import pickle
import logging
import copy

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
                 argument_types, array_type):
        """Register an HPX action.
        
        Note:
            This must be called prior to hpx_init().

        Args:
            python_func: A Python function to be registered as a HPX action
            action_type: Type of the action.
            key: A Python byte object to be specified as key for this action.
            argument_types: A Python list of argument types.
            marshalled (string): Can be 'true', 'false', or 'continuous'
            array_type: Type of the numpy array if marshalled is 'continuous'
        """
        self.id = ffi.new("hpx_action_t *")
        
        # generate default key if not specified
        if key is None:
            key = ((python_func.__module__ + ":" + python_func.__name__)
                  .encode('ascii'))
        self.key = key

        self.marshalled = marshalled
        self.pinned = pinned

        if marshalled == 'true':
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
            rtv = lib.hpx_register_action(action_type, lib.HPX_MARSHALLED, key, 
                                    self.id, 3, self._ffi_func, 
                                    Type.POINTER, Type.SIZE_T)
        elif marshalled == 'continuous':
            def callback_func(pointer, size):
                array_arg = np.frombuffer(ffi.buffer(pointer, size), dtype=array_type)
                # support pinned and continuous??
                rtv = python_func(array_arg)
                return rtv
            self._ffi_func = ffi.callback("int (void*, size_t)")(callback_func)
            rtv = lib.hpx_register_action(action_type, lib.HPX_MARSHALLED, key, self.id, 3, 
                                    self._ffi_func, Type.POINTER, Type.SIZE_T)
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
            
            rtv = lib.hpx_register_action(action_type, lib.HPX_ATTR_NONE, key,
                                    self.id, len(argument_types) + 1, 
                                    self._ffi_func, *argument_types)

        if rtv != SUCCESS:
            raise HPXError("action registration error")

    # Helper function for generating C arguments for this action
    def _generate_c_arguments(self, args):
        c_args = []
        for i in range(len(self._arguments_cdef)):
            c_type = self._arguments_cdef[i] + ' *'
            if self._arguments_cdef[i] == 'hpx_addr_t':
                c_args.append(ffi.new('hpx_addr_t *', args[i].addr))
            else:
                c_args.append(ffi.new(c_type, args[i]))
        return c_args

    # Helper function for generating marshalled arguments
    def _generate_marshalled_arguments(self, target_addr, args):
        if self.pinned:
            if not isinstance(target_addr, GlobalAddressBlock):
                raise TypeError("target_addr is not GlobalAddressBlock object") 
            expandargs = list(args)
            expandargs.insert(0, target_addr)
            args = tuple(expandargs)
        pointer, size = _parse_marshalled_args(args)
        return pointer, size

    # Helper function for generating array arguments
    def _generate_array_arguments(self, *args):
        if self.pinned:
            raise RuntimeError("Pinned action is not supported for array argument")
        pointer = ffi.cast("void *", args[0].__array_interface__['data'][0])
        size = ffi.cast("size_t", args[0].nbytes)
        return pointer, size

    # Helper function for getting target address of type int
    def _get_addr_int(target_addr):
        if isinstance(target_addr, GlobalAddressBlock):
            target_addr_int = target_addr.addr.addr
        elif isinstance(target_addr, GlobalAddress):
            target_addr_int = target_addr.addr
        elif isinstance(target_addr, int):
            target_addr_int = target_addr
        elif isinstance(target_addr, np.integer):
            target_addr_int = np.asscalar(target_addr)
        else:
            raise TypeError("target_addr must be GlobalAddressBlock, GlobalAddress or int")
        return target_addr_int        

    def __call__(self, target_addr, *args, sync='lsync', gate=None, 
                 lsync_lco=None, rsync_lco=None, out_array=None):
        """ Launch an Action.

        Args:
            target_addr (Union[hpx.GlobalAddressBlock, hpx.GlobalAddress, int]): Specify the 
                location where this action is launched. If this action is a pinned 
                action, this argument must be a GlobalAddressBlock object. Otherwise, 
                this argument can be either GlobalAddressBlock or GlobalAddress. You can 
                launch this action on every locality of this process by specifing this 
                argument to hpx.NULL().
            sync (string): This argument can be either 'aysnc', lsync' or 'rsync'. If 
                this argument is 'rsync', this is a completely synchronized call meaning
                this function call will be blocked until the action is completed. If 
                this argument is 'lsync', this is a locally synchronized call and you 
                can reuse or change the argument buffer after this function call. If 
                this argument is 'async', this is a completely asynchronized call, and
                this function will return immediately.
            gate (None): Not supported yet.
            lsync_lco (hpx.LCO): An LCO object to trigger when the argument can be 
                reused or changed. This is only meaningful when `sync` argument is 
                'async'. 
            rsync_lco (hpx.LCO): An LCO object ot trigger when the action is completed.
                This is only meaningful when `sync` arugument is `async` or `lsync`.
            out_array (numpy.ndarray): An numpy array to be filled with the return value
                of the action. This argument is only meaningful when `sync` argument is 
                'rsync'. If you do not care about the return value, you can specify this
                argument to None(default).
        """
        logging.debug("rank {0} on thread {1} calling action {2}".format(get_my_rank(), get_my_thread_id(), self.key))

        if self.marshalled == 'true':
            pointer, size = self._generate_marshalled_arguments(target_addr, args)
        elif self.marshalled == 'continuous':
            pointer, size = self._generate_array_arguments(args)
        else:
            c_args = self._generate_c_arguments(*args)

        lsync_addr = _get_lco_addr(lsync_lco)
        rsync_addr = _get_lco_addr(rsync_lco)
        
        # broadcast action 
        if (isinstance(target_addr, GlobalAddress) and target_addr.addr == lib.HPX_NULL):
            if self.pinned:
                raise RuntimeError("Pinned action is not supported for broadcast.")
            if sync == 'lsync':
                if self.marshalled == 'true' or self.marshalled == 'continous':
                    rtv = lib._hpx_process_broadcast_lsync(
                            lib.hpx_thread_current_pid(), self.id[0], 
                            rsync_addr, 2, pointer, size)
                else:
                    rtv = lib._hpx_process_broadcast_lsync(
                            lib.hpx_thread_current_pid(), self.id[0],
                            rsync_addr, len(c_args), *c_args)
            elif sync == 'rsync':
                if self.marshalled == 'true' or self.marshalled == 'continuous':
                    rtv = lib._hpx_process_broadcast_rsync(
                            lib.hpx_thread_current_pid(), self.id[0],
                            2, pointer, size)
                else:
                    rtv = lib._hpx_process_broadcast_rsync(
                            lib.hpx_thread_current_pid(), self.id[0],
                            len(c_args), *c_args)
            elif sync == 'async':
                if self.marshalled == 'true' or self.marshalled == 'continuous':
                    rtv = lib._hpx_process_broadcast(lib.hpx_thread_current_pid(),
                        self.id[0], lsync_addr, rsync_addr, 2, pointer, size)
                else:
                    rtv = lib._hpx_process_broadcast(lib.hpx_thread_current_pid(),
                        self.id[0], lsync_addr, rsync_addr,
                        len(c_args), *args)
            elif isinstance(sync, str):
                raise NameError("unrecognized string for sync argument")
            else:
                raise TypeError("sync argument should be of type str")
            
            if rtv != SUCCESS:
                raise HPXError("action launch failed")
            return

        # get the address of target_addr of type int
        target_addr_int = BaseAction._get_addr_int(target_addr)

        if gate is None:
            if sync == 'lsync':
                if self.marshalled == 'true' or self.marshalled == 'continuous':
                    rtv = lib._hpx_call(target_addr_int, self.id[0], rsync_addr, 2, pointer, size)
                else:
                    rtv = lib._hpx_call(target_addr_int, self.id[0], rsync_addr, len(c_args), *args)
            elif sync == 'rsync':
                if out_array is not None:
                    out_array_byte = out_array.nbytes
                    out_array_pointer = ffi.cast("void *", out_array.__array_interface__['data'][0])
                else:
                    out_array_byte = 0
                    out_array_pointer = ffi.NULL
                if self.marshalled == 'true' or self.marshalled == 'continuous':
                    rtv = lib._hpx_call_sync(target_addr_int, self.id[0], out_array_pointer, 
                        out_array_byte, 2, pointer, size)
                else:
                    rtv = lib._hpx_call_sync(target_addr_int, self.id[0], out_array_pointer, 
                        out_array_byte, len(c_args), *args)
            elif sync == 'async':
                rtv = lib._hpx_call_async(target_addr.addr, self.id[0], lsync_addr, rsync_addr,
                        len(c_args), *args)
            elif isinstance(sync, str):
                raise ValueError("sync argument not recognizable")
            else:
                raise TypeError("sync argument should be of type str")
        elif isinstance(gate, LCO):
            # TODO: support gate argument
            # if sync == 'lsync':
            #    if self.marshalled == 'true' or self.marshalled == 'continuous':
            #        rtv = 
            # elif sync == 'rsync':
            #
            # elif sync == 'async':
            # else:
            #    raise ValueError("sync argument not recognizable")
            pass
        else:
            raise TypeError("gate should be an instance of LCO")

        if rtv != SUCCESS:
            raise HPXError("action launch failed")

def call_cc(action, target_addr, *args, gate=None):
    target_addr_int = BaseAction._get_addr_int(target_addr)
    if gate is None:
        if action.marshalled == 'true':
            pointer, size = action._generate_marshalled_arguments(target_addr, args)
            rtv = lib._hpx_call_cc(target_addr_int, action.id[0], 2, pointer, size)
        elif action.marshalled == 'continuous':
            pointer, size = action._generate_array_arguments(args)
            rtv = lib._hpx_call_cc(target_addr_int, action.id[0], 2, pointer, size)
        else:
            c_args = action._generate_c_arguments(args)
            rtv = lib._hpx_call_cc(target_addr_int, action.id[0], len(c_args), *c_args)
    elif isinstance(gate, LCO):
        if action.marshalled == 'true':
            pointer, size = action._generate_marshalled_arguments(target_addr, args)
            rtv = lib._hpx_call_when_cc(gate.addr, target_addr_int, action.id[0], 2, pointer, size)
        elif action.marshalled == 'continuous':
            pointer, size = action._generate_array_arguments(args)
            rtv = lib._hpx_call_when_cc(gate.addr, target_addr_int, action.id[0], 2, pointer, size)
        else:
            c_args = action._generate_c_arguments(args)
            rtv = lib._hpx_call_when_cc(gate.addr, target_addr_int, action.id[0], len(c_args), *c_args)
    else:
        raise TypeError("Unrecognized gate argument")

    if rtv != SUCCESS:
        raise HPXError("call_cc error")

def call_with_continuation(target_action, target_addr, cont_action, cont_addr, *args, gate=None):
    target_addr_int = BaseAction._get_addr_int(target_addr)
    cont_addr_int = BaseAction._get_addr_int(cont_addr)

    if gate is None:
        if target_action.marshalled == 'true':
            pointer, size = target_action._generate_marshalled_arguments(target_addr, args)
            rtv = lib._hpx_call_with_continuation(target_addr_int, target_action.id[0], cont_addr_int, 
                                                cont_action.id[0], 2, pointer, size)
        elif target_action.marshalled == 'continuous':
            pointer, size = target_action._generate_array_arguments(args)
            rtv = lib._hpx_call_with_continuation(target_addr_int, target_action.id[0], cont_addr_int, 
                                                cont_action.id[0], 2, pointer, size)
        else:
            c_args = target_action._generate_c_arguments(args)
            rtv = lib._hpx_call_with_continuation(target_addr_int, target_action.id[0], cont_addr_int, 
                                                cont_action.id[0], len(c_args), *c_args)
    elif isinstance(gate, LCO):
        if target_action.marshalled == 'true':
            pointer, size = target_action._generate_marshalled_arguments(target_addr, args)
            rtv = lib._hpx_call_when_with_continuation(gate.addr, target_addr_int, target_action.id[0], cont_addr_int,
                                                cont_action.id[0], 2, pointer, size)
        elif target_action.marshalled == 'continuous':
            pointer, size = target_action._generate_array_arguments(args)
            rtv = lib._hpx_call_when_with_continuation(gate.addr, target_addr_int, target_action.id[0], cont_addr_int,
                                                cont_action.id[0], 2, pointer, size)
        else:
            c_args = target_action._generate_c_arguments(args)
            rtv = lib._hpx_call_when_with_continuation(gate.addr, target_addr_int, target_action.id[0], cont_addr_int,
                                                cont_action.id[0], len(c_args), *c_args)
    else:
        raise TypeError("Unrecognized gate argument")

class Action(BaseAction):
    def __init__(self, python_func, key=None, marshalled='true', pinned=False, 
                 argument_types=None, array_type=None):
        return super(Action, self).__init__(python_func, lib.HPX_DEFAULT, key, 
                                            marshalled, pinned, argument_types, array_type)

def create_action(key=None, marshalled='true', pinned=False, argument_types=None, 
                  array_type=None):
    """ Create an `Action` object.

    Args:
        key (bytes): An optional argument if you would like to support action identifier 
            yourself.
        marshalled (string): The value of this argument can be 'true', 'continuous', or 
            'false'. If this argument is 'true', this action is an marshalled action. If 
            this argument is 'continous', only one numpy array can be specified as 
            argument, and `array_types` argument needs to be specified. If this argument 
            is 'false', this action is not marshalled, and you need to specify the 
            argument types in the `argument_types` argument.
        pinned (bool): If this action is pinned, the first argument is the pinned 
            `GlobalAddressBlock`.
        argument_types (list): Only needed if `marshalled` is 'false' when argument 
            types are needed. This should be a list of `Type` object.
        array_type (numpy.dtype): Only needed if `marshalled` is `continuous` to 
            specify the type of the numpy array in the argument.
    
    Returns:
        A decorator which takes a Python function to register.

    Note:
        Action must be created before `hpx.init()`.
    """
    def decorator(python_func):
        return Action(python_func, key, marshalled, pinned, argument_types, array_type)
    return decorator


class Function(BaseAction):
    def __init__(self, python_func, argument_types, key=None):
        return super(Function, self).__init__(python_func, lib.HPX_FUNCTION, key, 
                                              marshalled='false', pinned=False,
                                              argument_types=argument_types, 
                                              array_type=None) 
    
    def __call__(self, *args):
        raise RuntimeError("Funtion action is not callable")

def create_function(argument_types, key=None):
    """ Create an Function object.

    Args:
        argument_types (list): A list of `Type` objects representing argument types.
        key (bytes): An optional argument if you would like to support action identifier 
            yourself.

    Returns:
        A decorator which takes a Python function to register.

    Note:
        Function must be created before `hpx.init()`.
    """
    def decorator(python_func):
        return Function(python_func, argument_types, key)
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
        HPXError: If the initialization fails.
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
    if lib.hpx_custom_init(c_argc, c_argv_address) != SUCCESS:
        raise HPXError("hpx.init failed")

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
        lib.hpx_exit(size, ffi.cast("void *", array.__array_interface__['data'][0]))
    else:
        lib.hpx_exit(0, ffi.NULL)

def run(action, *args, shape=None, dtype=None):
    """Start an HPX main process.
    
    This collective creates an HPX "main" process, and calls the given `action`
    entry in the context of this process.
    
    The process does not use termination detection and must be terminated
    through a single explicit call to hpx.exit().

    Args:
        action (hpx.BaseAction): The action to execute.
        *args: Arguments of this action.
        shape: Shape of numpy array returned.
    """
    if action.marshalled:
        args_pointer, size = _parse_marshalled_args(args)
        if shape is None:
            status = lib._hpx_run(action.id, ffi.NULL, 2, args_pointer, size)
        else:
            rtv = np.zeros(shape, dtype=dtype)
            rtv_pointer = ffi.cast("void*", rtv.__array_interface__['data'][0])
            status = lib._hpx_run(action.id, rtv_pointer, 2, args_pointer, size)
    else:
        c_args = action._generate_c_arguments(*args)
        if shape is None:
            status = lib._hpx_run(action.id, ffi.NULL, len(c_args), *c_args)
        else:
            rtv = np.zeros(shape, dtype=dtype)
            rtv_pointer = ffi.cast("void*", rtv.__array_interface__['data'][0])
            status = lib._hpx_run(action.id, rtv_pointer, len(c_args), *c_args)

    if status != lib.HPX_SUCCESS:
        raise HPXError("hpx.run failed")

    if shape is not None:
        return rtv

def finalize():
    """Finalize/cleanup from the HPX runtime.

    This function will remove almost all data structures and allocations, and
    will finalize the underlying network implementation. Note that hpx.run()
    must never be called after hpx.finalize().

    """
    lib.hpx_custom_finalize()

def print_help():
    lib.hpx_print_help()

# }}} 

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
            bsize (int): The block size used when allocating memory associated with `addr`.
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
            Local memory (int object) which corresponds to the given global memory if 
            successful and `return_local` is true.
        """
        if return_local == True:
            local = ffi.new("void **")
            rtv = lib.hpx_gas_try_pin(self.addr, local)
            if rtv == False:
                raise HPXError("Pinning the global memory fails")
            else:
                return int(ffi.cast("uintptr_t", local[0]))
        else:
            rtv = lib.hpx_gas_try_pin(self._addr, ffi.NULL)
            if rtv == False:
                raise HPXError("Pinning the global memory fails")

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

def _currentdim_is_slice(sliceObj, dimLimit):
    if sliceObj.start == None:
        start = 0
    else:
        start = sliceObj.start
    if sliceObj.stop == None:
        stop = dimLimit
    else:
        stop = sliceObj.stop
    return start, stop

class DummyArray:
    # This class is used for constructing new arrays in GlobalAddressBlock.try_pin
    def __init__(self, data, strides, shape, dtype):
        self.__array_interface__ = {}
        self.__array_interface__['data'] = (data, False)
        self.__array_interface__['strides'] = strides
        self.__array_interface__['shape'] = shape
        self.__array_interface__['typestr'] = dtype.str
        self.__array_interface__['descr'] = dtype.descr

def construct_array(addr, shape, dtype, strides=None):
    dummy_array = DummyArray(addr, strides, shape, dtype)
    return np.array(dummy_array, copy=False, dtype=dtype)

class GlobalAddressBlock:
    def __init__(self, addr, shape, dtype, strides):
        """Constructor of a GlobalAddressBlock object

        Args:
            addr (GlobalAddr): The starting address of this block.
            dtype (numpy.dtype): The type of each object in address block.
        """
        self.addr = addr
        self.shape = shape
        self.dtype = dtype
        self.strides = strides
    
    def __getitem__(self, key):

        # convert key (maybe int, slice or tuple) to tuple
        if isinstance(key, int) or isinstance(key, slice):
            keyWrap = (key,)
        elif isinstance(key, tuple):
            keyWrap = key
        else:
            raise TypeError("Invalid key type")
        
        newAddr = copy.copy(self.addr)
        newShape = []
        newStrides = []

        for i in range(len(keyWrap)):
            if isinstance(keyWrap[i], int):
                if keyWrap[i] >= self.shape[i] or keyWrap[i] < 0:
                    raise RuntimeError("GlobalAddressBlock object index out of bound")
                newAddr += keyWrap[i]*self.strides[i]
            elif isinstance(keyWrap[i], slice):
                start, stop = _currentdim_is_slice(keyWrap[i], self.shape[i])
                if start >= self.shape[i] or start < 0 or stop > self.shape[i] or stop < 0:
                    raise RuntimeError("GlobalAddressBlock object index out of bound")
                newAddr += start*self.strides[i]
                newShape.append(stop - start)
                newStrides.append(self.strides[i])
            else:
                raise TypeError("Invalid key type in dimension " + str(i))

        newShape = tuple(newShape)
        newStrides = tuple(newStrides)

        # if the indexing does not cover all dimension, fill in the remaining dimensions
        newShape += self.shape[len(keyWrap):]
        newStrides += self.strides[len(keyWrap):]
        
        return GlobalAddressBlock(newAddr, newShape, self.dtype, newStrides)  

    def try_pin(self):
        """ Performs address translation. See `Addr.try_pin` for detail.

        Returns:
            A numpy array representing this address block.
        """
        addrLocal = self.addr.try_pin(True)
        dummy_array = DummyArray(addrLocal, self.strides, self.shape, self.dtype)
        return np.array(dummy_array, copy=False, dtype=self.dtype)

    def unpin(self):
        """ Unpin this address block.
        """
        self.addr.unpin()

    def iscontinuous(self):
        """ Test whether current memory block is countinous.

        This method is used by get, set and try_pin for checking.
        """
        # discard first few dimensions where the size is 1
        i = 0
        while self.shape[i] == 1:
            i += 1
            if i == len(self.shape):
                break

        # at here, i is the first dimension not having size 1
        i += 1
        while i < len(self.shape):
            if self.shape[i] != self.strides[i-1]//self.strides[i]:
                return False
            i += 1

        # test last dimension has strides of itemsize
        if self.strides[-1] != self.dtype.itemsize:
            return False
        
        return True

    def get(self, sync='sync', lsync_lco=None):
        """ This copies data from a global address to a local buffer.

        This operation is not atomic. GlobalAddressBlock.get with concurrent 
        GlobalAddressBlock.set to overlapping addresses ranges will result in
        a data race with undefined behavior. Users should synchronize with 
        some out-of-band mechanism.

        Args:
            sync (string): can be 'sync' or 'async'
        """

        # get only works on continuous memory block
        if not self.iscontinuous():
            raise RuntimeError("GlobalAddressBlock.get must be applied on a continuous block")

        i = 0
        while self.shape[i] == 1 and i < len(self.shape) - 1:
            i += 1
        size = self.shape[i]*self.strides[i]

        array = np.zeros((size//self.dtype.itemsize,), dtype=self.dtype).reshape(self.shape)

        if sync == 'sync':
            lib.hpx_gas_memget_sync(ffi.cast("void *", array.__array_interface__['data'][0]), 
                                    self.addr.addr, size)
            return array
        elif sync == 'async':
            # TODO: How??
            pass
        else:
            raise ValueError("'sync' argument needs to be either 'sync' or 'async'")

    def set(self, from_array, sync='rsync', lsync_lco=None, rsync_lco=None):
        """ This method copies data from a local buffer to the global memory block this object referenced.

        Args:
            from_array (numpy.ndarray): A numpy array whose content to be copied.
            sync (string): This argument can be 'async', 'lsync', 'rsync'. When this argument is 'async' or 'lsync', 
                optional LCOs can be supplied in argument `lsync_lco` or `rsync_lco`.
            lsync_lco (LCO): An LCO object to be set when `from` can be reused or freed
            rsync_lco (LCO): An LCO object to be set when the remote setting is completed.
        """

        # test the global address is continous
        if not self.iscontinuous():
            raise RuntimeError("GlobalAddressBlock.set must be applied on a contiguous block")

        # test `from` is countinous
        if not from_array.flags['C_CONTIGUOUS']:
            raise RuntimeError("from_array argument must be C contiguous")
        from_addr = ffi.cast("void *", from_array.__array_interface__['data'][0])

        i = 0
        while self.shape[i] == 1 and i < len(self.shape) - 1:
            i += 1
        size = self.shape[i] * self.strides[i]

        lsync_addr = _get_lco_addr(lsync_lco)
        rsync_addr = _get_lco_addr(rsync_lco)

        if sync == 'rsync':
            lib.hpx_gas_memput_rsync(self.addr.addr, from_addr, size)
        elif sync == 'lsync':
            lib.hpx_gas_memput_lsync(self.addr.addr, from_addr, size, rsync_addr)
        elif sync == 'async':
            lib.hpx_gas_memput(self.addr.addr, from_addr, size, lsync_addr, rsync_addr)
        else:
            ValueError("'sync' argument can only be 'rsync', 'lsync' or 'async'")
        
# }}}

# {{{ GlobalMemory

def _calculate_block_size(shape):
    """ Helper function for calculating block size (not memory size) for GAS allocation functions.
    """
    total_size = 1
    for dim in shape:
        total_size *= dim
    return total_size

class GlobalMemory:

    def __init__(self, addr, numBlock, blockShape, dtype, strides):
        """ Constructor for a GlobalMemory object. 

        This is supposed to be used internally. User should refer to one of the class methods for allocating global
        memory.

        Args:
            addr (GlobalAddress): A GlobalAddress object representing the beginning of GAS memory this object 
                represented.
            numBlock (tuple): The number of blocks.
            blockShape (tuple): The shape of each block.
            strides (tuple): Memory increment for each dimension. This should not change after the initial allocation.
        """
        self.addr = addr
        self.numBlock = numBlock
        self.blockShape = blockShape
        self.dtype = dtype
        self.strides = strides
        
    def _calculate_strides(shape, dtype):
        """ This is used internally for GAS allocation implementation.

        Args:
            shape (tuple): This is the concatenation of numBlock and blockShape
        """
        strides = deque([dtype.itemsize])
        for i in range(len(shape)-1, 0, -1):
            strides.appendleft(strides[0] * shape[i])
        return tuple(strides)

    @classmethod
    def alloc_cyclic(cls, numBlock, blockShape, dtype, boundary=0):
        """Allocate cyclically distributed global memory.
        
        Args:
            numBlock (tuple, int): The number of blocks to allocate.
            blockShape (tuple, int): The shape of each block.
            dtype (numpy.dtype): The data type of each entry in the block.
            boundary (int): The alignment.

        Returns:
            A GlobalMemory object representing the allocated memory.
        """
        if isinstance(numBlock, int):
            numBlock = (numBlock,)
        if isinstance(blockShape, int):
            blockShape = (blockShape,)

        block_size = _calculate_block_size(blockShape) * dtype.itemsize
        block_num = _calculate_block_size(numBlock)
        addr = lib.hpx_gas_alloc_cyclic(block_num, block_size, boundary)
        strides = GlobalMemory._calculate_strides(numBlock + blockShape, dtype)
        return cls(GlobalAddress(addr, block_size), numBlock, blockShape, dtype, strides)

    @classmethod
    def calloc_cyclic(cls, numBlock, blockShape, dtype, boundary=0):
        """Allocate cyclically distributed global zeroed memory.
        """
        if isinstance(numBlock, int):
            numBlock = (numBlock,)
        if isinstance(blockShape, int):
            blockShape = (blockShape,)

        block_size = _calculate_block_size(blockShape) * dtype.itemsize
        block_num = _calculate_block_size(numBlock)
        addr = lib.hpx_gas_calloc_cyclic(block_num, block_size, boundary)
        strides = GlobalMemory._calculate_strides(numBlock + blockShape, dtype)
        return cls(GlobalAddress(addr, block_size), numBlock, blockShape, dtype, strides)

    @classmethod
    def alloc_local_at(cls, numBlock, blockShape, dtype, loc, boundary=0, sync='sync', 
        lco=None):
        """Allocate a block of global memory.

        This is a non-collective call to allocate memory in the global
        address space that can be moved. The allocated memory, by default,
        has affinity to the allocating node, however in low memory conditions the
        allocated memory may not be local to the caller. As it allocated in the GAS,
        it is accessible from any locality, and may be relocated by the
        runtime.
        
        Args:
            loc (GlobalAddress or int): The address which the allocation targets.
            sync (string): this argument can be either 'sync' or 'async'. If this argument is 
                'async', an optional argument `lco` can be provided for synchronization.
        """
        if isinstance(numBlock, int):
            numBlock = (numBlock,)
        if isinstance(blockShape, int):
            blockShape = (blockShape,)
        
        if isinstance(loc, GlobalAddress):
            loc_addr = loc.addr
        else:
            loc_addr = loc

        block_size = _calculate_block_size(blockShape) * dtype.itemsize
        strides = GlobalMemory._calculate_strides(numBlock + blockShape, dtype)
        block_num = _calculate_block_size(numBlock)

        if sync == 'sync':
            addr = lib.hpx_gas_alloc_local_at_sync(block_num, block_size, boundary, loc_addr)
        elif sync == 'async':
            if isinstance(lco, LCO):
                lco_addr = lco.addr
            elif lco is None:
                lco_addr = NULL()
            else:
                raise RuntimeError("Unrecognizable argument 'lco'")

            addr = lib.hpx_gas_alloc_local_at_async(block_num, block_size, boundary, loc_addr, lco_addr)
        else:
            raise RuntimeError("Unrecognizable argument 'sync'")

        return cls(GlobalAddress(addr, block_size), numBlock, blockShape, dtype, strides)

    def free(self, lco):
        """Free the global allocation associated with this object.

        Note that even if the current object is created by indexing, and hence 
        only part of the initial allocation, all memory of the initial 
        allocation will be freed.
        """
        lib.hpx_gas_free(self.addr.addr, lco.addr)

    def free_sync(self):
        lib.hpx_gas_free_sync(self.addr.addr)

    def __getitem__(self, key):

        block_dims = len(self.numBlock)
        dims = self.numBlock + self.blockShape

        # convert key (maybe tuple or int) to tuple
        if isinstance(key, slice) or isinstance(key, int):
            keyWrap = (key,)
        elif isinstance(key, tuple):
            keyWrap = key
        else:
            raise TypeError("invalid key type for indexing")

        newNumBlock = []
        newBlockShape = []
        newStrides = []
        newAddr = copy.copy(self.addr)

        for i in range(len(keyWrap)):
            if i >= len(self.strides):
                raise RuntimeError("Too many dimensions in indexing")

            if isinstance(keyWrap[i], int):
                if keyWrap[i] >= dims[i] or keyWrap[i] < 0:
                    raise RuntimeError("GlobalMemory object index out of bound")
                newAddr += keyWrap[i]*self.strides[i]
            elif isinstance(keyWrap[i], slice):
                start, stop = _currentdim_is_slice(keyWrap[i], dims[i])
                if start >= dims[i] or start < 0 or stop > dims[i] or stop < 0:
                    raise RuntimeError("GlobalMemory object index out of bound")
                newAddr += start*self.strides[i]
                newStrides.append(self.strides[i])
                if i < block_dims:
                    newNumBlock.append(stop - start)
                else:
                    newBlockShape.append(stop - start)
            else:
                raise TypeError("invalid key type for indexing")

        newNumBlock = tuple(newNumBlock)
        newBlockShape = tuple(newBlockShape)
        newStrides = tuple(newStrides)

        # if the indexing does not cover all dimension, fill in the remaining dimensions
        if len(keyWrap) < len(self.numBlock):
            newNumBlock += self.numBlock[len(keyWrap):]
            newBlockShape = self.blockShape
        else:
            newBlockShape += self.blockShape[len(keyWrap)-len(self.numBlock):]
        newStrides += self.strides[len(keyWrap):]

        # if all index are integer, then the shape should be (1,)
        if len(newBlockShape) == 0:
            newBlockShape = (1,)

        if len(newNumBlock) > 0:
            return GlobalMemory(newAddr, newNumBlock, newBlockShape, self.dtype, newStrides)
        else:
            return GlobalAddressBlock(newAddr, newBlockShape, self.dtype, newStrides)


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
            self.size = _calculate_block_size(shape) * dtype.itemsize
        else:
            self.size = 0

    def delete(self, sync='sync', sync_lco=None):
        """
        Args:
            sync (string): can be 'async' or 'sync'
            sync_lco (LCO): An LCO to signal remote completion.
        """
        if sync == 'sync':
            lib.hpx_lco_delete_sync(self.addr)
        elif sync == 'async':
            lco_addr = _get_lco_addr(sync_lco)
            lib.hpx_lco_delete(self.addr, lco_addr)
        else:
            raise ValueError("Unrecognized 'sync' argument for hpx.LCO.delete")
    

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
            size = _calculate_block_size(shape) * dtype.itemsize
        else:
            size = 0
        addr = lib.hpx_lco_future_new(size)
        super(Future, self).__init__(addr, shape, dtype)

def create_id_action(dtype, shape=None):
    """ Create an Function object as initialization action of Reduce LCO.

    This is a wrapper around hpx.create_function specifically for initialization action 
    of Reduce LCO. There should be exactly one numpy array in the decorated function. 
    You can modify this numpy array in place or return desired numpy array. 

    Args:
        dtype (numpy.dtype): The data type of the numpy array.
        shape (tuple): An optional argument to represent the shape of the numpy array, 
            if this argument is None, the shape is a linear one-dimentional array.

    Returns:
        A decorator which takes a Python function to register.

    Note:
        Action must be created before hpx.init().    
    """
    def decorator(python_func):
        @create_function(argument_types=[Type.POINTER, Type.SIZE_T])
        def callback_action(pointer, size):
            logging.debug("rank {0} thread {1} start id callback {2}".format(
                         get_my_rank(), get_my_thread_id(),
                         python_func.__module__ + ':' + python_func.__name__))
            buf = ffi.buffer(pointer, size)
            array = np.frombuffer(buf, dtype=dtype)
            if shape is not None:
                array = array.reshape(shape)
            rtn = python_func(array)
            if rtn is not None:
                array[:] = rtn
            logging.debug("rank {0} thread {1} finish id callback {2}".format(
                         get_my_rank(), get_my_thread_id(),
                         python_func.__module__ + ':' + python_func.__name__))
        return callback_action
    return decorator

def create_op_action(dtype, shape=None):
    """ Create an Function object as reduction action of Reduce LCO.
    
    This is a wrapper around hpx.create_function specifically for reduction action of 
    Reduce LCO. There should be exactly two numpy arrays in the decorated function. You 
    can modify the first numpy array in place or return desired numpy array.

    Args:
        dtype (numpy.dtype): The data type of the numpy array.
        shape (tuple): An optional argument to represent the shape of the numpy array, 
            if this argument is None, the shape is a linear one-dimentional array.
    
    Returns:
        A decorator which takes a Python function to register.

    Note:
        Action must be created before hpx.init().  
    """
    def decorator(python_func):
        @create_function(argument_types=[Type.POINTER, Type.POINTER, Type.SIZE_T])
        def callback_action(lhs, rhs, size):
            logging.debug("rank {0} thread {1} start op callback {2}".format(
                         get_my_rank(), get_my_thread_id(),
                         python_func.__module__ + ':' + python_func.__name__))
            lhs_array = np.frombuffer(ffi.buffer(lhs, size), dtype=dtype)
            rhs_array = np.frombuffer(ffi.buffer(rhs, size), dtype=dtype)
            if shape is not None:
                lhs_array = lhs_array.reshape(shape)
                rhs_array = rhs_array.reshape(shape)
            rtn = python_func(lhs_array, rhs_array)
            if rtn is not None:
                lhs_array[:] = rtn
            logging.debug("rank {0} thread {1} finish op callback {2}".format(
                         get_my_rank(), get_my_thread_id(),
                         python_func.__module__ + ':' + python_func.__name__))
        return callback_action
    return decorator 

class Reduce(LCO):
    def __init__(self, inputs, shape, dtype, id_action, op_action):
        """
        Args:
            id_action (Function)
            op_action (Function)
        """
        size = _calculate_block_size(shape) * dtype.itemsize
        addr = lib.hpx_lco_reduce_new(inputs, size, id_action.id[0], op_action.id[0])
        super(Reduce, self).__init__(addr, shape, dtype) 
# }}}

# {{{ Threads

def thread_continue(type, *args):
    """ Initiate the current thread's continuation.

    Args:
        type (string): 'marshalled', 'array' 
    """
    # Note: Continuation currently does not support non-marshalled action
    if type == 'marshalled':
        pointer, size = _parse_marshalled_args(args)
    elif type == 'array':
        pointer = ffi.cast("void *", args[0].__array_interface__['data'][0])
        size = ffi.cast("size_t", args[0].nbytes)
    else:
        raise RuntimeError("unrecognized type argument for thread_continue")

    if lib._hpx_thread_continue(2, pointer, size) != SUCCESS:
        raise HPXError("Errors occurred when launching continuation")

def thread_current_target():
    return lib.hpx_thread_current_target()

# }}}

# {{{ Logging

def set_loglevel(loglevel: str):
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level)

# }}}

# {{{ Topology

def get_my_rank():
    return lib.hpx_get_my_rank()

def get_num_ranks():
    return lib.hpx_get_num_ranks()

def get_num_threads():
    return lib.hpx_get_num_threads()

def get_my_thread_id():
    return lib.hpx_get_my_thread_id()

# }}}

# {{{ Time

def time_now():
    return lib.hpx_time_now()

def time_from_start_ns(t):
    return lib.hpx_time_from_start_ns(t)

def time_elapsed_ms(from_time):
    return lib.hpx_time_elapsed_ms(from_time)

# }}}

# {{{ Error handling

def HPXError(Exception):
    """ Base class for exceptions in HPX runtime
    """
    pass

# }}}

