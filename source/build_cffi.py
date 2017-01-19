from cffi import FFI
import subprocess
ffi = FFI()

# Get the compilation flags
compile_libraries = []
compile_include_dirs = []
compile_library_dirs = []
compile_extra_compile_args = []
pkg_config_process_output = subprocess.check_output(
    ["pkg-config", "--libs", "hpx"],
    universal_newlines=True)
pkg_libs_result = pkg_config_process_output[:-1]
for argument in pkg_libs_result.split():
    if argument[:2] == "-L":
        compile_library_dirs.append(argument[2:])
    elif argument[:2] == "-l":
        compile_libraries.append(argument[2:])
    else:
        compile_extra_compile_args.append(argument)
pkg_config_process_output = subprocess.check_output(
    ["pkg-config", "--cflags", "hpx"],
    universal_newlines=True
)
pkg_libs_result = pkg_config_process_output[:-1]
for argument in pkg_libs_result.split():
    if argument[:2] == '-I':
        compile_include_dirs.append(argument[2:])


ffi.cdef("""

/* Begin types.h */

// Port HPX status
#define  HPX_ERROR           ...
#define  HPX_SUCCESS         ...
#define  HPX_RESEND          ...
#define  HPX_LCO_ERROR       ...
#define  HPX_LCO_CHAN_EMPTY  ...
#define  HPX_LCO_TIMEOUT     ...
#define  HPX_LCO_RESET       ...
#define  HPX_ENOMEM          ...
#define  HPX_USER            ...

// Port argument types
typedef struct _ffi_type ffi_type;
typedef ffi_type *hpx_type_t;
hpx_type_t HPX_CHAR_lvalue;
hpx_type_t HPX_UCHAR_lvalue;
hpx_type_t HPX_SCHAR_lvalue;
hpx_type_t HPX_SHORT_lvalue;
hpx_type_t HPX_USHORT_lvalue;
hpx_type_t HPX_SSHORT_lvalue;
hpx_type_t HPX_INT_lvalue;
hpx_type_t HPX_UINT_lvalue;
hpx_type_t HPX_SINT_lvalue;
hpx_type_t HPX_LONG_lvalue;
hpx_type_t HPX_ULONG_lvalue;
hpx_type_t HPX_SLONG_lvalue;
hpx_type_t HPX_VOID_lvalue;
hpx_type_t HPX_UINT8_lvalue;
hpx_type_t HPX_SINT8_lvalue;
hpx_type_t HPX_UINT16_lvalue;
hpx_type_t HPX_SINT16_lvalue;
hpx_type_t HPX_UINT32_lvalue;
hpx_type_t HPX_SINT32_lvalue;
hpx_type_t HPX_UINT64_lvalue;
hpx_type_t HPX_SINT64_lvalue;
hpx_type_t HPX_FLOAT_lvalue;
hpx_type_t HPX_DOUBLE_lvalue;
hpx_type_t HPX_POINTER_lvalue;
hpx_type_t HPX_LONGDOUBLE_lvalue;
// hpx_type_t HPX_COMPLEX_FLOAT_lvalue;
// hpx_type_t HPX_COMPLEX_DOUBLE_lvalue;
// hpx_type_t HPX_COMPLEX_LONGDOUBLE_lvalue;
hpx_type_t HPX_ADDR_lvalue;
hpx_type_t HPX_SIZE_T_lvalue;

/* End types.h */

/* Begin action.h */

// Port action types
typedef uint16_t hpx_action_t;
typedef enum {
  HPX_DEFAULT = 0,
  HPX_TASK,
  HPX_INTERRUPT,
  HPX_FUNCTION,
  HPX_OPENCL,
} hpx_action_type_t;

// Port action attributes
#define HPX_ATTR_NONE  ...
#define HPX_MARSHALLED ...
#define HPX_PINNED     ...
#define HPX_INTERNAL   ...
#define HPX_VECTORED   ...
#define HPX_COALESCED  ...
#define HPX_COMPRESSED ...

// Port action API
int hpx_register_action(hpx_action_type_t type, uint32_t attr, const char *key,
                        hpx_action_t *id, unsigned n, ...);

/* End action.h */


/* Begin Runtime.h */

int hpx_init(int *argc, char ***argv);
void hpx_finalize(void);
void hpx_exit(size_t bytes, const void *out);
int _hpx_run(hpx_action_t *entry, void *out, int nargs, ...);
void hpx_print_help(void);
int hpx_custom_init(int *argc, char ***argv);
void hpx_custom_finalize(void);

/* End Runtime.h */

// Port parcel API
typedef short hpx_status_t;
typedef struct hpx_parcel hpx_parcel_t;
hpx_parcel_t *hpx_parcel_acquire(const void *data, size_t bytes);
hpx_status_t hpx_parcel_send_sync(hpx_parcel_t *p);
void hpx_parcel_set_action(hpx_parcel_t *p, hpx_action_t action);

/* Begin topology.h */

int hpx_get_my_rank(void);
int hpx_get_num_ranks(void);
int hpx_get_num_threads(void);
int hpx_get_my_thread_id(void);

/* End topology.h */

/* Begin addr.h */

#define HPX_NULL ...
typedef uint64_t hpx_addr_t;
hpx_addr_t HPX_THERE(uint32_t i);
hpx_addr_t HPX_HERE;
hpx_addr_t hpx_addr_add(hpx_addr_t addr, int64_t bytes, uint32_t bsize);
int64_t hpx_addr_sub(hpx_addr_t lhs, hpx_addr_t rhs, uint32_t bsize);

/* End addr.h */

/* Begin process.h */

typedef hpx_addr_t hpx_pid_t;
int _hpx_process_broadcast(hpx_pid_t pid, hpx_action_t action, hpx_addr_t lsync, hpx_addr_t rsync, int nargs, ...);
int _hpx_process_broadcast_lsync(hpx_pid_t pid, hpx_action_t action, hpx_addr_t rsync, int nargs, ...);
int _hpx_process_broadcast_rsync(hpx_pid_t pid, hpx_action_t action, int nargs, ...);

/* End process.h */

/* Begin thread.h */

hpx_pid_t hpx_thread_current_pid(void);

/* End thread.h */

/* Begin gas.h */
hpx_addr_t hpx_gas_alloc_cyclic(size_t n, size_t bsize, uint32_t boundary);
hpx_addr_t hpx_gas_calloc_cyclic(size_t n, size_t bsize, uint32_t boundary);
hpx_addr_t hpx_gas_alloc_local_at_sync(size_t n, uint32_t bsize, uint32_t boundary,
                                       hpx_addr_t loc);
void hpx_gas_alloc_local_at_async(size_t n, uint32_t bsize, uint32_t boundary,
                                  hpx_addr_t loc, hpx_addr_t lco);
void hpx_gas_free(hpx_addr_t addr, hpx_addr_t rsync);
void hpx_gas_free_sync(hpx_addr_t addr);
int hpx_gas_memput_rsync(hpx_addr_t to, const void *from, size_t size);
bool hpx_gas_try_pin(hpx_addr_t addr, void **local);
void hpx_gas_unpin(hpx_addr_t addr);

/* End gas.h */

/* Begin lco.h */

void hpx_lco_delete(hpx_addr_t lco, hpx_addr_t rsync);
void hpx_lco_delete_sync(hpx_addr_t lco);
hpx_addr_t hpx_lco_and_new(int64_t inputs);
void hpx_lco_and_set(hpx_addr_t lco, hpx_addr_t sync);
void hpx_lco_and_set_num(hpx_addr_t lco, int num, hpx_addr_t sync);
hpx_addr_t hpx_lco_future_new(int size);
void hpx_lco_set(hpx_addr_t lco, size_t size, const void *value, hpx_addr_t lsync, hpx_addr_t rsync);
void hpx_lco_set_lsync(hpx_addr_t lco, size_t size, const void *value, hpx_addr_t rsync);
int hpx_lco_set_rsync(hpx_addr_t lco, size_t size, const void *value);
hpx_status_t hpx_lco_wait(hpx_addr_t lco);
hpx_status_t hpx_lco_get(hpx_addr_t lco, size_t size, void *value);
hpx_addr_t hpx_lco_reduce_new(int inputs, size_t size, hpx_action_t id, hpx_action_t op);

/* End lco.h */

/* Being rpc.h */

int _hpx_call_sync(hpx_addr_t addr, hpx_action_t action, void *out, size_t olen, int n, ...);
int _hpx_call(hpx_addr_t addr, hpx_action_t action, hpx_addr_t result, int n, ...);
int _hpx_call_when(hpx_addr_t gate, hpx_addr_t addr, hpx_action_t action, hpx_addr_t result, int n, ...);
int _hpx_call_when_with_continuation(hpx_addr_t gate, hpx_addr_t addr, hpx_action_t action, hpx_addr_t c_target, hpx_action_t c_action, int n, ...);
int _hpx_call_when_sync(hpx_addr_t gate, hpx_addr_t addr, hpx_action_t action, void *out, size_t olen, int n, ...);
int _hpx_call_with_continuation(hpx_addr_t addr, hpx_action_t action, hpx_addr_t c_target, hpx_action_t c_action, int n, ...);
int _hpx_call_async(hpx_addr_t addr, hpx_action_t action, hpx_addr_t lsync, hpx_addr_t result, int n, ...);
int _hpx_call_when_cc(hpx_addr_t gate, hpx_addr_t addr, hpx_action_t action, int n, ...);
int _hpx_call_cc(hpx_addr_t addr, hpx_action_t action, int n, ...);

/* End rpc.h */

""")

compile_include_dirs.append('contrib')

ffi.set_source("build._hpx",
"""
#include <hpx/hpx.h>
#include "uthash/uthash.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pythread.h>
#include <pthread.h>

hpx_type_t HPX_CHAR_lvalue = HPX_CHAR;
hpx_type_t HPX_UCHAR_lvalue = HPX_UCHAR;
hpx_type_t HPX_SCHAR_lvalue = HPX_SCHAR;
hpx_type_t HPX_SHORT_lvalue = HPX_SHORT;
hpx_type_t HPX_USHORT_lvalue = HPX_USHORT;
hpx_type_t HPX_SSHORT_lvalue = HPX_SSHORT;
hpx_type_t HPX_INT_lvalue = HPX_INT;
hpx_type_t HPX_UINT_lvalue = HPX_UINT;
hpx_type_t HPX_SINT_lvalue = HPX_SINT;
hpx_type_t HPX_LONG_lvalue = HPX_LONG;
hpx_type_t HPX_ULONG_lvalue = HPX_ULONG;
hpx_type_t HPX_SLONG_lvalue = HPX_SLONG;
hpx_type_t HPX_VOID_lvalue = HPX_VOID;
hpx_type_t HPX_UINT8_lvalue = HPX_UINT8;
hpx_type_t HPX_SINT8_lvalue = HPX_SINT8;
hpx_type_t HPX_UINT16_lvalue = HPX_UINT16;
hpx_type_t HPX_SINT16_lvalue = HPX_SINT16;
hpx_type_t HPX_UINT32_lvalue = HPX_UINT32;
hpx_type_t HPX_SINT32_lvalue = HPX_SINT32;
hpx_type_t HPX_UINT64_lvalue = HPX_UINT64;
hpx_type_t HPX_SINT64_lvalue = HPX_SINT64;
hpx_type_t HPX_FLOAT_lvalue = HPX_FLOAT;
hpx_type_t HPX_DOUBLE_lvalue = HPX_DOUBLE;
hpx_type_t HPX_POINTER_lvalue = HPX_POINTER;
hpx_type_t HPX_LONGDOUBLE_lvalue = HPX_LONGDOUBLE;
// hpx_type_t HPX_COMPLEX_FLOAT_lvalue = HPX_COMPLEX_FLOAT;
// hpx_type_t HPX_COMPLEX_DOUBLE_lvalue = HPX_COMPLEX_DOUBLE;
// hpx_type_t HPX_COMPLEX_LONGDOUBLE_lvalue = HPX_COMPLEX_LONGDOUBLE;
hpx_type_t HPX_ADDR_lvalue = HPX_ADDR;
hpx_type_t HPX_SIZE_T_lvalue = HPX_SIZE_T;

typedef struct thread_state_map thread_state_map;
struct thread_state_map {
    int tls_id;
    PyThreadState* ts;
    UT_hash_handle hh;
};
static thread_state_map* dict = NULL;
static pthread_mutex_t dict_lock = PTHREAD_MUTEX_INITIALIZER;
static int autoTLSkey = 0;

static void begin_callback(void)
{
}

static void before_transfer_callback(void)
{
    // Get thread state and tls_id 
    PyThreadState* current_thread_state = PyGILState_GetThisThreadState();
    int tls_id = hpx_thread_get_tls_id();
    
    if(current_thread_state != NULL) { 
        
        // Construct map
        thread_state_map* current_map = malloc(sizeof(thread_state_map));
        current_map->tls_id = tls_id;
        current_map->ts = current_thread_state;

        if(pthread_mutex_lock(&dict_lock) != 0) {
            fprintf(stderr, \"Fatal: Error acquiring thread state dict mutex lock!\\n\");
            exit(EXIT_FAILURE);
        }
        
        // Add map to dictionary
        HASH_ADD_INT(dict, tls_id, current_map);

        if(pthread_mutex_unlock(&dict_lock) != 0) {
            fprintf(stderr, \"Fatal: Error releasing thread state dict mutex lock!\\n\");
            exit(EXIT_FAILURE);
        }
        
        assert(current_thread_state == PyThread_get_key_value(autoTLSkey));

        // modify TLS
        PyThread_delete_key_value(autoTLSkey);
    }
}

static void after_transfer_callback(void)
{
    
    if(pthread_mutex_lock(&dict_lock) != 0) {
        fprintf(stderr, \"Fatal: Error acquiring thread state dict mutex lock!\\n\");
        exit(EXIT_FAILURE);
    }

    // Search for dict to check whether this lightweight thread has executed 
    // before
    int tls_id = hpx_thread_get_tls_id();
    thread_state_map* target_map;
    HASH_FIND_INT(dict, &tls_id, target_map);
    PyThreadState* target_thread_state = 
    (target_map == NULL ? NULL : target_map->ts); 
    
    // Delete item in dict and free resources
    if(target_map != NULL) {
        HASH_DEL(dict, target_map);
        free(target_map);
    }
    
    if(pthread_mutex_unlock(&dict_lock) != 0) {
        fprintf(stderr, \"Fatal: Error releasing thread state dict mutex lock!\\n\");
        exit(EXIT_FAILURE);
    }

    if(target_thread_state != NULL) {

        assert(PyGILState_GetThisThreadState() == NULL);

        // Restore the old lightweight thread
        target_thread_state -> thread_id = PyThread_get_thread_ident();       
        if(PyThread_set_key_value(autoTLSkey, (void *)target_thread_state) < 0) {
            fprintf(stderr, \"Fatal: Cannot update TLS\\n\");
            exit(EXIT_FAILURE);
        }
    }
}

int hpx_custom_init(int *argc, char ***argv)
{
    register_begin_callback((CallbackType) begin_callback);
    register_before_transfer_callback((CallbackType) before_transfer_callback);
    register_after_transfer_callback((CallbackType) after_transfer_callback);

    int rtv = hpx_init(argc, argv);
    return rtv;
}

void hpx_custom_finalize(void)
{
    hpx_finalize();
}

""",
               libraries=compile_libraries,
               include_dirs=compile_include_dirs,
               library_dirs=compile_library_dirs,
               extra_compile_args=compile_extra_compile_args.append('-g')
)

if __name__ == "__main__":
    ffi.compile(verbose=True)
