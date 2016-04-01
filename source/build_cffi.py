from cffi import FFI
import subprocess
ffi = FFI()

# Get the compilation flags
compile_libraries = []
compile_include_dirs = []
compile_library_dirs = []
compile_extra_compile_args = []
pkg_config_process = subprocess.run(
    ["pkg-config", "--libs", "hpx"],
    stdout=subprocess.PIPE,
    universal_newlines=True)
pkg_libs_result = pkg_config_process.stdout[:-1]
for argument in pkg_libs_result.split():
    if argument[:2] == "-L":
        compile_library_dirs.append(argument[2:])
    elif argument[:2] == "-l":
        compile_libraries.append(argument[2:])
    else:
        compile_extra_compile_args.append(argument)
pkg_config_process = subprocess.run(
    ["pkg-config", "--cflags", "hpx"],
    stdout=subprocess.PIPE,
    universal_newlines=True
)
pkg_libs_result = pkg_config_process.stdout[:-1]
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
hpx_type_t HPX_COMPLEX_FLOAT_lvalue;
hpx_type_t HPX_COMPLEX_DOUBLE_lvalue;
hpx_type_t HPX_COMPLEX_LONGDOUBLE_lvalue;

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
void hpx_finalize();
void hpx_exit(int code);
int _hpx_run(hpx_action_t *entry, int nargs, ...);

/* End Runtime.h */

// Port parcel API
typedef short hpx_status_t;
typedef struct hpx_parcel hpx_parcel_t;
hpx_parcel_t *hpx_parcel_acquire(const void *data, size_t bytes);
hpx_status_t hpx_parcel_send_sync(hpx_parcel_t *p);
void hpx_parcel_set_action(hpx_parcel_t *p, hpx_action_t action);


""")

ffi.set_source("build._hpx",
"""
#include <hpx/hpx.h>
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
hpx_type_t HPX_COMPLEX_FLOAT_lvalue = HPX_COMPLEX_FLOAT;
hpx_type_t HPX_COMPLEX_DOUBLE_lvalue = HPX_COMPLEX_DOUBLE;
hpx_type_t HPX_COMPLEX_LONGDOUBLE_lvalue = HPX_COMPLEX_LONGDOUBLE;
""",
               libraries=compile_libraries,
               include_dirs=compile_include_dirs,
               library_dirs=compile_library_dirs,
               extra_compile_args=compile_extra_compile_args.append('-g')
)

if __name__ == "__main__":
    ffi.compile(verbose=True)
