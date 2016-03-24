from cffi import FFI
import subprocess
ffi = FFI()

# get the compilation flags
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

// Port argument types
typedef struct _ffi_type ffi_type;
typedef ffi_type *hpx_type_t;
hpx_type_t HPX_INT_lvalue;

// Port action types
typedef uint16_t hpx_action_t;
typedef enum {
  HPX_DEFAULT = 0,
  HPX_TASK,
  HPX_INTERRUPT,
  HPX_FUNCTION,
  HPX_OPENCL,
} hpx_action_type_t;

// Port parcel API
typedef short hpx_status_t;
typedef struct hpx_parcel hpx_parcel_t;
hpx_parcel_t *hpx_parcel_acquire(const void *data, size_t bytes);
hpx_status_t hpx_parcel_send_sync(hpx_parcel_t *p);
void hpx_parcel_set_action(hpx_parcel_t *p, hpx_action_t action);

// Port action API
int hpx_init(int *argc, char ***argv);
void hpx_finalize();
void hpx_exit(int code);
int _hpx_run(hpx_action_t *entry, int nargs, ...);
int hpx_register_action(hpx_action_type_t type, uint32_t attr, const char *key,
                        hpx_action_t *id, unsigned n, ...);
""")

ffi.set_source("_hpx",
"""
#include <hpx/hpx.h>
hpx_type_t HPX_INT_lvalue = HPX_INT;

""",
               libraries=compile_libraries,
               include_dirs=compile_include_dirs,
               library_dirs=compile_library_dirs,
               extra_compile_args=compile_extra_compile_args.append('-g'))

if __name__ == "__main__":
    ffi.compile(verbose=True)
