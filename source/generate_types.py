# This script generates C code needed for porting HPX callback types.

# The following code snipped are copied from "types.h" line 48-77
all_def = """
#define HPX_CHAR               &ffi_type_schar
#define HPX_UCHAR              &ffi_type_uchar
#define HPX_SCHAR              &ffi_type_schar
#define HPX_SHORT              &ffi_type_sshort
#define HPX_USHORT             &ffi_type_ushort
#define HPX_SSHORT             &ffi_type_sshort
#define HPX_INT                &ffi_type_sint
#define HPX_UINT               &ffi_type_uint
#define HPX_SINT               &ffi_type_sint
#define HPX_LONG               &ffi_type_slong
#define HPX_ULONG              &ffi_type_ulong
#define HPX_SLONG              &ffi_type_slong
#define HPX_VOID               &ffi_type_void
#define HPX_UINT8              &ffi_type_uint8
#define HPX_SINT8              &ffi_type_sint8
#define HPX_UINT16             &ffi_type_uint16
#define HPX_SINT16             &ffi_type_sint16
#define HPX_UINT32             &ffi_type_uint32
#define HPX_SINT32             &ffi_type_sint32
#define HPX_UINT64             &ffi_type_uint64
#define HPX_SINT64             &ffi_type_sint64
#define HPX_FLOAT              &ffi_type_float
#define HPX_DOUBLE             &ffi_type_double
#define HPX_POINTER            &ffi_type_pointer
#define HPX_LONGDOUBLE         &ffi_type_longdouble
#define HPX_COMPLEX_FLOAT      &ffi_type_complex_float
#define HPX_COMPLEX_DOUBLE     &ffi_type_complex_double
#define HPX_COMPLEX_LONGDOUBLE &ffi_type_complex_longdouble
"""

with open('types_def.txt', 'w') as f:
    for line in all_def.splitlines():
        if len(line) == 0:
            continue
        line_split = line.split()
        f.write("hpx_type_t " + line_split[1] + "_lvalue;\n")


with open('types_assig.txt', 'w') as f:
    for line in all_def.splitlines():
        if len(line) == 0:
            continue
        line_split = line.split()
        f.write("hpx_type_t " + line_split[1] + "_lvalue = " + line_split[1] + ";\n")