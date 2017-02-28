from distutils.core import setup, Extension

giltestmodule = Extension('giltest', 
                          sources = ['giltest.c'],
                          extra_compile_args = ["-O0"])

setup (name = 'giltest',
       description = 'Testing GIL behavoir',
       ext_modules = [giltestmodule])
