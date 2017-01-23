# HPX-Python

## Installation Guide

1. Checkout the modified version of HPX-5 source code [here](https://github.com/gaohao95/hpx/tree/tmp).
2. Switch to the **tmp** branch and install HPX-5 runtime according to [HPX-5 documentation](https://hpx.crest.iu.edu/users_guide#building_and_installing).
3. Make sure INSTALL_DIRECTORY/lib is included in LD_LIBRARY_PATH and INSTALL_DIRECTORY/lib/pkgconfig is included in PKG_CONFIG_PATH.
4. Checkout and install modified Python-CFFI, available [here](https://github.com/gaohao95/cffi).
5. Build HPX-Python by running build_cffi.py script.
6. Add HPX-Python/source to PYTHONPATH.
