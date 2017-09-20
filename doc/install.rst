Installation
============

1. Get the latest version of HPX-5 `here <https://hpx.crest.iu.edu/download>`_.
2. Install HPX-5 runtime according to `HPX-5 documentation <https://hpx.crest.iu.edu/users_guide#building_and_installing>`_.
3. Make sure INSTALL_DIRECTORY/lib is included in LD_LIBRARY_PATH and INSTALL_DIRECTORY/lib/pkgconfig is included in PKG_CONFIG_PATH.
4. Checkout and install modified Python-CFFI, available `here <https://github.com/gaohao95/cffi>`_.
5. Build PyHPX by running build_cffi.py script.
6. Add PyHPX/source to PYTHONPATH.