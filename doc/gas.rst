Global Address Space (GAS)
==========================

.. toctree::
   :maxdepth: 2

GAS Allocation
--------------
.. automethod:: hpx.GlobalMemory.alloc_cyclic
.. automethod:: hpx.GlobalMemory.calloc_cyclic
.. automethod:: hpx.GlobalMemory.alloc_local_at

Interaction Between Local and Global Memory
-------------------------------------------
.. automethod:: hpx.GlobalAddressBlock.try_pin
.. automethod:: hpx.GlobalAddressBlock.unpin
.. automethod:: hpx.GlobalAddressBlock.get
.. automethod:: hpx.GlobalAddressBlock.set

Free previous allocated GlobalMemory
------------------------------------
.. automethod:: hpx.GlobalMemory.free
.. automethod:: hpx.GlobalMemory.free_sync

Indexing
--------
.. automethod:: hpx.GlobalMemory.__getitem__
