Action
======

**Action** is globally callable object used as thread entry point for lightweight 
thread. **Action** object can be created by one of the functions in the Action
Creation section. There are two types of **Action**, the default one, is simply
represented by **Action** class. The other one is represented by **Function** class, 
which is used for callback by **LCO** like **Reduce**. Both **Action** class and
**Function** class are subclass of **BaseAction**, which defines common behaviors.

.. toctree::
   :maxdepth: 2

Action Creation
---------------
.. autofunction:: hpx.create_action

Argument Types
--------------
.. autoclass:: hpx.Type
   :members:
   :undoc-members: