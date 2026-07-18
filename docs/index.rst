Memcache Client for Python
==========================

Experimental memcached client library for python. This project is in WIP status, please don't use it in production environment.

Key features:

* Based on memcached's new meta commands;
* Asyncio support;
* Type hints.

Installation
------------

.. code-block:: bash

   $ pip install memcache

API document
------------

Core client
~~~~~~~~~~~

.. automodule:: memcache
   :members:
   :undoc-members:

Meta client (experimental)
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   ``MetaClient`` and ``AsyncMetaClient`` live under ``memcache.experiment`` and their API may change in any minor release. If you depend on them, pin the minor version in your dependency spec (e.g. ``memcache~=0.14.0``).

``MetaClient`` is a high-level client for memcached's meta protocol. The core methods (``get``/``set``/``delete``/``increment``/``batch``) cover the full protocol surface, while convenience wrappers such as ``add``, ``cas``, ``invalidate`` and ``get_with_lease`` package common usage patterns with safer defaults. Every result has an explicit status.

.. code-block:: python

   from memcache.experiment import Get, GetStatus, Meta, MetaClient, Set

   with MetaClient(("localhost", 11211)) as client:
       client.set("key", {"message": "value"}, ttl=60)

       result = client.get("key", meta=Meta.CAS | Meta.TTL | Meta.SIZE)
       if result.status is GetStatus.HIT:
           print(result.value, result.item.cas, result.item.ttl)

``AsyncMetaClient`` has the same concepts and call shape; its methods and lease ``fulfill()`` are awaited.

For protocol experts, ``client.meta`` maps the wire commands one-to-one (``mg``/``ms``/``md``/``ma``/``me``) with one keyword argument per protocol flag. It works on raw bytes and returns lightly parsed responses without serialization or semantic mapping.

.. automodule:: memcache.experiment
   :members:
   :undoc-members:

License
-------

Memcache is distributed by a `MIT license <https://github.com/aisk/memcache/tree/master/LICENSE>`_.
