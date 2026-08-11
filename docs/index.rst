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

Failures never arrive silently. A single operation has nowhere to put "I never got an answer", so infrastructure trouble (a refused connection, a timeout, a malformed response, a value that will not deserialize) raises ``OperationFailedError`` with the original cause attached, and a write that was sent but never acknowledged raises ``AmbiguousWriteError``. Semantic outcomes such as a miss, an ``add`` on a taken key or a CAS mismatch are real answers, so they stay in the returned status where you can branch on them. Call ``check()`` when success is the only outcome you accept:

.. code-block:: python

       client.add("lock", token, ttl=30).check()   # raises AlreadyExistsError if taken
       client.cas("key", value, token).check()     # raises CasMismatchError if it moved
       value = client.get("key").check().value

A batch does have somewhere to put per-operation failure, so one unreachable server spoils only its own operations and the rest of the results stand. Inspect them with ``results.failures``, or opt the whole batch into raising with ``results.raise_for_failures()``.

For protocol experts, ``client.meta`` maps the wire commands one-to-one (``mg``/``ms``/``md``/``ma``/``me``) with one keyword argument per protocol flag. It works on raw bytes and returns lightly parsed responses without serialization or semantic mapping.

.. automodule:: memcache.experiment
   :members:
   :undoc-members:

License
-------

Memcache is distributed by a `MIT license <https://github.com/aisk/memcache/tree/master/LICENSE>`_.
