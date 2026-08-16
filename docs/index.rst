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

Scenario client (experimental)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   ``Memcache`` and ``AsyncMemcache`` under ``memcache.experiment`` may change in any minor release. If you depend on them, pin the minor version in your dependency spec (e.g. ``memcache~=0.14.0``).

``memcache.experiment.Memcache`` is a scenario-level client built on memcached's meta protocol: one method per usage scenario, business values in and out. Reads answer a miss with a default instead of an error, failures leave through exceptions, and protocol concepts (CAS tokens, lease flags, stale markers) never appear in caller code. Policies such as the serializer, a key prefix, and the failure behavior live in the constructor; every write states its lifetime (``FOREVER`` for no expiry).

.. code-block:: python

   from memcache.experiment import Memcache, JsonSerializer

   with Memcache(("localhost", 11211), serializer=JsonSerializer()) as cache:
       cache.set("user:1", {"name": "alice"}, ttl=600)
       user = cache.get("user:1")

       # Compute on miss, stampede-safe: one winner recomputes per key.
       report = cache.get("report:q3", factory=build_report, ttl=3600)

       # Atomic read-modify-write; conflict retries live in the library.
       cache.update("cart:42", lambda cart: cart + [item], default=[], ttl=1800)

       # Independent operations in one round trip per server.
       with cache.pipeline() as p:
           user = p.get("user:1")
           hits = p.incr("rate:ip", ttl=60)
       print(user.value, hits.value)

``AsyncMemcache`` is the same table of verbs plus ``await``; ``factory`` and ``fn`` accept sync or async callables, and refresh-ahead recomputations run as background tasks when the client is used as an async context manager.

Failure behavior is a constructor policy. The default ``on_error="raise"`` surfaces infrastructure trouble as ``OperationFailedError`` (a sent-but-unacknowledged write raises ``AmbiguousWriteError``, and that never degrades). ``on_error="degrade"`` decouples a cache outage from a site outage: reads become misses, a ``get`` with a factory computes locally, blind writes are dropped silently, while operations whose answer feeds business decisions (``add``, ``replace``, ``incr``, ``update``, ``pop``) still raise. Absorbed failures go to the ``on_failure`` hook.

For protocol experts, ``cache.meta`` maps the wire commands one-to-one (``mg``/``ms``/``md``/``ma``/``me``) with one keyword argument per protocol flag. It works on raw bytes and returns lightly parsed responses without serialization or semantic mapping. See ``docs/design-scenario-api.md`` for the design rationale.

.. automodule:: memcache.experiment
   :members:
   :undoc-members:

License
-------

Memcache is distributed by a `MIT license <https://github.com/aisk/memcache/tree/master/LICENSE>`_.
