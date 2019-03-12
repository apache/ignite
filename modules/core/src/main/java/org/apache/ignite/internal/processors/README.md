Apache Ignite Processors
------------------------

A processor is Apache Ignite component with the lifecycle. This lifecycle is associated with Ignite Node lifecycle.

Despite Managers, Ignite processors are not associated with an SPI.

Cache Processors and Implementation
-----------------------------------
Main grid function from the point of end-user view is a mapping of keys (K) to values (V)
```K->V```
This mapping is implemented by [cache](cache)

There is also an affinity key. Usually Key and Affinity Key are equivalent.

But collocation of data may require transformation from Key to
```K->Affinity Key```

Affinity key is mapped to [cache](cache) partition
```K->Affinity Key->Partition```

Affinity Key to partition mapping should always be static (any conditions, any JVM). Number of partitions is constant and does not change during grid lifetime, 1024 default

Affinity Function is also responsible for mapping from partition to target (ideal) node:
```Partition->Node```

There are primary nodes and backup (default # of backups = 0 for performance reasons). For replicated cache backups count = nodes count.