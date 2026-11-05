# Per-operation allocation inventory of cache operations on master (2026-10-26)

Tree: `master` at 08b8e3ed7b3 (core, binary, nio, unsafe, thin-client modules compiled in a detached worktree; the
`jmh/alloc` harness from `master-2`). Case: Integer key -> `IntValue` value, ATOMIC cache, one client thread.
LOCAL = one server, no backups, operations on the server itself. CLUSTER = 3 servers, 2 backups, FULL_SYNC,
operations issued from a client node, all nodes in one JVM.

Method: `AllocInventoryRunner` in histo mode under Epsilon GC (`-Xms8g -Xmx8g`, no collections). 200000 warm-up
operations, then a class histogram, 20000 measured operations, a second histogram, an idle window of the same
duration and a third histogram. Per op = ((B - A) - (C - B)) / 20000, so background allocation of the running
nodes is subtracted. `count/op` = instances per operation, `size B` = bytes per instance, `Total bytes` =
`count/op` x `size B` = bytes per operation. Tables are sorted by `Total bytes` descending. Rows with fewer than
0.5 instances per operation are omitted (JIT, lambda linkage, periodic workers).

## Summary

| case | objects/op | Total bytes |
|---|---:|---:|
| put LOCAL | 53.1 | 2097 |
| putNew LOCAL | 53.1 | 2133 |
| get LOCAL | 27.1 | 1050 |
| getKeepBinary LOCAL | 18.1 | 628 |
| put CLUSTER | 218.4 | 6822 |
| putNew CLUSTER | 212.8 | 6659 |
| get CLUSTER | 84.6 | 2981 |
| getKeepBinary CLUSTER | 75.7 | 2699 |

## put LOCAL: 53.1 objects/op, 2097 bytes/op

35 classes, 53.0 objects, 2073 bytes per op shown.

| count/op | size B | Total bytes | class |
|---:|---:|---:|---|
| 1.00 | 128 | 128.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateFuture |
| 3.02 | 40 | 120.6 | [B |
| 5.00 | 24 | 120.0 | java.lang.Long |
| 2.00 | 56 | 112.0 | org.apache.ignite.internal.binary.BinaryWriterExImpl |
| 2.00 | 56 | 112.0 | org.apache.ignite.internal.processors.cache.tree.DataRow |
| 1.00 | 104 | 104.0 | org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$Invoke |
| 3.00 | 32 | 96.0 | java.util.concurrent.ConcurrentHashMap$Node |
| 2.00 | 48 | 96.0 | java.util.HashMap |
| 1.00 | 96 | 96.0 | org.apache.ignite.internal.processors.cache.GridCacheMapEntry$AtomicCacheUpdateClosure |
| 1.00 | 88 | 88.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest |
| 2.00 | 40 | 80.0 | org.apache.ignite.internal.binary.BinaryObjectImpl |
| 1.00 | 80 | 80.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream |
| 1.00 | 64 | 64.0 | org.apache.ignite.internal.processors.cache.GridCacheUpdateAtomicResult |
| 1.00 | 64 | 64.0 | org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry |
| 1.00 | 64 | 64.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateFuture |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.processors.cache.version.GridCacheVersion |
| 3.00 | 16 | 48.0 | java.lang.Integer |
| 2.00 | 24 | 48.0 | org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl |
| 2.00 | 24 | 48.0 | org.apache.ignite.internal.util.future.GridFinishedFuture |
| 1.00 | 40 | 40.0 | org.apache.ignite.internal.processors.cache.GridCacheReturn |
| 1.00 | 32 | 32.0 | java.util.concurrent.locks.ReentrantLock$NonfairSync |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.DhtAtomicUpdateResult |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateFuture$PrimaryRequestState |
| 1.00 | 24 | 24.0 | java.util.Collections$SingletonList |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.BinaryReaderHandles |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.BinaryWriterHandles |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.streams.BinaryHeapInputStream |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy$CacheOperationGate |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateFuture$1 |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.tree.SearchRow |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl |
| 1.00 | 16 | 16.0 | java.util.concurrent.locks.ReentrantLock |
| 1.00 | 16 | 16.0 | org.apache.ignite.internal.binary.BinaryReaderHandlesHolder |
| 1.00 | 16 | 16.0 | org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras |

## putNew LOCAL: 53.1 objects/op, 2133 bytes/op

37 classes, 53.0 objects, 2059 bytes per op shown.

| count/op | size B | Total bytes | class |
|---:|---:|---:|---|
| 1.00 | 128 | 128.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateFuture |
| 5.00 | 24 | 120.0 | java.lang.Long |
| 2.00 | 56 | 112.0 | org.apache.ignite.internal.binary.BinaryWriterExImpl |
| 1.00 | 104 | 104.0 | org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$Invoke |
| 1.00 | 104 | 104.0 | org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$Put |
| 3.00 | 32 | 96.0 | java.util.concurrent.ConcurrentHashMap$Node |
| 2.00 | 48 | 96.0 | java.util.HashMap |
| 1.00 | 96 | 96.0 | org.apache.ignite.internal.processors.cache.GridCacheMapEntry$AtomicCacheUpdateClosure |
| 1.00 | 88 | 88.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest |
| 5.00 | 16 | 80.0 | java.lang.Integer |
| 1.00 | 80 | 80.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse |
| 2.02 | 37 | 74.7 | [B |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream |
| 1.00 | 64 | 64.0 | org.apache.ignite.internal.processors.cache.GridCacheUpdateAtomicResult |
| 1.00 | 64 | 64.0 | org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry |
| 1.00 | 64 | 64.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateFuture |
| 1.00 | 56 | 56.0 | org.apache.ignite.internal.processors.cache.tree.DataRow |
| 2.00 | 24 | 48.0 | org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl |
| 2.00 | 24 | 48.0 | org.apache.ignite.internal.util.future.GridFinishedFuture |
| 1.00 | 40 | 40.0 | org.apache.ignite.internal.binary.BinaryObjectImpl |
| 1.00 | 40 | 40.0 | org.apache.ignite.internal.processors.cache.GridCacheReturn |
| 1.00 | 32 | 32.0 | java.util.concurrent.locks.ReentrantLock$NonfairSync |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.DhtAtomicUpdateResult |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateFuture$PrimaryRequestState |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.version.GridCacheVersion |
| 1.00 | 24 | 24.0 | java.util.Collections$SingletonList |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.BinaryReaderHandles |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.BinaryWriterHandles |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.streams.BinaryHeapInputStream |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy$CacheOperationGate |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateFuture$1 |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.tree.SearchRow |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl |
| 1.00 | 16 | 16.0 | java.util.concurrent.locks.ReentrantLock |
| 1.00 | 16 | 16.0 | org.apache.ignite.internal.binary.BinaryReaderHandlesHolder |
| 1.00 | 16 | 16.0 | org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras |

## get LOCAL: 27.1 objects/op, 1050 bytes/op

21 classes, 27.0 objects, 915 bytes per op shown.

| count/op | size B | Total bytes | class |
|---:|---:|---:|---|
| 1.00 | 96 | 96.0 | org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedSingleGetFuture |
| 1.00 | 96 | 96.0 | org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$GetOne |
| 1.00 | 88 | 88.0 | org.apache.ignite.internal.binary.BinaryReaderExImpl |
| 5.00 | 16 | 80.0 | java.lang.Integer |
| 2.02 | 37 | 74.7 | [B |
| 1.00 | 56 | 56.0 | org.apache.ignite.internal.binary.BinaryWriterExImpl |
| 1.00 | 56 | 56.0 | org.apache.ignite.internal.processors.cache.tree.DataRow |
| 2.00 | 24 | 48.0 | org.apache.ignite.internal.util.future.GridFinishedFuture |
| 1.00 | 40 | 40.0 | org.apache.ignite.internal.binary.BinaryObjectImpl |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.version.GridCacheVersion |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.BinaryReaderHandles |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.streams.BinaryHeapInputStream |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy$CacheOperationGate |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.tree.SearchRow |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl |
| 1.00 | 24 | 24.0 | org.apache.ignite.lang.IgniteUuid |
| 1.01 | 16 | 16.1 | [Ljava.lang.Object; |
| 1.00 | 16 | 16.0 | org.apache.ignite.internal.benchmarks.model.IntValue |
| 1.00 | 16 | 16.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicCache$15 |

## getKeepBinary LOCAL: 18.1 objects/op, 628 bytes/op

16 classes, 18.0 objects, 682 bytes per op shown.

| count/op | size B | Total bytes | class |
|---:|---:|---:|---|
| 1.00 | 96 | 96.0 | org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedSingleGetFuture |
| 1.00 | 96 | 96.0 | org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$GetOne |
| 2.01 | 37 | 74.5 | [B |
| 1.00 | 56 | 56.0 | org.apache.ignite.internal.binary.BinaryWriterExImpl |
| 1.00 | 56 | 56.0 | org.apache.ignite.internal.processors.cache.tree.DataRow |
| 2.00 | 24 | 48.0 | org.apache.ignite.internal.util.future.GridFinishedFuture |
| 1.00 | 40 | 40.0 | org.apache.ignite.internal.binary.BinaryObjectImpl |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.version.GridCacheVersion |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy$CacheOperationGate |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.tree.SearchRow |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl |
| 1.00 | 24 | 24.0 | org.apache.ignite.lang.IgniteUuid |
| 1.00 | 16 | 16.0 | java.lang.Integer |
| 1.00 | 16 | 16.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicCache$15 |

## put CLUSTER: 218.4 objects/op, 6822 bytes/op

49 classes, 217.5 objects, 7459 bytes per op shown.

| count/op | size B | Total bytes | class |
|---:|---:|---:|---|
| 39.80 | 16 | 636.9 | java.lang.Integer |
| 11.01 | 39 | 429.5 | [B |
| 4.00 | 104 | 416.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest |
| 10.02 | 40 | 400.9 | org.apache.ignite.internal.managers.communication.GridIoMessage |
| 6.00 | 56 | 336.0 | org.apache.ignite.internal.processors.cache.tree.DataRow |
| 10.02 | 32 | 320.7 | org.apache.ignite.internal.GridTopicMessage |
| 3.00 | 104 | 312.0 | org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$Invoke |
| 3.00 | 96 | 288.0 | org.apache.ignite.internal.processors.cache.GridCacheMapEntry$AtomicCacheUpdateClosure |
| 7.00 | 40 | 280.0 | org.apache.ignite.internal.binary.BinaryObjectImpl |
| 4.00 | 64 | 256.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicNearResponse |
| 10.55 | 24 | 253.2 | java.lang.Long |
| 10.02 | 24 | 240.6 | java.util.concurrent.ConcurrentLinkedQueue$Node |
| 8.01 | 24 | 192.3 | org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion |
| 3.00 | 64 | 192.0 | org.apache.ignite.internal.processors.cache.GridCacheUpdateAtomicResult |
| 3.00 | 64 | 192.0 | org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry |
| 6.00 | 32 | 192.0 | org.apache.ignite.internal.processors.cache.version.GridCacheVersion |
| 2.00 | 88 | 176.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest |
| 5.01 | 32 | 160.4 | org.apache.ignite.internal.managers.communication.GridIoManager$6 |
| 5.01 | 32 | 160.4 | org.apache.ignite.internal.util.nio.GridNioServer$WriteRequestImpl |
| 5.00 | 32 | 160.0 | java.util.concurrent.ConcurrentHashMap$Node |
| 4.01 | 32 | 128.4 | java.util.HashMap$Node |
| 4.00 | 32 | 128.0 | java.util.UUID |
| 1.00 | 128 | 128.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateFuture |
| 5.01 | 24 | 120.3 | java.util.concurrent.ConcurrentLinkedDeque$Node |
| 2.00 | 56 | 112.0 | org.apache.ignite.internal.binary.BinaryWriterExImpl |
| 2.00 | 48 | 96.0 | java.util.HashMap |
| 3.00 | 32 | 96.0 | java.util.concurrent.locks.ReentrantLock$NonfairSync |
| 4.00 | 24 | 96.0 | org.apache.ignite.internal.binary.streams.BinaryHeapInputStream |
| 2.00 | 40 | 80.0 | org.apache.ignite.internal.processors.cache.GridCacheReturn |
| 1.00 | 80 | 80.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse |
| 3.00 | 24 | 72.0 | org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl |
| 3.00 | 24 | 72.0 | org.apache.ignite.internal.processors.cache.tree.SearchRow |
| 2.00 | 32 | 64.0 | [Ljava.util.HashMap$Node; |
| 4.00 | 16 | 64.0 | org.apache.ignite.internal.binary.BinaryReaderHandlesHolder |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream |
| 1.00 | 64 | 64.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateFuture |
| 3.00 | 16 | 48.0 | java.util.concurrent.locks.ReentrantLock |
| 3.00 | 16 | 48.0 | org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras |
| 2.00 | 24 | 48.0 | org.apache.ignite.internal.util.future.GridFinishedFuture |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.DhtAtomicUpdateResult |
| 2.00 | 16 | 32.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateFuture$NodeResult |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateFuture$PrimaryRequestState |
| 1.00 | 24 | 24.0 | java.util.Collections$SingletonList |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.BinaryReaderHandles |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.BinaryWriterHandles |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy$CacheOperationGate |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.util.future.GridFutureAdapter$Node |
| 1.00 | 16 | 16.0 | java.util.HashMap$Values |

## putNew CLUSTER: 212.8 objects/op, 6659 bytes/op

50 classes, 211.9 objects, 7296 bytes per op shown.

| count/op | size B | Total bytes | class |
|---:|---:|---:|---|
| 43.81 | 16 | 700.9 | java.lang.Integer |
| 4.00 | 104 | 416.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest |
| 10.02 | 40 | 400.9 | org.apache.ignite.internal.managers.communication.GridIoMessage |
| 10.02 | 32 | 320.7 | org.apache.ignite.internal.GridTopicMessage |
| 3.00 | 104 | 312.0 | org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$Invoke |
| 3.00 | 104 | 312.0 | org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$Put |
| 8.02 | 36 | 288.7 | [B |
| 3.00 | 96 | 288.0 | org.apache.ignite.internal.processors.cache.GridCacheMapEntry$AtomicCacheUpdateClosure |
| 4.00 | 64 | 256.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicNearResponse |
| 10.02 | 24 | 240.6 | java.util.concurrent.ConcurrentLinkedQueue$Node |
| 9.95 | 24 | 238.7 | java.lang.Long |
| 8.01 | 24 | 192.3 | org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion |
| 3.00 | 64 | 192.0 | org.apache.ignite.internal.processors.cache.GridCacheUpdateAtomicResult |
| 3.00 | 64 | 192.0 | org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry |
| 2.00 | 88 | 176.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest |
| 3.00 | 56 | 168.0 | org.apache.ignite.internal.processors.cache.tree.DataRow |
| 5.01 | 32 | 160.4 | org.apache.ignite.internal.managers.communication.GridIoManager$6 |
| 5.01 | 32 | 160.4 | org.apache.ignite.internal.util.nio.GridNioServer$WriteRequestImpl |
| 5.00 | 32 | 160.0 | java.util.concurrent.ConcurrentHashMap$Node |
| 4.00 | 40 | 160.0 | org.apache.ignite.internal.binary.BinaryObjectImpl |
| 4.01 | 32 | 128.4 | java.util.HashMap$Node |
| 4.00 | 32 | 128.0 | java.util.UUID |
| 1.00 | 128 | 128.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateFuture |
| 5.01 | 24 | 120.3 | java.util.concurrent.ConcurrentLinkedDeque$Node |
| 2.00 | 56 | 112.0 | org.apache.ignite.internal.binary.BinaryWriterExImpl |
| 2.00 | 48 | 96.0 | java.util.HashMap |
| 3.00 | 32 | 96.0 | java.util.concurrent.locks.ReentrantLock$NonfairSync |
| 4.00 | 24 | 96.0 | org.apache.ignite.internal.binary.streams.BinaryHeapInputStream |
| 3.00 | 32 | 96.0 | org.apache.ignite.internal.processors.cache.version.GridCacheVersion |
| 2.00 | 40 | 80.0 | org.apache.ignite.internal.processors.cache.GridCacheReturn |
| 1.00 | 80 | 80.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse |
| 3.00 | 24 | 72.0 | org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl |
| 3.00 | 24 | 72.0 | org.apache.ignite.internal.processors.cache.tree.SearchRow |
| 2.00 | 32 | 64.0 | [Ljava.util.HashMap$Node; |
| 4.00 | 16 | 64.0 | org.apache.ignite.internal.binary.BinaryReaderHandlesHolder |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream |
| 1.00 | 64 | 64.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateFuture |
| 3.00 | 16 | 48.0 | java.util.concurrent.locks.ReentrantLock |
| 3.00 | 16 | 48.0 | org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras |
| 2.00 | 24 | 48.0 | org.apache.ignite.internal.util.future.GridFinishedFuture |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.DhtAtomicUpdateResult |
| 2.00 | 16 | 32.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateFuture$NodeResult |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateFuture$PrimaryRequestState |
| 1.00 | 24 | 24.0 | java.util.Collections$SingletonList |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.BinaryReaderHandles |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.BinaryWriterHandles |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy$CacheOperationGate |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.util.future.GridFutureAdapter$Node |
| 1.00 | 16 | 16.0 | java.util.HashMap$Values |

## get CLUSTER: 84.6 objects/op, 2981 bytes/op

41 classes, 84.3 objects, 2685 bytes per op shown.

| count/op | size B | Total bytes | class |
|---:|---:|---:|---|
| 17.31 | 16 | 276.9 | java.lang.Integer |
| 2.00 | 88 | 176.0 | org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest |
| 4.00 | 40 | 160.0 | org.apache.ignite.internal.managers.communication.GridIoMessage |
| 4.01 | 36 | 144.2 | [B |
| 4.00 | 32 | 128.0 | org.apache.ignite.internal.GridTopicMessage |
| 2.00 | 64 | 128.0 | org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse |
| 1.00 | 96 | 96.0 | org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedSingleGetFuture |
| 1.00 | 96 | 96.0 | org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$GetOne |
| 4.00 | 24 | 96.0 | java.util.concurrent.ConcurrentLinkedQueue$Node |
| 1.00 | 88 | 88.0 | org.apache.ignite.internal.binary.BinaryReaderExImpl |
| 2.00 | 40 | 80.0 | org.apache.ignite.internal.binary.BinaryObjectImpl |
| 1.00 | 80 | 80.0 | org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtGetSingleFuture |
| 3.00 | 24 | 72.0 | org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion |
| 3.00 | 24 | 72.0 | org.apache.ignite.internal.util.future.GridFinishedFuture |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.managers.communication.GridIoManager$6 |
| 1.00 | 64 | 64.0 | org.apache.ignite.internal.processors.cache.GridCacheEntryInfo |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.processors.cache.distributed.near.CacheVersionedValue |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.processors.cache.version.GridCacheVersion |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.util.nio.GridNioServer$WriteRequestImpl |
| 1.00 | 56 | 56.0 | org.apache.ignite.internal.binary.BinaryWriterExImpl |
| 1.00 | 56 | 56.0 | org.apache.ignite.internal.processors.cache.tree.DataRow |
| 2.00 | 24 | 48.0 | java.util.concurrent.ConcurrentLinkedDeque$Node |
| 2.00 | 24 | 48.0 | org.apache.ignite.internal.binary.streams.BinaryHeapInputStream |
| 2.00 | 24 | 48.0 | org.apache.ignite.internal.util.future.GridFutureAdapter$Node |
| 2.00 | 24 | 48.0 | org.apache.ignite.lang.IgniteUuid |
| 1.00 | 32 | 32.0 | java.util.concurrent.ConcurrentHashMap$Node |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter$8 |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.BinaryReaderHandles |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.EntryGetResult |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy$CacheOperationGate |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.tree.SearchRow |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.thread.context.function.OperationContextAwareInClosure |
| 1.00 | 24 | 24.0 | org.apache.ignite.lang.IgniteBiTuple |
| 1.01 | 16 | 16.1 | [Ljava.lang.Object; |
| 1.00 | 16 | 16.0 | java.util.Collections$SingletonSet |
| 1.00 | 16 | 16.0 | org.apache.ignite.internal.benchmarks.model.IntValue |
| 1.00 | 16 | 16.0 | org.apache.ignite.internal.binary.BinaryReaderHandlesHolder |
| 1.00 | 16 | 16.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicCache$15 |

## getKeepBinary CLUSTER: 75.7 objects/op, 2699 bytes/op

37 classes, 75.3 objects, 2453 bytes per op shown.

| count/op | size B | Total bytes | class |
|---:|---:|---:|---|
| 13.31 | 16 | 212.9 | java.lang.Integer |
| 2.00 | 88 | 176.0 | org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest |
| 4.00 | 40 | 160.0 | org.apache.ignite.internal.managers.communication.GridIoMessage |
| 4.01 | 36 | 144.3 | [B |
| 4.00 | 32 | 128.0 | org.apache.ignite.internal.GridTopicMessage |
| 2.00 | 64 | 128.0 | org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse |
| 1.00 | 96 | 96.0 | org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedSingleGetFuture |
| 1.00 | 96 | 96.0 | org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$GetOne |
| 4.00 | 24 | 96.0 | java.util.concurrent.ConcurrentLinkedQueue$Node |
| 2.00 | 40 | 80.0 | org.apache.ignite.internal.binary.BinaryObjectImpl |
| 1.00 | 80 | 80.0 | org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtGetSingleFuture |
| 3.00 | 24 | 72.0 | org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion |
| 3.00 | 24 | 72.0 | org.apache.ignite.internal.util.future.GridFinishedFuture |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.managers.communication.GridIoManager$6 |
| 1.00 | 64 | 64.0 | org.apache.ignite.internal.processors.cache.GridCacheEntryInfo |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.processors.cache.distributed.near.CacheVersionedValue |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.processors.cache.version.GridCacheVersion |
| 2.00 | 32 | 64.0 | org.apache.ignite.internal.util.nio.GridNioServer$WriteRequestImpl |
| 1.00 | 56 | 56.0 | org.apache.ignite.internal.binary.BinaryWriterExImpl |
| 1.00 | 56 | 56.0 | org.apache.ignite.internal.processors.cache.tree.DataRow |
| 2.00 | 24 | 48.0 | org.apache.ignite.lang.IgniteUuid |
| 2.00 | 24 | 48.0 | java.util.concurrent.ConcurrentLinkedDeque$Node |
| 2.00 | 24 | 48.0 | org.apache.ignite.internal.util.future.GridFutureAdapter$Node |
| 1.00 | 32 | 32.0 | java.util.concurrent.ConcurrentHashMap$Node |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream |
| 1.00 | 32 | 32.0 | org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter$8 |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.binary.streams.BinaryHeapInputStream |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.EntryGetResult |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy$CacheOperationGate |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cache.tree.SearchRow |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl |
| 1.00 | 24 | 24.0 | org.apache.ignite.internal.thread.context.function.OperationContextAwareInClosure |
| 1.00 | 24 | 24.0 | org.apache.ignite.lang.IgniteBiTuple |
| 1.00 | 16 | 16.0 | java.util.Collections$SingletonSet |
| 1.00 | 16 | 16.0 | org.apache.ignite.internal.binary.BinaryReaderHandlesHolder |
| 1.00 | 16 | 16.0 | org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicCache$15 |

