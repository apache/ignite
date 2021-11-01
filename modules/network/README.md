This module provides implementations for the Network API module.

## Brief overview
Ignite uses ScaleCube to form a network cluster and exchange messages.

## Message serialization
Ignite uses direct marshalling to serialize and deserialize messages.
For every `@Transferable` message interface `ignite-network-annotation-processor` generates
an implementation for the message interface, a serializer and a deserializer.
Supported types:
 + All primitives
 + Other `@Transferable` objects
 + `java.lang.String`
 + `java.util.UUID`
 + `org.apache.ignite.lang.IgniteUuid`
 + `java.util.BitSet`
 + `java.util.Collection<V>` where `V` can be any supported type
 + `java.util.Map<K, V>` where `K` and `V` can be any supported type
 + Arrays of all supported types

## Threading
Every Ignite node has three network thread pool executors and thread naming formats:
+ Client worker - handles channel events on a client (`{consistentId}-client-X`)
+ Server boss - accepting incoming connections (`{consistentId}-srv-accept-X`)
+ Server worker - handles channel events on a server (`{consistentId}-srv-worker-X`),
where `X` is the index of the thread in a pool.

Messages are then passed on to message listeners of the ConnectionManager.   
In case of ClusterService over ScaleCube (see `ScaleCubeClusterServiceFactory`),
messages are passed down to the ClusterService via the Project Reactor's Sink which enforces a strict order of message handling:
a new message can't be received by ClusterService until a previous message is **handled** (see [message handling](#message-handling)).
![Threading](docs/threading.png)
Message handling can also be offloaded to another thread:
![Threading](docs/threading-2.png)
Note that in this case the network message would be considered **handled** before it is processed  
by another thread.

ScaleCube uses `sc-cluster-X` (where X is an address of the node, e.g. `localhost-3344`) scheduler for the failure 
detector, gossip protocol, metadata store and the membership protocol.


## Message handling
Message handlers are called in the order they were added.  
Message is considered **handled** after all the message handlers have been invoked.

## Message's flow example
Two nodes, Alice and Bob.
User is sending a message from Alice to Bob within any thread.
![Network flow between two nodes](docs/network-flow.png)
