# Ignite Network API module

This module provides an abstraction and core interfaces of the networking layer for Ignite. The module provides
two major functions:

* Discovery and group membership. The networking layer interface provides weakly-consistent discovery and group
  membership service which uses the Scalecube implementation of the SWIM gossip protocol. The default implementation
  requires a set of seed IP addresses to provided on start.
* Messaging service. The networking layer provides several useful abstractions that can be used for sending and
  receiving messages across nodes in the cluster. Several delivery guarantee options are available:
    * Weak mode provides a 'shoot and forget' style of sending a message. These messages are not tracked for delivery and
      may be dropped by either sender or receiver. This is the preferred way of communication between nodes as such messages
      do not incur any risks of OOME or other resource shortage. For example, Raft server messages should be sent via weak
      mode because all Raft messages can be arbitrarily dropped.
    * Patient mode attempts to deliver a message until either it is acknowledged, or we received a notification that a
      target cluster member left the cluster. Internally, the networking module preserves a queue which orders scheduled
      messages and persistently attempts to establish a connection and deliver scheduled messages in that particular order.
      Messages sent via patient mode cannot be arbitrarily dropped and are kept in a memory buffer until either they are
      delivered or a destination node is reported as failed.

On top of the described primitives, the networking module provides a higher-level request-response primitive which can
be thought of as an RPC call, implying a single response for the given request. This primitive requires that the message
being sent has a unique identifier that can be matched with response on receipt.

## Concepts and interfaces

This module provides the following interfaces and implementations:

1. `ClusterService` interface represents the current node and the entry point for network-related activity in a cluster.
2. `ClusterLocalConfiguration` contains some state of the current node, e.g. its alias and configuration.
3. `ClusterServiceFactory` is the main way of starting a node.
4. `TopologyService` provides information about the cluster members and allows registering listeners for topology change
   events.
5. `MessagingService` provides a mechanism for sending messages between network members in both weak and patient mode
   and allows registering listeners for events related to cluster member communication.
