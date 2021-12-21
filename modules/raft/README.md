# Ignite module for RAFT (Reliable, Replicated, Redundant, And Fault-Tolerant) protocol implementation.

This module contains a fork of a JRaft project <sup id="a1">[1](#f1)</sup>

## Top level overview

The main entry point is `JRaftServerImpl`, which starts server node for handling RAFT messages.
Responsible for managing one or many RAFT replication groups, see `NodeImpl`.

A RAFT node represents a single group node, which can be voting or non-voting (so called learner).

## Threading model

The implementation uses several thread pools and threads for different purposes.

Each node can work in shared pools mode or start it's dedicated set of pools.
When using many groups it's recommended to use shared mode, otherwise, threads count may be too big.

Shared pools can be passed to `NodeOptions` as parameter values.

Currently, the following pools are used:

### JRaft-Common-Executor
A pool for processing short-lived asynchronous tasks.
Should never be blocked.

### JRaft-Node-Scheduler
A scheduled executor for running delayed or repeating tasks.

### JRaft-Request-Processor
A default pool for handling RAFT requests.
Should never be blocked.

### JRaft-Response-Processor
A default pool for handling RAFT responses.
Should never be blocked.

### JRaft-AppendEntries-Processor
A pool of single thread executors.
Used only if a replication pipelining is enabled. 
Handles append entries requests and responses (used by the replication flow). 
Threads are started on demand.
Each replication pair (leader-follower) uses dedicated single thread executor from the pool, so all messages 
between replication peer pairs are processed sequentially.

### NodeImpl-Disruptor
A striped disruptor for batching FSM (finite state machine) user tasks.

### ReadOnlyService-Disruptor
A striped disruptor for batching read requests before doing read index request.

### LogManager-Disruptor
A striped disruptor for delivering log entries to a storage.

### FSMCaller-Disruptor
A striped disruptor for FSM callbacks.

### SnapshotTimer
A timer for periodic snapshot creation.

### ElectionTimer
A timer to handle election timeout on followers.

### VoteTimer
A timer to handle vote timeout when a leader was not confirmed by majority.

### StepDownTimer
A timer to process leader step down condition.

## Snapshots threading details.

onSnapshotSave and onSnapshotLoad are called from a FSMCaller disruptor thread. 
So, snapshot creation must be delegated to another thread/executor to avoid blocking the progress.
All other snapshot related messages are processed in Request/Response pools.

<em id="f1">[1]</em> <a href="https://github.com/sofastack/sofa-jraft">JRaft repository</a> [↩](#a1)<br/>