# Ignite transactions
This module provides transactions support for cross partition operations. Using the transactions, such operations are
executed in atomic way (either all changes all applied, or nothing at all) with a strong isolation.

Transactions support is supposed to be icremental. In the first approach, we are trying to put existing ideas from
ignite 2 to the new consensus based replication infrastructure. 
In the next phases, MVCC support should be added to avoid blocking reads and possibly some other optimization, 
like parallel commits from <sup id="a1">[1](#f1)</sup>

# Transaction protocol design (first iteration)
At a high level, we utilize 2 phase locking (2PL) for concurrency control, 2 phase commit (2PC) as an atomic commitment 
protocol, in conjunction with WAIT_DIE deadlock prevention, described in <sup id="a2">[2](#f2)</sup>. 
This implementation is similar to Ignite 2 optimistic serializable mode. 
Additional goals are: 
1) retain only strong isolation 
2) support for SQL 
3) utilize new replication infrastructure based on RAFT.

# Two phase commit
This protocol is responsible for atomic commitment (all or nothing) tx guraranties.
Each update is **pre-written** to a replication groups on first phase (and replicated to a majority).
As soon as all updates are pre-written, it's safe to commit.
This slightly differs from ignite 2, because there is no PREPARED state.

# Two phase locking
2PL states the transaction constist of growing phase, where locks are acquired, and shrinking phase where locks are released.
A tx acquires shared locks for reads and exclusive locks for writes.
It's possible to lock for read in exclusive mode (select for update semantics)
Locks for different keys can be acquired in parallel.
Shared lock is obtained on DN-read operation.
Exclusive lock is obtained on DN-prewrite operation.

(DN - data node)

# Lock manager
Locking functionality is implemented by LockManager.
Each **leaseholder** for a partition replication group deploys an instance of LockManager. 
All reads and writes go through the **leaseholder**. Only raft leader for some term can become a **leaseholder**.
It's important what no two different leaseholder intervals can overlap for the same group, so a lock for the same key
can't be held on different leaseholders in the same time.

Current leasholder map can be loaded from metastore (watches can be used for a fast notification about leaseholder change)
or discovered on demand by asking raft group nodes.

The simplest implementation of a leaseholder is using raft leader leases, as described if RAFT paper. 
In this approach a leaseholder is a same node as a raft leader.

The lockmanager should keep locks in the offheap to reduce GC pressure, but can be heap based in the first iteration.

# Locking precausion
LockManager has a volatile state, so some precausions must be taken before locking the keys due to possible node restarts.
Before taking a lock, LockManager should consult a tx state for the key (by reading it's metadata if present and looking into txstate map).
If a key is enlisted in transaction and wait is possible according to tx priority, lock cannot be taken immediately.

Consider a scenario when a leaseholder fails holding some lock. When a new leaseholder is elected, it's LockManager state 
will be empty, but some keys will still be actually locked until old transactions are finished by recovery.

# TX coordinator
TX coordination can be done from any grid node. Coordinators can be dedicated nodes or collocated with data.
Coordinators are responsible for id assignment, tx mapping and failover handling if some nodes from tx topology have failed. 
Knows full tx topology just before committing.

# Deadlock prevention
Deadlock prevention in WAIT_DIE mode (described in details in <sup id="a2">[2](#f2)</sup>)- uses tx priorities to decide which tx should be restarted.
Each transaction is assigned a unique globally comparable timestamp (for example UUID), which defines tx priority.
If T1 has lower priority when T2 (T1 is younger) it can wait for lock, otherwise it's restarted keeping it's timestamp.
committing transaction can't be restarted.
Deadlock detection is not an option due to huge computation resources requirement and no real-time guaranties.
This functionality should be implemented by LockManager.

# Tx metadata
Each node maintains a persistent tx map:

txid -> txstate(PENDING|ABORTED|COMMITED)

This map is used for a failover and for reading. Oldest entries in txid map must be cleaned to avoid unlimited grow.

# Data format
A row is stored in key-value database with additional attached metadata for referencing associated tx.
The record format is:
Key -> Tuple (
           new value (as seen from the current transaction's perspective), 
           old value (used for rolling back), 
           timestamp of a last transaction modifiyng the value (or null for new record or after reset)
       )
       
Such format allows O(1) commit (or rollback) time simply by changing tx state, see next paragraph for details.

# Read rules
TX state and timestamp are used for reading with required isolation using the following rules:

```
    BinaryRow read(BinaryRow key, Timestamp myTs) {
        DataRow val = storage.read(key);
        
        Tuple tuple = asTuple(val);

        if (tuple.timestamp == null) { // New or after reset.
            assert tuple.oldRow == null : tuple;

            return tuple.newRow;
        }

        // Checks "read in tx" condition. Will be false if this is a first transactional op.
        if (tuple.timestamp.equals(myTs))
            return tuple.newRow;

        TxState state = txState(tuple.timestamp);

        if (state == ABORTED) // Was aborted and had written a temporary value.
            return tuple.oldRow;
        else
            return tuple.newRow;        
    }           
```

# Write tx example.
Assume the current row is: key -> (value, null, null)
A transaction with timestamp = "ts" tries to update a value to newValue.

The steps to update a row:

1. acquire write lock for "ts" for a key on prewrite

2. prewrite new row: key -> (newValue, oldValue, ts); 

3. on commit:
   txState(ts) = COMMITTED
   release exclusive lock

4. on abort:
   txState(ts) = ABORTED
   release exclusive lock
   
5. clean up garbage asynchronously

The corresponding diagram:

```
Tx client               TxCoordinator                                               Partition leaseholder.    
tx.start
            --------->  
                        assign timestamp = ts
                        txstate = PENDING
            <---------		   	               
table.put(k,v)   
            --------->   
                        enlist(partition(k));
                        lh = getLeaseholder(partition(k))
                        send UpsertCommand(k,ts) to lh
				                                                      ------------>
                                                                                     replicate txstate = PENDING
                                                                                     lockManager.tryAcquire(k,ts);
                                                                                     wait for completion async
                                                                                     prewrite(k,v,oldV,ts) -- replicate to all replicas
repeat for each enlisted key...                        
            <---------
tx.finish - commit or rollback
            --------->  
                        send finish request to all remote enlisted nodes
                                                                      ------------>
                                                                                     replicate txstate = COMMITTED/ABORTED
                        txState = COMMITTED/ABORTED                                  lockManager.tryRelease(k,ts)
                                                                      <------------ 
                        		
                        when all leasholders are replied,
                        reply to initiator
            <--------
```

# Garbage cleaning

It makes sense to remove obsolete values and timestamps from record by running async "vacuum" procedure:

Key -> Tuple (newVal, oldVal, ts) will change after resetting to Key -> Tuple (newVal, null, null) and can be compacted.

# One phase commit

Implicit tx can be fast committed if all keys belongs to the same partition. TODO IGNITE-15927

# SQL and indexes.

We assume only row level locking in the first approach, which gives us a repeatable_read isolation.
When the SQL query is executed, it acquires locks while the the data is collected on data nodes.
Locks are acquired lazily as result set is consumed.
The locking rules are same as for get/put operations.
Then values are removed from indexes on step 2, they are written as tombstones to avoid read inconsistency and should be 
cleaned up after tx finish.

TODO IGNITE-15087: tx example flow with enabled index(es)

# Failover handling
Failover protocol is similar to Ignite 2 with a main difference: until tx is sure it can commit or rollback, it holds
its locks. This means in the case of split-brain, some keys will be locked until split-brain situation is resolved and
tx recovery protocol will converge. Consult a 2PC paper for details when it's possible, for example <sup id="a3">[3](#f3)</sup>

## Leaserholder fail
If a tx is not started to COMMIT, the coordinator reverts a transaction on remaining leaseholders.
Then a new leasholder is elected, it check for its pending transactions and asks a coordinator if it's possible to commit.

## Coordinator fail
Broadcast recovery (various strategies are possible: via gossip or dedicated node) is necessary (because we don't have 
full tx topology on each enlisted node - because it's unknown until commit). All nodes are requested about local txs state. 
If at least one is commiting, it's safe to commit.

**Note: a failover handling is still work in progress.**

<em id="f1">[1]</em> CockroachDB: The Resilient Geo-Distributed SQL Database, 2020 [↩](#a1)<br/>
<em id="f2">[2]</em> Concurrency Control in Distributed Database Systems, 1981 [↩](#a2)<br/>
<em id="f3">[3]</em> <a href="https://www.researchgate.net/publication/275155037_Two-Phase_Commit">Two-Phase Commit, 2009</a> [↩](#a3)
