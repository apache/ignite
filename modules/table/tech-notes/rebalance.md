# How to read this doc
Every algorithm phase has the following main sections:
- Trigger - how current phase will be invoked
- Steps/Pseudocode - the main logical steps of the current phase
- Result (optional, if pseudocode provided) - events and system state changes, which this phase produces

# Rebalance algorithm
## Short algorithm description
  - Operations, which can trigger rebalance occurred:
    
    Write new baseline to metastore (effectively from 1 node in cluster)

    OR
    
    Write new replicas configuration number to table config (effectively from 1 node)
    
    OR
    
    Write new partitions configuration number to table config (effectively from 1 node)
- Write new assignments' intention to metastore (effectively from 1 node in cluster)
- Start new raft nodes. Initiate/update change peer request to raft group (effectively from 1 node per partition)
- Stop all redundant nodes. Change stable partition assignment to the new one and finish rebalance process.

## New metastore keys
For further steps, we should introduce some new metastore keys:
- `partition.assignments.stable` - the list of peers, which process operations for partition at the current moment.
- `partition.assignments.pending` - the list of peers, where current rebalance move the partition.
- `partition.assignments.planned` - the list of peers, which will be used for new rebalance, when current will be finished.

Also, we will need the utility key:
- `partition.assignments.change.trigger.revision` - the key, needed for processing the event about assignments' update trigger only once.

## Operations, which can trigger rebalance
Three types of events can trigger the rebalance:
- API call of any special method like `org.apache.ignite.Ignite.setBaseline`, which will change baseline value in metastore (1 for all tables for now, but maybe it should be separate per table in future)
- Configuration change through `org.apache.ignite.configuration.schemas.table.TableChange.changeReplicas` produce metastore update event
- Configuration change through `org.apache.ignite.configuration.schemas.table.TableChange.changePartitions` produce metastore update event (IMPORTANT: this type of trigger has additional difficulties because of cross raft group data migration and it is out of scope of this document)

**Result**: So, one of three metastore keys' changes will trigger rebalance:
```
<global>.baseline
<tableScope>.replicas
<tableScope>.partitions // out of scope
```
## Write new pending assignments (1)
**Trigger**:
- Metastore event about change in `<global>.baseline`
- Metastore event about changes in `<tableScope>.replicas`

**Pseudocode**:
```
onBaselineEvent:
    for table in tableCfg.tables():
        for partition in table.partitions:
            <inline metastoreInvoke>
            
onReplicaNumberChange:
    with table as event.table:
        for partitoin in table.partitions:
            <inline metastoreInvoke>

metastoreInvoke: // atomic metastore call through multi-invoke api
    if empty(partition.assignments.change.trigger.revision) || partition.assignments.change.trigger.revision < event.revision:
        if empty(partition.assignments.pending) && partition.assignments.stable != calcPartAssighments():
            partition.assignments.pending = calcPartAssignments() 
            partition.assignments.change.trigger.revision = event.revision
        else:
            if partition.assignments.pending != calcPartAssignments
                partition.assignments.planned = calcPartAssignments()
                partition.assignments.change.trigger.revision = event.revision
            else
                remove(partition.assignments.planned)
    else:
        skip
```

## Start new raft nodes and initiate change peers (2)
**Trigger**: Metastore event about new `partition.assignments.pending` received

**Steps**:
- Start all new needed nodes `partition.assignments.pending / partition.assignments.stable` 
- After successful starts - check if current node is the leader of raft group (leader response must be updated by current term) and `changePeers(leaderTerm, peers)`. `changePeers` from old terms must be skipped.

**Result**:
- New needed raft nodes started
- Change peers state initiated for every raft group

## When changePeers done inside the raft group - stop all redundant nodes
**Trigger**: When leader applied new Configuration with list of resulting peers `<applied peer>`, it calls `onChangePeersCommitted(<applied peers>)`

**Pseudocode**:
```
metastoreInvoke: \\ atomic
    partition.assignments.stable = appliedPeers
    if empty(partition.assignments.planned):
        partition.assignments.pending = empty
    else:
        partition.assignments.pending = partition.assignments.planned
```

Failover helpers (detailed failover scenarious must be developed in future)
- `onLeaderElected()` - must be executed from the new leader when raft group elected the new leader. Maybe we actually need to also check if a new lease is received.
- `onChangePeersError()` - must be executed when any errors during changePeers occurred.
- `onChangePeersCommitted(peers)` - must be executed with the list of new peers when changePeers has successfully done.

At the moment, this set of listeners seems to enough for restart rebalance and/or notify node's failover mechanism about fatal issues. Failover scenarios will be explained with more details during first phase of implementation.

## Cleanup redundant raft nodes (3)
**Trigger**: Node receive update about partition stable assignments

**Steps**:
- Replace current raft client with new one, with appropriate peers
- Stop unneeded raft nodes (be careful - node can be not a part of stable topology, but needed for current pending - so, we need to stop current raft node, only if it is not a part of `partition.assignments.pending + partition.assignments.stable` set)

**Result**:
- Raft clients for new assignments refreshed
- Redundant raft nodes stopped

# Failover
We need to provide Failover thread, which can handle the following cases:
- `changePeers` can't start even catchup process, because of any new raft nodes wasn't started yet for instance.
- `changePeers` failed to complete catchup due to catchup timeout, for example.

We have the following mechanisms for handling these cases:
- `onChangerPeersError(errorContext)`, which must schedule retries, if needed
- Separate special thread must process all needed retries on the current node
- If current node is not the leader of partition raft group anymore - it will request `changePeers` with legacy term, receive appropriate answer from the leader and stop retries for this partition.
- If leader changed, new node will receive `onLeaderElected()` invoke and start needed changePeers from the pending key. Also, it will create new failover thread for retry logic.
- If failover exhaust maximum number for query retries - it must notify about the issue global node failover (details must be specified later)


# Metastore rebalance
It seems, that rebalance of metastore can be handled the same process, because:
- during the any stage of `changePeers` raft group can handle any another entries
- any rebalance failures must not end up by raft group unavailability (if majority is kept)

Also, failover mechanism above doesn't use metastore, but raft term and special listeners.

# Further optimisations:
## Adjustable `changePeers`
Algorithm above seems working well, but it has one serious caveat. When the leader is busy by current `changePeers`, we can't start new one.
That's a big issue - because data rebalance process can be long enough, while all nodes sync raft logs with data. According to 
https://github.com/apache/ignite-3/blob/main/modules/raft/tech-notes/changePeers.md - we can relatively painless update the peers' list, if leader is in the STAGE_CATCHING_UP phase still. Alternatively, we can cancel current `changePeers`, if it is in the STAGE_CATCHING_UP and run new one

### Approach 1. Update the peers' list of current `changePeers`
This approach can be addressed with different implemetation details, but let's describe the simplest one.

- Introduce the new metastore key `partition.assignments.pending.lock`
- Update the (1) step with the following:

**Pseudocode**
```
metastoreInvoke: // atomic metastore call through multi-invoke api
    if empty(partition.assignments.change.trigger.revision) || partition.assignments.change.trigger.revision < event.revision:
        if empty(partition.assignments.pending):
            if partition.assignments.stable != calcPartAssignments():
                partition.assignments.pending = calcPartAssignments() 
                partition.assignments.change.trigger.revision = event.revision
            else:
                skip
        else:
            if empty(partition.assignments.pending.lock):
                partition.assignments.pending = calcPartAssignments() 
                partition.assignments.change.trigger.revision = event.revision
            else:
                partition.assignments.planned = calcPartAssignments() 
                partition.assignments.change.trigger.revision = event.revision
    else:
        skip
```
- Update `changePeers` request-response behaviour with:
    - response with `received` if no current `changePeers` or current `changePeers` in the `STAGE_CATCHING_UP`. Updates the catching up peers with the new peers, stop redundant replicators, if needed.
    - response with `busy` if current leader is not in the `STAGE_NONE` or `STAGE_CATCHING_UP` phase.
- Update `changePeers` behaviour with new listener from the caller `tryCatchUpFinish(peers)`. This listener must execute the following metastore call:

**Pseudocode:**
```
metastoreInvoke: // atomic metastore call through multi-invoke api
    if empty(partition.assignments.pending.lock):
        if peers == partition.assignments.pending:
            partition.assignments.pending.lock = true
            return true
        else:
            return false
```
If the listener return false - we should to await the new peer list, process it and try to call it again.
- We need to empty the lock, after rebalance fails or done, in appropriate listeners.
    

### Approach 2. Cancel current `changePeers` and run new one 
Instead of updating current `changePeers` with new peers' list - we can cancel it and start the new one.

For this dish we will need:
- New raft service's method `cancelChangePeers()`. This method should cancel current `changePeers` if and only if it is in the STAGE_CATCHING_UP phase. Method must return:
  - true: if no changePeers to cancel or successful cancel occurred.
  - false: if `changePeers` in progress and can't be cancelled (like in approach 1 - if the leader is not in STAGE_CATCHING_UP/STAGE_NONE)
- Listen the `partition.assignments.planned` key and on update:
  - Execute `cancelChangePeers()` on the node with the partition leader. If it returns `false` - do nothing.
  - If it returns `true` - move planned peers to pending in metastore
