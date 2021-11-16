## Introduction
ChangePeers is not a separate jraft log command, but an algorithm with some separate phases
- Start replicators for new nodes on the leader and waiting for nodes' catchup
- Push configuration LogEntry to raft quorum (old quorum, new quorum) and apply ConfigurationEntry(conf=\<new peers configuration>, oldConf=\<previous peers configuration>) to leader. 
- When previous configuration committed by quorum, push configuration LogEntry to raft quorum (only new quorum) and apply configuration ConfigurationEntry(conf=\<new peers configuration>, oldConf=null) to leader. Legacy replicators will be stopped and if current leader is not in new topology - it will be stepped down.

## Catchup phase (STAGE_CATCHING_UP)
On changePeers request leader start all needed replicators for new peers. NodeImpl#confCtx stage set to STAGE_CATCHING_UP. We will use these stages as logical step names for further process explanation.

The end of catchup phase - is the moment, when all new peers caught up the leader. It means that a difference between leader.last_log_index and peer.last_log_index is smaller than NodeOptions#catchupMargin (1000 by default).

So, when catchup finished - NodeImpl#confCtx stage will be moved to STAGE_JOINT and first real configuration changes started.

## Apply composite configuration with old and new peers (STAGE_JOINT)
First LogEntry(type=ENTRY_TYPE_CONFIGURATION, peers=newConf.peers, oldPeers=oldConf.peers, ...) pushed to composite raft quorum (oldQuorum, newQuorum). Created the listener ConfigurationChangeDone, which will be invoked when both quorum accept entry.

Also, put it to logManager of leader and apply this configuration to leader immediately. 

ConfigurationChangeDone is invoked (so, both quorums accept config entry) and NodeImpl#confCtx stage move to STAGE_STABLE.

## Apply configuration with only new peers (STAGE_STABLE)
The last chord of this song.

Put LogEntry(type=ENTRY_TYPE_CONFIGURATION, peers=newConf.peers, oldPeers=null, ...) to the new quorum.

Change configuration of the leader to the new peers only.

When new quorum accept new config - stop legacy replicators and step down the leader if it was removed from the new configuration.

After that - changePeers finished and client receive response.

## Questions
>Is it possible to change peers for the case when the old and new sets of raft nodes do not intersect?

Yes, according to algorithm it is not an issue.

>When changePeers() returns to the client?

In the end of the whole process (including data migration) and it looks like it is not a problem of algorithm at all, but the problem of ChangePeersRequestProcessor from cli package. The group will be fully workin during the longest phase of changePeers - data migration. So, maybe we need just async version of ChangePeerRequestProcessor.

>Let’s check whether dataRebalance is a raft command that works just as any other raft commands and do not expect index gaps.

As mentioned earlier - dataRebalance is a background process. But configuration changes processed through usual raft LogEntry flow.

>Let’s check that snapshot works the same way as log-rebalance in the context of index moving.

Snapshots can be used on catchup phase along with entries - is not an issue, I think. Every snapshot has last entry index, it will affect only the internal mechanism of data moving during catchup.
