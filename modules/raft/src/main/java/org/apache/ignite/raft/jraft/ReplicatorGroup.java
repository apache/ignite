/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft;

import java.util.List;
import org.apache.ignite.raft.jraft.closure.CatchUpClosure;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.core.ReplicatorType;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.ReplicatorGroupOptions;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesResponse;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosure;
import org.apache.ignite.raft.jraft.util.Describer;
import org.apache.ignite.raft.jraft.util.ThreadId;

/**
 * Replicators in a raft group.
 */
public interface ReplicatorGroup extends Describer {
    /**
     * Init the replicator group.
     *
     * @param nodeId node id
     * @param opts options of replicator grop
     * @return true if init success
     */
    boolean init(final NodeId nodeId, final ReplicatorGroupOptions opts);

    /**
     * Adds a replicator for follower({@link ReplicatorType#Follower}).
     *
     * @param peer target peer
     * @return true on success
     * @see #addReplicator(PeerId, ReplicatorType)
     */
    default boolean addReplicator(final PeerId peer) {
        return addReplicator(peer, ReplicatorType.Follower);
    }

    /**
     * Add a replicator attached with |peer| will be a notification when the replicator catches up according to the
     * arguments. NOTE: when calling this function, the replicators starts to work immediately, and might call
     * Node#stepDown which might have race with the caller, you should deal with this situation.
     *
     * @param peer target peer
     * @param replicatorType replicator type
     * @return true on success
     */
    default boolean addReplicator(final PeerId peer, ReplicatorType replicatorType) {
        return addReplicator(peer, replicatorType, true);
    }

    /**
     * Try to add a replicator attached with |peer| will be a notification when the replicator catches up according to
     * the arguments. NOTE: when calling this function, the replicators starts to work immediately, and might call
     * Node#stepDown which might have race with the caller, you should deal with this situation.
     *
     * @param peer target peer
     * @param replicatorType replicator type
     * @param sync synchronous
     * @return true on success
     */
    boolean addReplicator(final PeerId peer, ReplicatorType replicatorType, boolean sync);

    /**
     * Send heartbeat to a peer.
     *
     * @param peer target peer
     * @param closure callback
     */
    void sendHeartbeat(final PeerId peer, final RpcResponseClosure<AppendEntriesResponse> closure);

    /**
     * Get replicator id by peer, null if not found.
     *
     * @param peer peer of replicator
     * @return the replicator id
     */
    ThreadId getReplicator(final PeerId peer);

    /**
     * Check replicator state, if it's not started, start it; if it is blocked, unblock it. It should be called by
     * leader.
     *
     * @param peer peer of replicator
     * @param lockNode if lock with node
     */
    void checkReplicator(final PeerId peer, final boolean lockNode);

    /**
     * Clear failure to start replicators
     */
    void clearFailureReplicators();

    /**
     * Wait the peer catchup.
     */
    boolean waitCaughtUp(final PeerId peer, final long maxMargin, final long dueTime, final CatchUpClosure done);

    /**
     * Get peer's last rpc send timestamp (monotonic time in milliseconds).
     *
     * @param peer the peer of replicator
     */
    long getLastRpcSendTimestamp(final PeerId peer);

    /**
     * Stop all replicators.
     */
    boolean stopAll();

    /**
     * Stop replicator for the peer.
     *
     * @param peer the peer of replicator
     * @return true on success
     */
    boolean stopReplicator(final PeerId peer);

    /**
     * Reset the term of all to-add replicators. This method is supposed to be called when the very candidate becomes
     * the leader, so we suppose that there are no running replicators. Return true on success, false otherwise
     *
     * @param newTerm new term num
     * @return true on success
     */
    boolean resetTerm(final long newTerm);

    /**
     * Reset the interval of heartbeat, This method is supposed to be called when the very candidate becomes the leader,
     * so we suppose that there are no running replicators. return true when success, false otherwise.
     *
     * @param newIntervalMs new heartbeat interval millis
     * @return true on success
     */
    boolean resetHeartbeatInterval(final int newIntervalMs);

    /**
     * Reset the interval of electionTimeout for replicator.
     *
     * @param newIntervalMs new election timeout millis
     * @return true on success
     */
    boolean resetElectionTimeoutInterval(final int newIntervalMs);

    /**
     * Returns true if the there's a replicator attached to the given |peer|
     *
     * @param peer target peer
     * @return true on contains
     */
    boolean contains(final PeerId peer);

    /**
     * Transfer leadership to the given |peer|
     *
     * @param peer target peer
     * @param logIndex log index
     * @return true on success
     */
    boolean transferLeadershipTo(final PeerId peer, final long logIndex);

    /**
     * Stop transferring leadership to the given |peer|
     *
     * @param peer target peer
     * @return true on success
     */
    boolean stopTransferLeadership(final PeerId peer);

    /**
     * Stop all the replicators except for the one that we think can be the candidate of the next leader, which has the
     * largest `last_log_id' among peers in |current_conf|. |candidate| would be returned if we found one and the caller
     * is responsible for stopping it, or an invalid value if we found none. Returns candidate replicator id on success
     * and null otherwise.
     *
     * @param conf configuration of all replicators
     * @return candidate replicator id on success
     */
    ThreadId stopAllAndFindTheNextCandidate(final ConfigurationEntry conf);

    /**
     * Find the follower with the most log entries in this group, which is likely becomes the leader according to the
     * election algorithm of raft. Returns the follower peerId on success and null otherwise.
     *
     * @param conf configuration of all replicators
     * @return the follower peerId on success
     */
    PeerId findTheNextCandidate(final ConfigurationEntry conf);

    /**
     * Returns all replicators.
     */
    List<ThreadId> listReplicators();
}
