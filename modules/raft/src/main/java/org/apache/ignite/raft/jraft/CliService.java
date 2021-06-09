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
import java.util.Map;
import java.util.Set;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.CliOptions;

/**
 * Client command-line service
 */
public interface CliService extends Lifecycle<CliOptions> {
    /**
     * Add a new peer into the replicating group which consists of |conf|. return OK status when success.
     *
     * @param groupId the raft group id
     * @param conf current configuration
     * @param peer peer to add
     * @return operation status
     */
    Status addPeer(final String groupId, final Configuration conf, final PeerId peer);

    /**
     * Remove a peer from the replicating group which consists of |conf|. return OK status when success.
     *
     * @param groupId the raft group id
     * @param conf current configuration
     * @param peer peer to remove
     * @return operation status
     */
    Status removePeer(final String groupId, final Configuration conf, final PeerId peer);

    /**
     * Gracefully change the peers of the replication group.
     *
     * @param groupId the raft group id
     * @param conf current configuration
     * @param newPeers new peers to change
     * @return operation status
     */
    Status changePeers(final String groupId, final Configuration conf, final Configuration newPeers);

    /**
     * Reset the peer set of the target peer.
     *
     * @param groupId the raft group id
     * @param peer target peer
     * @param newPeers new peers to reset
     * @return operation status
     */
    Status resetPeer(final String groupId, final PeerId peer, final Configuration newPeers);

    /**
     * Add some new learners into the replicating group which consists of |conf|. return OK status when success.
     *
     * @param groupId the raft group id
     * @param conf current configuration
     * @param learners learner peers to add
     * @return operation status
     */
    Status addLearners(final String groupId, final Configuration conf, final List<PeerId> learners);

    /**
     * Remove some learners from the replicating group which consists of |conf|. return OK status when success.
     *
     * @param groupId the raft group id
     * @param conf current configuration
     * @param learners learner peers to remove
     * @return operation status
     */
    Status removeLearners(final String groupId, final Configuration conf, final List<PeerId> learners);

    /**
     * Update learners set in the replicating group which consists of |conf|. return OK status when success.
     *
     * @param groupId the raft group id
     * @param conf current configuration
     * @param learners learner peers to set
     * @return operation status
     */
    Status resetLearners(final String groupId, final Configuration conf, final List<PeerId> learners);

    /**
     * Transfer the leader of the replication group to the target peer
     *
     * @param groupId the raft group id
     * @param conf current configuration
     * @param peer target peer of new leader
     * @return operation status
     */
    Status transferLeader(final String groupId, final Configuration conf, final PeerId peer);

    /**
     * Ask the peer to dump a snapshot immediately.
     *
     * @param groupId the raft group id
     * @param peer target peer
     * @return operation status
     */
    Status snapshot(final String groupId, final PeerId peer);

    /**
     * Get the leader of the replication group.
     *
     * @param groupId the raft group id
     * @param conf configuration
     * @param leaderId id of leader
     * @return operation status
     */
    Status getLeader(final String groupId, final Configuration conf, final PeerId leaderId);

    /**
     * Ask all peers of the replication group.
     *
     * @param groupId the raft group id
     * @param conf target peers configuration
     * @return all peers of the replication group
     */
    List<PeerId> getPeers(final String groupId, final Configuration conf);

    /**
     * Ask all alive peers of the replication group.
     *
     * @param groupId the raft group id
     * @param conf target peers configuration
     * @return all alive peers of the replication group
     */
    List<PeerId> getAlivePeers(final String groupId, final Configuration conf);

    /**
     * Ask all learners of the replication group.
     *
     * @param groupId the raft group id
     * @param conf target peers configuration
     * @return all learners of the replication group
     */
    List<PeerId> getLearners(final String groupId, final Configuration conf);

    /**
     * Ask all alive learners of the replication group.
     *
     * @param groupId the raft group id
     * @param conf target peers configuration
     * @return all alive learners of the replication group
     */
    List<PeerId> getAliveLearners(final String groupId, final Configuration conf);

    /**
     * Balance the number of leaders.
     *
     * @param balanceGroupIds all raft group ids to balance
     * @param conf configuration of all nodes
     * @param balancedLeaderIds the result of all balanced leader ids
     * @return operation status
     */
    Status rebalance(final Set<String> balanceGroupIds, final Configuration conf,
        final Map<String, PeerId> balancedLeaderIds);
}
