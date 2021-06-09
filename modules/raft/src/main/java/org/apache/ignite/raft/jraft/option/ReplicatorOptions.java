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
package org.apache.ignite.raft.jraft.option;

import java.util.concurrent.ExecutorService;
import org.apache.ignite.raft.jraft.core.BallotBox;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.ReplicatorType;
import org.apache.ignite.raft.jraft.core.Scheduler;
import org.apache.ignite.raft.jraft.core.TimerManager;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.util.Copiable;

/**
 * Replicator options.
 */
public class ReplicatorOptions implements Copiable<ReplicatorOptions> {

    private int dynamicHeartBeatTimeoutMs;
    private int electionTimeoutMs;
    private String groupId;
    private PeerId serverId;
    private PeerId peerId;
    private LogManager logManager;
    private BallotBox ballotBox;
    private NodeImpl node;
    private long term;
    private SnapshotStorage snapshotStorage;
    private RaftClientService raftRpcService;
    private Scheduler timerManager;
    private ReplicatorType replicatorType;

    public ReplicatorOptions() {
        super();
    }

    public ReplicatorOptions(final ReplicatorType replicatorType, final int dynamicHeartBeatTimeoutMs,
        final int electionTimeoutMs, final String groupId, final PeerId serverId,
        final PeerId peerId, final LogManager logManager, final BallotBox ballotBox,
        final NodeImpl node, final long term, final SnapshotStorage snapshotStorage,
        final RaftClientService raftRpcService, final TimerManager timerManager) {
        super();
        this.replicatorType = replicatorType;
        this.dynamicHeartBeatTimeoutMs = dynamicHeartBeatTimeoutMs;
        this.electionTimeoutMs = electionTimeoutMs;
        this.groupId = groupId;
        this.serverId = serverId;
        if (peerId != null) {
            this.peerId = peerId.copy();
        }
        else {
            this.peerId = null;
        }
        this.logManager = logManager;
        this.ballotBox = ballotBox;
        this.node = node;
        this.term = term;
        this.snapshotStorage = snapshotStorage;
        this.raftRpcService = raftRpcService;
        this.timerManager = timerManager;
    }

    public final ReplicatorType getReplicatorType() {
        return this.replicatorType;
    }

    public void setReplicatorType(final ReplicatorType replicatorType) {
        this.replicatorType = replicatorType;
    }

    public RaftClientService getRaftRpcService() {
        return this.raftRpcService;
    }

    public void setRaftRpcService(final RaftClientService raftRpcService) {
        this.raftRpcService = raftRpcService;
    }

    @Override
    public ReplicatorOptions copy() {
        final ReplicatorOptions replicatorOptions = new ReplicatorOptions();
        replicatorOptions.setDynamicHeartBeatTimeoutMs(this.dynamicHeartBeatTimeoutMs);
        replicatorOptions.setReplicatorType(this.replicatorType);
        replicatorOptions.setElectionTimeoutMs(this.electionTimeoutMs);
        replicatorOptions.setGroupId(this.groupId);
        replicatorOptions.setServerId(this.serverId);
        replicatorOptions.setPeerId(this.peerId);
        replicatorOptions.setLogManager(this.logManager);
        replicatorOptions.setBallotBox(this.ballotBox);
        replicatorOptions.setNode(this.node);
        replicatorOptions.setTerm(this.term);
        replicatorOptions.setSnapshotStorage(this.snapshotStorage);
        replicatorOptions.setRaftRpcService(this.raftRpcService);
        replicatorOptions.setTimerManager(this.timerManager);
        return replicatorOptions;
    }

    public Scheduler getTimerManager() {
        return this.timerManager;
    }

    public void setTimerManager(final Scheduler timerManager) {
        this.timerManager = timerManager;
    }

    public PeerId getPeerId() {
        return this.peerId;
    }

    public void setPeerId(final PeerId peerId) {
        if (peerId != null) {
            this.peerId = peerId.copy();
        }
        else {
            this.peerId = null;
        }
    }

    public int getDynamicHeartBeatTimeoutMs() {
        return this.dynamicHeartBeatTimeoutMs;
    }

    public void setDynamicHeartBeatTimeoutMs(final int dynamicHeartBeatTimeoutMs) {
        this.dynamicHeartBeatTimeoutMs = dynamicHeartBeatTimeoutMs;
    }

    public int getElectionTimeoutMs() {
        return this.electionTimeoutMs;
    }

    public void setElectionTimeoutMs(final int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
    }

    public String getGroupId() {
        return this.groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    public PeerId getServerId() {
        return this.serverId;
    }

    public void setServerId(final PeerId serverId) {
        this.serverId = serverId;
    }

    public LogManager getLogManager() {
        return this.logManager;
    }

    public void setLogManager(final LogManager logManager) {
        this.logManager = logManager;
    }

    public BallotBox getBallotBox() {
        return this.ballotBox;
    }

    public void setBallotBox(final BallotBox ballotBox) {
        this.ballotBox = ballotBox;
    }

    public NodeImpl getNode() {
        return this.node;
    }

    public void setNode(final NodeImpl node) {
        this.node = node;
    }

    public long getTerm() {
        return this.term;
    }

    public void setTerm(final long term) {
        this.term = term;
    }

    public SnapshotStorage getSnapshotStorage() {
        return this.snapshotStorage;
    }

    public void setSnapshotStorage(final SnapshotStorage snapshotStorage) {
        this.snapshotStorage = snapshotStorage;
    }

    @Override
    public String toString() {
        return "ReplicatorOptions{" + "replicatorType=" + this.replicatorType + "dynamicHeartBeatTimeoutMs="
            + this.dynamicHeartBeatTimeoutMs + ", electionTimeoutMs=" + this.electionTimeoutMs + ", groupId='"
            + this.groupId + '\'' + ", serverId=" + this.serverId + ", peerId=" + this.peerId + ", logManager="
            + this.logManager + ", ballotBox=" + this.ballotBox + ", node=" + this.node + ", term=" + this.term
            + ", snapshotStorage=" + this.snapshotStorage + ", raftRpcService=" + this.raftRpcService
            + ", timerManager=" + this.timerManager + '}';
    }

    /**
     * @return Common executor.
     */
    public ExecutorService getCommonExecutor() {
        return getNode().getOptions().getCommonExecutor();
    }
}
