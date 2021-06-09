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

import org.apache.ignite.raft.jraft.core.BallotBox;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.Scheduler;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;

/**
 * Replicator group options.
 */
public class ReplicatorGroupOptions {

    private int heartbeatTimeoutMs;
    private int electionTimeoutMs;
    private LogManager logManager;
    private BallotBox ballotBox;
    private NodeImpl node;
    private SnapshotStorage snapshotStorage;
    private RaftClientService raftRpcClientService;
    private RaftOptions raftOptions;
    private Scheduler timerManager;

    public Scheduler getTimerManager() {
        return this.timerManager;
    }

    public void setTimerManager(Scheduler timerManager) {
        this.timerManager = timerManager;
    }

    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    public void setRaftOptions(RaftOptions raftOptions) {
        this.raftOptions = raftOptions;
    }

    public RaftClientService getRaftRpcClientService() {
        return this.raftRpcClientService;
    }

    public void setRaftRpcClientService(RaftClientService raftRpcService) {
        this.raftRpcClientService = raftRpcService;
    }

    public int getHeartbeatTimeoutMs() {
        return this.heartbeatTimeoutMs;
    }

    public void setHeartbeatTimeoutMs(int heartbeatTimeoutMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public int getElectionTimeoutMs() {
        return this.electionTimeoutMs;
    }

    public void setElectionTimeoutMs(int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
    }

    public LogManager getLogManager() {
        return this.logManager;
    }

    public void setLogManager(LogManager logManager) {
        this.logManager = logManager;
    }

    public BallotBox getBallotBox() {
        return this.ballotBox;
    }

    public void setBallotBox(BallotBox ballotBox) {
        this.ballotBox = ballotBox;
    }

    public NodeImpl getNode() {
        return this.node;
    }

    public void setNode(NodeImpl node) {
        this.node = node;
    }

    public SnapshotStorage getSnapshotStorage() {
        return this.snapshotStorage;
    }

    public void setSnapshotStorage(SnapshotStorage snapshotStorage) {
        this.snapshotStorage = snapshotStorage;
    }

    @Override
    public String toString() {
        return "ReplicatorGroupOptions{" + "heartbeatTimeoutMs=" + heartbeatTimeoutMs + ", electionTimeoutMs="
            + electionTimeoutMs + ", logManager=" + logManager + ", ballotBox=" + ballotBox + ", node=" + node
            + ", snapshotStorage=" + snapshotStorage + ", raftRpcClientService=" + raftRpcClientService
            + ", raftOptions=" + raftOptions + ", timerManager=" + timerManager + '}';
    }
}
