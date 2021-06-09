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

import org.apache.ignite.raft.jraft.util.Copiable;

/**
 * Raft options.
 */
public class RaftOptions implements Copiable<RaftOptions> {
    /**
     * Maximum of block size per RPC
     */
    private int maxByteCountPerRpc = 128 * 1024;

    /**
     * File service check hole switch, default disable
     */
    private boolean fileCheckHole = false;

    /**
     * The maximum number of entries in AppendEntriesRequest
     */
    private int maxEntriesSize = 1024;

    /**
     * The maximum byte size of AppendEntriesRequest
     */
    private int maxBodySize = 512 * 1024;

    /**
     * Flush buffer to LogStorage if the buffer size reaches the limit
     */
    private int maxAppendBufferSize = 256 * 1024;

    /**
     * Maximum election delay time allowed by user
     */
    private int maxElectionDelayMs = 1000;

    /**
     * Raft election:heartbeat timeout factor
     */
    private int electionHeartbeatFactor = 10;

    /**
     * Maximum number of tasks that can be applied in a batch
     */
    private int applyBatch = 32;

    /**
     * Call fsync when need
     */
    private boolean sync = true;

    /**
     * Sync log meta, snapshot meta and raft meta
     */
    private boolean syncMeta = false;

    /**
     * Statistics to analyze the performance of db
     */
    private boolean openStatistics = true;

    /**
     * Whether to enable replicator pipeline.
     */
    private boolean replicatorPipeline = true;

    /**
     * The maximum replicator pipeline in-flight requests/responses, only valid when enable replicator pipeline.
     */
    private int maxReplicatorInflightMsgs = 256;
    /**
     * Internal disruptor buffers size for Node/FSMCaller/LogManager etc.
     */
    private int disruptorBufferSize = 16384;

    /**
     * The maximum timeout in seconds to wait when publishing events into disruptor, default is 10 seconds. If the
     * timeout happens, it may halt the node.
     */
    private int disruptorPublishEventWaitTimeoutSecs = 10;

    /**
     * When true, validate log entry checksum when transferring the log entry from disk or network, default is false. If
     * true, it would hurt the performance of JRAft but gain the data safety.
     *
     */
    private boolean enableLogEntryChecksum = false; // TODO asch https://issues.apache.org/jira/browse/IGNITE-14833.

    /**
     * ReadOnlyOption specifies how the read only request is processed. * {@link ReadOnlyOption#ReadOnlySafe} guarantees
     * the linearizability of the read only request by communicating with the quorum. It is the default and suggested
     * option. * {@link ReadOnlyOption#ReadOnlyLeaseBased} ensures linearizability of the read only request by relying
     * on the leader lease. It can be affected by clock drift. If the clock drift is unbounded, leader might keep the
     * lease longer than it should (clock can move backward/pause without any bound). ReadIndex is not safe in that
     * case.
     */
    private ReadOnlyOption readOnlyOptions = ReadOnlyOption.ReadOnlySafe;

    /**
     * Candidate steps down when election reaching timeout, default is true(enabled).
     */
    private boolean stepDownWhenVoteTimedout = true;

    public boolean isStepDownWhenVoteTimedout() {
        return this.stepDownWhenVoteTimedout;
    }

    public void setStepDownWhenVoteTimedout(final boolean stepDownWhenVoteTimeout) {
        this.stepDownWhenVoteTimedout = stepDownWhenVoteTimeout;
    }

    public int getDisruptorPublishEventWaitTimeoutSecs() {
        return this.disruptorPublishEventWaitTimeoutSecs;
    }

    public void setDisruptorPublishEventWaitTimeoutSecs(final int disruptorPublishEventWaitTimeoutSecs) {
        this.disruptorPublishEventWaitTimeoutSecs = disruptorPublishEventWaitTimeoutSecs;
    }

    public boolean isEnableLogEntryChecksum() {
        return this.enableLogEntryChecksum;
    }

    public void setEnableLogEntryChecksum(final boolean enableLogEntryChecksumValidation) {
        this.enableLogEntryChecksum = enableLogEntryChecksumValidation;
    }

    public ReadOnlyOption getReadOnlyOptions() {
        return this.readOnlyOptions;
    }

    public void setReadOnlyOptions(final ReadOnlyOption readOnlyOptions) {
        this.readOnlyOptions = readOnlyOptions;
    }

    public boolean isReplicatorPipeline() {
        return this.replicatorPipeline;
    }

    public void setReplicatorPipeline(final boolean replicatorPipeline) {
        this.replicatorPipeline = replicatorPipeline;
    }

    public int getMaxReplicatorInflightMsgs() {
        return this.maxReplicatorInflightMsgs;
    }

    public void setMaxReplicatorInflightMsgs(final int maxReplicatorPiplelinePendingResponses) {
        this.maxReplicatorInflightMsgs = maxReplicatorPiplelinePendingResponses;
    }

    public int getDisruptorBufferSize() {
        return this.disruptorBufferSize;
    }

    public void setDisruptorBufferSize(final int disruptorBufferSize) {
        this.disruptorBufferSize = disruptorBufferSize;
    }

    public int getMaxByteCountPerRpc() {
        return this.maxByteCountPerRpc;
    }

    public void setMaxByteCountPerRpc(final int maxByteCountPerRpc) {
        this.maxByteCountPerRpc = maxByteCountPerRpc;
    }

    public boolean isFileCheckHole() {
        return this.fileCheckHole;
    } // TODO asch review properties https://issues.apache.org/jira/browse/IGNITE-14832

    public void setFileCheckHole(final boolean fileCheckHole) {
        this.fileCheckHole = fileCheckHole;
    }

    public int getMaxEntriesSize() {
        return this.maxEntriesSize;
    }

    public void setMaxEntriesSize(final int maxEntriesSize) {
        this.maxEntriesSize = maxEntriesSize;
    }

    public int getMaxBodySize() {
        return this.maxBodySize;
    }

    public void setMaxBodySize(final int maxBodySize) {
        this.maxBodySize = maxBodySize;
    }

    public int getMaxAppendBufferSize() {
        return this.maxAppendBufferSize;
    }

    public void setMaxAppendBufferSize(final int maxAppendBufferSize) {
        this.maxAppendBufferSize = maxAppendBufferSize;
    }

    public int getMaxElectionDelayMs() {
        return this.maxElectionDelayMs;
    }

    public void setMaxElectionDelayMs(final int maxElectionDelayMs) {
        this.maxElectionDelayMs = maxElectionDelayMs;
    }

    public int getElectionHeartbeatFactor() {
        return this.electionHeartbeatFactor;
    }

    public void setElectionHeartbeatFactor(final int electionHeartbeatFactor) {
        this.electionHeartbeatFactor = electionHeartbeatFactor;
    }

    public int getApplyBatch() {
        return this.applyBatch;
    }

    public void setApplyBatch(final int applyBatch) {
        this.applyBatch = applyBatch;
    }

    public boolean isSync() {
        return this.sync;
    }

    public void setSync(final boolean sync) {
        this.sync = sync;
    }

    public boolean isSyncMeta() {
        return this.sync || this.syncMeta;
    }

    public void setSyncMeta(final boolean syncMeta) {
        this.syncMeta = syncMeta;
    }

    public boolean isOpenStatistics() {
        return this.openStatistics;
    }

    public void setOpenStatistics(final boolean openStatistics) {
        this.openStatistics = openStatistics;
    }

    @Override
    public RaftOptions copy() {
        final RaftOptions raftOptions = new RaftOptions();
        raftOptions.setMaxByteCountPerRpc(this.maxByteCountPerRpc);
        raftOptions.setFileCheckHole(this.fileCheckHole);
        raftOptions.setMaxEntriesSize(this.maxEntriesSize);
        raftOptions.setMaxBodySize(this.maxBodySize);
        raftOptions.setMaxAppendBufferSize(this.maxAppendBufferSize);
        raftOptions.setMaxElectionDelayMs(this.maxElectionDelayMs);
        raftOptions.setElectionHeartbeatFactor(this.electionHeartbeatFactor);
        raftOptions.setApplyBatch(this.applyBatch);
        raftOptions.setSync(this.sync);
        raftOptions.setSyncMeta(this.syncMeta);
        raftOptions.setOpenStatistics(this.openStatistics);
        raftOptions.setReplicatorPipeline(this.replicatorPipeline);
        raftOptions.setMaxReplicatorInflightMsgs(this.maxReplicatorInflightMsgs);
        raftOptions.setDisruptorBufferSize(this.disruptorBufferSize);
        raftOptions.setDisruptorPublishEventWaitTimeoutSecs(this.disruptorPublishEventWaitTimeoutSecs);
        raftOptions.setEnableLogEntryChecksum(this.enableLogEntryChecksum);
        raftOptions.setReadOnlyOptions(this.readOnlyOptions);
        return raftOptions;
    }

    @Override
    public String toString() {
        return "RaftOptions{" + "maxByteCountPerRpc=" + this.maxByteCountPerRpc + ", fileCheckHole="
            + this.fileCheckHole + ", maxEntriesSize=" + this.maxEntriesSize + ", maxBodySize=" + this.maxBodySize
            + ", maxAppendBufferSize=" + this.maxAppendBufferSize + ", maxElectionDelayMs="
            + this.maxElectionDelayMs + ", electionHeartbeatFactor=" + this.electionHeartbeatFactor
            + ", applyBatch=" + this.applyBatch + ", sync=" + this.sync + ", syncMeta=" + this.syncMeta
            + ", openStatistics=" + this.openStatistics + ", replicatorPipeline=" + this.replicatorPipeline
            + ", maxReplicatorInflightMsgs=" + this.maxReplicatorInflightMsgs + ", disruptorBufferSize="
            + this.disruptorBufferSize + ", disruptorPublishEventWaitTimeoutSecs="
            + this.disruptorPublishEventWaitTimeoutSecs + ", enableLogEntryChecksum=" + this.enableLogEntryChecksum
            + ", readOnlyOptions=" + this.readOnlyOptions + '}';
    }
}
