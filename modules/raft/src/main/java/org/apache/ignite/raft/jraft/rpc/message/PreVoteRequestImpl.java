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
package org.apache.ignite.raft.jraft.rpc.message;

import org.apache.ignite.raft.jraft.rpc.RpcRequests;

class PreVoteRequestImpl implements RpcRequests.RequestVoteRequest, RpcRequests.RequestVoteRequest.Builder {
    private String groupId;
    private String serverId;
    private String peerId;
    private long term;
    private long lastLogTerm;
    private long lastLogIndex;
    private boolean preVote;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public String getServerId() {
        return serverId;
    }

    @Override public Builder setServerId(String serverId) {
        this.serverId = serverId;

        return this;
    }

    @Override public String getPeerId() {
        return peerId;
    }

    @Override public Builder setPeerId(String peerId) {
        this.peerId = peerId;

        return this;
    }

    @Override public long getTerm() {
        return term;
    }

    @Override public Builder setTerm(long term) {
        this.term = term;

        return this;
    }

    @Override public long getLastLogTerm() {
        return lastLogTerm;
    }

    @Override public Builder setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;

        return this;
    }

    @Override public long getLastLogIndex() {
        return lastLogIndex;
    }

    @Override public Builder setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;

        return this;
    }

    @Override public boolean getPreVote() {
        return preVote;
    }

    @Override public Builder setPreVote(boolean preVote) {
        this.preVote = preVote;

        return this;
    }

    @Override public RpcRequests.RequestVoteRequest build() {
        return this;
    }
}
