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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.apache.ignite.raft.jraft.util.Marshaller;

class AppendEntriesRequestImpl implements RpcRequests.AppendEntriesRequest, RpcRequests.AppendEntriesRequest.Builder {
    private String groupId;
    private String serverId;
    private String peerId;
    private long term;
    private long prevLogTerm;
    private long prevLogIndex;
    private List<RaftOutter.EntryMeta> entiesList = new ArrayList<>();
    private long committedIndex;
    private ByteString data;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public String getServerId() {
        return serverId;
    }

    @Override public String getPeerId() {
        return peerId;
    }

    @Override public long getTerm() {
        return term;
    }

    @Override public long getPrevLogTerm() {
        return prevLogTerm;
    }

    @Override public long getPrevLogIndex() {
        return prevLogIndex;
    }

    @Override public List<RaftOutter.EntryMeta> getEntriesList() {
        return entiesList;
    }

    @Override public RaftOutter.EntryMeta getEntries(int index) {
        return entiesList.get(index);
    }

    @Override public int getEntriesCount() {
        return entiesList.size();
    }

    @Override public long getCommittedIndex() {
        return committedIndex;
    }

    @Override public ByteString getData() {
        return data;
    }

    @Override public boolean hasData() {
        return data != null;
    }

    @Override public byte[] toByteArray() {
        return Marshaller.DEFAULT.marshall(this);
    }

    @Override public RpcRequests.AppendEntriesRequest build() {
        return this;
    }

    @Override public Builder setData(ByteString data) {
        this.data = data;

        return this;
    }

    @Override public Builder setTerm(long term) {
        this.term = term;

        return this;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setServerId(String serverId) {
        this.serverId = serverId;

        return this;
    }

    @Override public Builder setPeerId(String peerId) {
        this.peerId = peerId;

        return this;
    }

    @Override public Builder setPrevLogIndex(long prevLogIndex) {
        this.prevLogIndex = prevLogIndex;

        return this;
    }

    @Override public Builder setPrevLogTerm(long prevLogTerm) {
        this.prevLogTerm = prevLogTerm;

        return this;
    }

    @Override public Builder setCommittedIndex(long lastCommittedIndex) {
        this.committedIndex = lastCommittedIndex;

        return this;
    }

    @Override public Builder addEntries(RaftOutter.EntryMeta entryMeta) {
        entiesList.add(entryMeta);

        return this;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        AppendEntriesRequestImpl that = (AppendEntriesRequestImpl) o;

        if (term != that.term)
            return false;
        if (prevLogTerm != that.prevLogTerm)
            return false;
        if (prevLogIndex != that.prevLogIndex)
            return false;
        if (committedIndex != that.committedIndex)
            return false;
        if (!groupId.equals(that.groupId))
            return false;
        if (!serverId.equals(that.serverId))
            return false;
        if (!peerId.equals(that.peerId))
            return false;
        if (!entiesList.equals(that.entiesList))
            return false;
        return data != null ? data.equals(that.data) : that.data == null;
    }

    @Override public int hashCode() {
        int result = groupId.hashCode();
        result = 31 * result + serverId.hashCode();
        result = 31 * result + peerId.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (prevLogTerm ^ (prevLogTerm >>> 32));
        result = 31 * result + (int) (prevLogIndex ^ (prevLogIndex >>> 32));
        result = 31 * result + entiesList.hashCode();
        result = 31 * result + (int) (committedIndex ^ (committedIndex >>> 32));
        result = 31 * result + (data != null ? data.hashCode() : 0);
        return result;
    }
}
