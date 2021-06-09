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
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.util.ByteString;

class ReadIndexRequestImpl implements RpcRequests.ReadIndexRequest, RpcRequests.ReadIndexRequest.Builder {
    private String groupId;
    private String serverId;
    private List<ByteString> entriesList = new ArrayList<>();
    private String peerId;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public String getServerId() {
        return serverId;
    }

    @Override public List<ByteString> getEntriesList() {
        return entriesList;
    }

    @Override public int getEntriesCount() {
        return entriesList.size();
    }

    @Override public ByteString getEntries(int index) {
        return entriesList.get(index);
    }

    @Override public String getPeerId() {
        return peerId;
    }

    @Override public RpcRequests.ReadIndexRequest build() {
        return this;
    }

    @Override public Builder mergeFrom(RpcRequests.ReadIndexRequest request) {
        setGroupId(request.getGroupId());
        setServerId(request.getServerId());
        setPeerId(request.getPeerId());
        for (ByteString data : request.getEntriesList()) {
            addEntries(data);
        }

        return this;
    }

    @Override public Builder setPeerId(String peerId) {
        this.peerId = peerId;

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

    @Override public Builder addEntries(ByteString data) {
        entriesList.add(data);

        return this;
    }
}
