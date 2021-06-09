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

import org.apache.ignite.raft.jraft.rpc.CliRequests;

public class TransferLeaderRequestImpl implements CliRequests.TransferLeaderRequest, CliRequests.TransferLeaderRequest.Builder {
    private String groupId;
    private String leaderId;
    private String peerId;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public String getLeaderId() {
        return leaderId;
    }

    @Override public String getPeerId() {
        return peerId;
    }

    @Override public boolean hasPeerId() {
        return peerId != null;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setLeaderId(String leaderId) {
        this.leaderId = leaderId;

        return this;
    }

    @Override public Builder setPeerId(String peerId) {
        this.peerId = peerId;

        return this;
    }

    @Override public CliRequests.TransferLeaderRequest build() {
        return this;
    }
}
