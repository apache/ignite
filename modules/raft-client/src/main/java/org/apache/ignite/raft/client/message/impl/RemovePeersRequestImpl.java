/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.client.message.impl;

import java.util.List;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RemovePeersRequest;

/** */
class RemovePeersRequestImpl implements RemovePeersRequest, RemovePeersRequest.Builder {
    /** */
    private String groupId;

    /** */
    private List<Peer> peers;

    /** {@inheritDoc} */
    @Override public String groupId() {
        return groupId;
    }

    /** {@inheritDoc} */
    @Override public List<Peer> peers() {
        return peers;
    }

    /** {@inheritDoc} */
    @Override public Builder groupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    /** {@inheritDoc} */
    @Override public Builder peers(List<Peer> peers) {
        this.peers = peers;

        return this;
    }

    /** {@inheritDoc} */
    @Override public RemovePeersRequest build() {
        return this;
    }
}
