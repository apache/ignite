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

import org.apache.ignite.raft.client.message.AddLearnersRequest;
import org.apache.ignite.raft.client.message.AddPeersRequest;
import org.apache.ignite.raft.client.message.ChangePeersResponse;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.GetPeersRequest;
import org.apache.ignite.raft.client.message.GetPeersResponse;
import org.apache.ignite.raft.client.message.RaftErrorResponse;
import org.apache.ignite.raft.client.message.RemoveLearnersRequest;
import org.apache.ignite.raft.client.message.RemovePeersRequest;
import org.apache.ignite.raft.client.message.SnapshotRequest;
import org.apache.ignite.raft.client.message.TransferLeadershipRequest;
import org.apache.ignite.raft.client.message.ActionRequest;
import org.apache.ignite.raft.client.message.ActionResponse;

/**
 * The default implementation.
 */
public class RaftClientMessageFactoryImpl implements RaftClientMessageFactory {
    /** {@inheritDoc} */
    @Override public AddPeersRequest.Builder addPeersRequest() {
        return new AddPeersRequestImpl();
    }

    /** {@inheritDoc} */
    @Override public ChangePeersResponse.Builder changePeersResponse() {
        return new ChangePeersResponseImpl();
    }

    /** {@inheritDoc} */
    @Override public RemovePeersRequest.Builder removePeerRequest() {
        return new RemovePeersRequestImpl();
    }

    /** {@inheritDoc} */
    @Override public SnapshotRequest.Builder snapshotRequest() {
        return new SnapshotRequestImpl();
    }

    /** {@inheritDoc} */
    @Override public TransferLeadershipRequest.Builder transferLeaderRequest() {
        return new TransferLeadershipRequestImpl();
    }

    /** {@inheritDoc} */
    @Override public GetLeaderRequest.Builder getLeaderRequest() {
        return new GetLeaderRequestImpl();
    }

    /** {@inheritDoc} */
    @Override public GetLeaderResponse.Builder getLeaderResponse() {
        return new GetLeaderResponseImpl();
    }

    /** {@inheritDoc} */
    @Override public GetPeersRequest.Builder getPeersRequest() {
        return new GetPeersRequestImpl();
    }

    /** {@inheritDoc} */
    @Override public GetPeersResponse.Builder getPeersResponse() {
        return new GetPeersResponseImpl();
    }

    /** {@inheritDoc} */
    @Override public AddLearnersRequest.Builder addLearnersRequest() {
        return new AddLearnersRequestImpl();
    }

    /** {@inheritDoc} */
    @Override public RemoveLearnersRequest.Builder removeLearnersRequest() {
        return new RemoveLearnersRequestImpl();
    }

    /** {@inheritDoc} */
    @Override public ActionRequest.Builder actionRequest() {
        return new ActionRequestImpl();
    }

    /** {@inheritDoc} */
    @Override public ActionResponse.Builder actionResponse() {
        return new ActionResponseImpl();
    }

    /** {@inheritDoc} */
    @Override public RaftErrorResponse.Builder raftErrorResponse() {
        return new RaftErrorResponseImpl();
    }
}
