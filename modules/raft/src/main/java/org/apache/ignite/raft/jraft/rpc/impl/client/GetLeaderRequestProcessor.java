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
package org.apache.ignite.raft.jraft.rpc.impl.client;

import java.util.concurrent.Executor;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;

import static org.apache.ignite.raft.jraft.JRaftUtils.addressFromEndpoint;

/**
 * Process get leader request.
 */
public class GetLeaderRequestProcessor implements RpcProcessor<GetLeaderRequest> {
    private final Executor executor;

    private final RaftClientMessagesFactory factory;

    public GetLeaderRequestProcessor(Executor executor, RaftClientMessagesFactory factory) {
        this.executor = executor;
        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override public void handleRequest(RpcContext rpcCtx, GetLeaderRequest request) {
        NetworkAddress localAddr = rpcCtx.getLocalAddress();

        Node node = rpcCtx.getNodeManager().get(request.groupId(), new PeerId(localAddr.host(), localAddr.port()));

        // There's a race between starting a raft group and requesting a leader for it.
        // Let's say that there are 3 ignite nodes {A,B,C} in the cluster and according to an affinity raft group
        // with only one raft node should be created on ignite node A. However it's possible to request given
        // raft group from another ignite node B from within {@link RaftGroupService}. So that it's possible
        // that raftGroupService will be already created and requests a leader from a not yet existing raft group.
        if (node == null) {
            rpcCtx.sendResponse(factory.raftErrorResponse().errorCode(RaftErrorCode.NO_LEADER).build());

            return;
        }

        PeerId leaderId = node.getLeaderId();

        if (leaderId == null) {
            rpcCtx.sendResponse(factory.raftErrorResponse().errorCode(RaftErrorCode.NO_LEADER).build());

            return;
        }

        // Find by host and port.
        Peer leader0 = new Peer(addressFromEndpoint(leaderId.getEndpoint()));

        rpcCtx.sendResponse(factory.getLeaderResponse().leader(leader0).build());
    }

    /** {@inheritDoc} */
    @Override public String interest() {
        return GetLeaderRequest.class.getName();
    }

    /** {@inheritDoc} */
    @Override public Executor executor() {
        return executor;
    }
}
