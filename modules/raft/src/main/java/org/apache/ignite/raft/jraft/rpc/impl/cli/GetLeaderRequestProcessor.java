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
package org.apache.ignite.raft.jraft.rpc.impl.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;

/**
 * Process get leader request.
 */
public class GetLeaderRequestProcessor extends BaseCliRequestProcessor<GetLeaderRequest> {

    public GetLeaderRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final GetLeaderRequest request) {
        return request.peerId();
    }

    @Override
    protected String getGroupId(final GetLeaderRequest request) {
        return request.groupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final GetLeaderRequest request,
        final RpcRequestClosure done) {
        // ignore
        return null;
    }

    @Override
    public Message processRequest(final GetLeaderRequest request, final RpcRequestClosure done) {
        List<Node> nodes = new ArrayList<>();
        final String groupId = getGroupId(request);
        if (request.peerId() != null) {
            final String peerIdStr = getPeerId(request);
            final PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                final Status st = new Status();
                nodes.add(getNode(groupId, peer, st, done.getRpcCtx().getNodeManager()));
                if (!st.isOk()) {
                    return RaftRpcFactory.DEFAULT //
                        .newResponse(msgFactory(), st);
                }
            }
            else {
                return RaftRpcFactory.DEFAULT //
                    .newResponse(msgFactory(), RaftError.EINVAL, "Fail to parse peer id %s", peerIdStr);
            }
        }
        else {
            nodes = done.getRpcCtx().getNodeManager().getNodesByGroupId(groupId);
        }
        if (nodes == null || nodes.isEmpty()) {
            return RaftRpcFactory.DEFAULT //
                .newResponse(msgFactory(), RaftError.ENOENT, "No nodes in group %s", groupId);
        }
        for (final Node node : nodes) {
            final PeerId leader = node.getLeaderId();
            if (leader != null && !leader.isEmpty()) {
                return msgFactory().getLeaderResponse()
                    .leaderId(leader.toString())
                    .build();
            }
        }
        return RaftRpcFactory.DEFAULT //
            .newResponse(msgFactory(), RaftError.EAGAIN, "Unknown leader");
    }

    @Override
    public String interest() {
        return GetLeaderRequest.class.getName();
    }

}
