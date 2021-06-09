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

import java.util.List;
import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersResponse;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;

/**
 * Process get all peers of the replication group request.
 */
public class GetPeersRequestProcessor extends BaseCliRequestProcessor<GetPeersRequest> {

    public GetPeersRequestProcessor(final Executor executor) {
        super(executor, GetPeersResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final GetPeersRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final GetPeersRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final GetPeersRequest request,
        final RpcRequestClosure done) {
        final List<PeerId> peers;
        final List<PeerId> learners;
        if (request.getOnlyAlive()) {
            peers = ctx.node.listAlivePeers();
            learners = ctx.node.listAliveLearners();
        }
        else {
            peers = ctx.node.listPeers();
            learners = ctx.node.listLearners();
        }
        final GetPeersResponse.Builder builder = GetPeersResponse.newBuilder();
        for (final PeerId peerId : peers) {
            builder.addPeers(peerId.toString());
        }
        for (final PeerId peerId : learners) {
            builder.addLearners(peerId.toString());
        }
        return builder.build();
    }

    @Override
    public String interest() {
        return GetPeersRequest.class.getName();
    }
}
