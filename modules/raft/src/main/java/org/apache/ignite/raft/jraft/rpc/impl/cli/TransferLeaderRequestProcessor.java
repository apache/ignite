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

import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.CliRequests.TransferLeaderRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;

/**
 * Snapshot request processor.
 */
public class TransferLeaderRequestProcessor extends BaseCliRequestProcessor<TransferLeaderRequest> {

    public TransferLeaderRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final TransferLeaderRequest request) {
        return request.leaderId();
    }

    @Override
    protected String getGroupId(final TransferLeaderRequest request) {
        return request.groupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final TransferLeaderRequest request,
        final IgniteCliRpcRequestClosure done) {
        final PeerId peer = new PeerId();
        if (request.peerId() != null && !peer.parse(request.peerId())) {
            return RaftRpcFactory.DEFAULT //
                .newResponse(msgFactory(), RaftError.EINVAL, "Fail to parse peer id %s", request.peerId());
        }
        LOG.info("Receive TransferLeaderRequest to {} from {}, newLeader will be {}.", ctx.node.getNodeId(), done
            .getRpcCtx().getRemoteAddress(), peer);
        final Status st = ctx.node.transferLeadershipTo(peer);
        return RaftRpcFactory.DEFAULT //
            .newResponse(msgFactory(), st);
    }

    @Override
    public String interest() {
        return TransferLeaderRequest.class.getName();
    }

}
