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
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ResetPeerRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;

/**
 * Reset peer request processor.
 */
public class ResetPeerRequestProcessor extends BaseCliRequestProcessor<ResetPeerRequest> {

    public ResetPeerRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final ResetPeerRequest request) {
        return request.peerId();
    }

    @Override
    protected String getGroupId(final ResetPeerRequest request) {
        return request.groupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final ResetPeerRequest request,
        final RpcRequestClosure done) {
        final Configuration newConf = new Configuration();
        for (final String peerIdStr : request.newPeersList()) {
            final PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                newConf.addPeer(peer);
            }
            else {
                return RaftRpcFactory.DEFAULT //
                    .newResponse(msgFactory(), RaftError.EINVAL, "Fail to parse peer id %s", peerIdStr);
            }
        }
        LOG.info("Receive ResetPeerRequest to {} from {}, new conf is {}", ctx.node.getNodeId(), done.getRpcCtx()
            .getRemoteAddress(), newConf);
        final Status st = ctx.node.resetPeers(newConf);
        return RaftRpcFactory.DEFAULT //
            .newResponse(msgFactory(), st);
    }

    @Override
    public String interest() {
        return ResetPeerRequest.class.getName();
    }

}
