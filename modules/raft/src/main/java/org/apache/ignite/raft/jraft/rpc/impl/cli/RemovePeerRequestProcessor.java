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
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerResponse;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;

/**
 * Remove peer request processor.
 */
public class RemovePeerRequestProcessor extends BaseCliRequestProcessor<RemovePeerRequest> {

    public RemovePeerRequestProcessor(Executor executor) {
        super(executor, RemovePeerResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final RemovePeerRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final RemovePeerRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final RemovePeerRequest request,
        final RpcRequestClosure done) {
        final List<PeerId> oldPeers = ctx.node.listPeers();
        final String removingPeerIdStr = request.getPeerId();
        final PeerId removingPeer = new PeerId();
        if (removingPeer.parse(removingPeerIdStr)) {
            LOG.info("Receive RemovePeerRequest to {} from {}, removing {}", ctx.node.getNodeId(), done.getRpcCtx()
                .getRemoteAddress(), removingPeerIdStr);
            ctx.node.removePeer(removingPeer, status -> {
                if (!status.isOk()) {
                    done.run(status);
                }
                else {
                    final RemovePeerResponse.Builder rb = RemovePeerResponse.newBuilder();
                    for (final PeerId oldPeer : oldPeers) {
                        rb.addOldPeers(oldPeer.toString());
                        if (!oldPeer.equals(removingPeer)) {
                            rb.addNewPeers(oldPeer.toString());
                        }
                    }
                    done.sendResponse(rb.build());
                }
            });
        }
        else {
            return RaftRpcFactory.DEFAULT //
                .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", removingPeerIdStr);
        }

        return null;
    }

    @Override
    public String interest() {
        return RemovePeerRequest.class.getName();
    }
}
