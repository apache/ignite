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
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerResponse;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;

/**
 * Remove peer request processor.
 */
public class RemovePeerRequestProcessor extends BaseCliRequestProcessor<RemovePeerRequest> {

    public RemovePeerRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final RemovePeerRequest request) {
        return request.leaderId();
    }

    @Override
    protected String getGroupId(final RemovePeerRequest request) {
        return request.groupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final RemovePeerRequest request,
        final IgniteCliRpcRequestClosure done) {
        final List<PeerId> oldPeers = ctx.node.listPeers();
        final String removingPeerIdStr = request.peerId();
        final PeerId removingPeer = new PeerId();
        if (removingPeer.parse(removingPeerIdStr)) {
            LOG.info("Receive RemovePeerRequest to {} from {}, removing {}", ctx.node.getNodeId(), done.getRpcCtx()
                .getRemoteAddress(), removingPeerIdStr);
            ctx.node.removePeer(removingPeer, status -> {
                if (!status.isOk()) {
                    done.run(status);
                }
                else {
                    List<String> oldPeersList = new ArrayList<>();
                    List<String> newPeersList = new ArrayList<>();

                    for (final PeerId oldPeer : oldPeers) {
                        oldPeersList.add(oldPeer.toString());
                        if (!oldPeer.equals(removingPeer)) {
                            newPeersList.add(oldPeer.toString());
                        }
                    }

                    RemovePeerResponse rb = msgFactory().removePeerResponse()
                        .newPeersList(newPeersList)
                        .oldPeersList(oldPeersList)
                        .build();

                    done.sendResponse(rb);
                }
            });
        }
        else {
            return RaftRpcFactory.DEFAULT //
                .newResponse(msgFactory(), RaftError.EINVAL, "Fail to parse peer id %s", removingPeerIdStr);
        }

        return null;
    }

    @Override
    public String interest() {
        return RemovePeerRequest.class.getName();
    }
}
