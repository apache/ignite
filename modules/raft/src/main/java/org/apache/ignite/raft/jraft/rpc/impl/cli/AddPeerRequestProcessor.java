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
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerResponse;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;

/**
 * AddPeer request processor.
 */
public class AddPeerRequestProcessor extends BaseCliRequestProcessor<AddPeerRequest> {

    public AddPeerRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final AddPeerRequest request) {
        return request.leaderId();
    }

    @Override
    protected String getGroupId(final AddPeerRequest request) {
        return request.groupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final AddPeerRequest request,
        final RpcRequestClosure done) {
        final List<PeerId> oldPeers = ctx.node.listPeers();
        final String addingPeerIdStr = request.peerId();
        final PeerId addingPeer = new PeerId();
        if (addingPeer.parse(addingPeerIdStr)) {
            LOG.info("Receive AddPeerRequest to {} from {}, adding {}", ctx.node.getNodeId(), done.getRpcCtx()
                .getRemoteAddress(), addingPeerIdStr);
            ctx.node.addPeer(addingPeer, status -> {
                if (!status.isOk()) {
                    done.run(status);
                }
                else {
                    List<String> oldPeersList = new ArrayList<>();
                    List<String> newPeersList = new ArrayList<>();

                    boolean alreadyExists = false;
                    for (final PeerId oldPeer : oldPeers) {
                        oldPeersList.add(oldPeer.toString());
                        newPeersList.add(oldPeer.toString());
                        if (oldPeer.equals(addingPeer)) {
                            alreadyExists = true;
                        }
                    }
                    if (!alreadyExists) {
                        newPeersList.add(addingPeerIdStr);
                    }

                    AddPeerResponse req = msgFactory().addPeerResponse()
                        .newPeersList(newPeersList)
                        .oldPeersList(oldPeersList)
                        .build();

                    done.sendResponse(req);
                }
            });
        }
        else {
            return RaftRpcFactory.DEFAULT //
                .newResponse(msgFactory(), RaftError.EINVAL, "Fail to parse peer id %s", addingPeerIdStr);
        }

        return null;
    }

    @Override
    public String interest() {
        return AddPeerRequest.class.getName();
    }

}
