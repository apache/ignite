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
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.LearnersOpResponse;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;

/**
 * AddLearners request processor.
 *
 * @author jiachun.fjc
 */
public class AddLearnersRequestProcessor extends BaseCliRequestProcessor<AddLearnersRequest> {
    public AddLearnersRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final AddLearnersRequest request) {
        return request.leaderId();
    }

    @Override
    protected String getGroupId(final AddLearnersRequest request) {
        return request.groupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final AddLearnersRequest request,
        final RpcRequestClosure done) {
        final List<PeerId> oldLearners = ctx.node.listLearners();
        final List<PeerId> addingLearners = new ArrayList<>();

        for (final String peerStr : request.learnersList()) {
            final PeerId peer = new PeerId();
            if (!peer.parse(peerStr)) {
                return RaftRpcFactory.DEFAULT //
                    .newResponse(msgFactory(), RaftError.EINVAL, "Fail to parse peer id %s", peerStr);
            }
            addingLearners.add(peer);
        }

        LOG.info("Receive AddLearnersRequest to {} from {}, adding {}.", ctx.node.getNodeId(),
            done.getRpcCtx().getRemoteAddress(), addingLearners);
        ctx.node.addLearners(addingLearners, status -> {
            if (!status.isOk()) {
                done.run(status);
            }
            else {
                List<String> oldLearnersList = new ArrayList<>();
                List<String> newLearnersList = new ArrayList<>();

                for (final PeerId peer : oldLearners) {
                    oldLearnersList.add(peer.toString());
                    newLearnersList.add(peer.toString());
                }

                for (final PeerId peer : addingLearners) {
                    if (!oldLearners.contains(peer)) {
                        newLearnersList.add(peer.toString());
                    }
                }

                LearnersOpResponse req = msgFactory().learnersOpResponse()
                    .oldLearnersList(oldLearnersList)
                    .newLearnersList(newLearnersList)
                    .build();

                done.sendResponse(req);
            }
        });

        return null;
    }

    @Override
    public String interest() {
        return AddLearnersRequest.class.getName();
    }

}
