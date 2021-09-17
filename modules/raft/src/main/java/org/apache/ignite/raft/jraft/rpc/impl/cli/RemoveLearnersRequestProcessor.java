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
import org.apache.ignite.raft.jraft.rpc.CliRequests.LearnersOpResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemoveLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;

/**
 * RemoveLearners request processor.
 */
public class RemoveLearnersRequestProcessor extends BaseCliRequestProcessor<RemoveLearnersRequest> {

    public RemoveLearnersRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final RemoveLearnersRequest request) {
        return request.leaderId();
    }

    @Override
    protected String getGroupId(final RemoveLearnersRequest request) {
        return request.groupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final RemoveLearnersRequest request,
        final IgniteCliRpcRequestClosure done) {
        final List<PeerId> oldLearners = ctx.node.listLearners();
        final List<PeerId> removeingLearners = new ArrayList<>(request.learnersList().size());

        for (final String peerStr : request.learnersList()) {
            final PeerId peer = new PeerId();
            if (!peer.parse(peerStr)) {
                return RaftRpcFactory.DEFAULT //
                    .newResponse(msgFactory(), RaftError.EINVAL, "Fail to parse peer id %s", peerStr);
            }
            removeingLearners.add(peer);
        }

        LOG.info("Receive RemoveLearnersRequest to {} from {}, removing {}.", ctx.node.getNodeId(),
            done.getRpcCtx().getRemoteAddress(), removeingLearners);
        ctx.node.removeLearners(removeingLearners, status -> {
            if (!status.isOk()) {
                done.run(status);
            }
            else {
                List<String> oldLearnersList = new ArrayList<>();
                List<String> newLearnersList = new ArrayList<>();

                for (final PeerId peer : oldLearners) {
                    oldLearnersList.add(peer.toString());
                    if (!removeingLearners.contains(peer)) {
                        newLearnersList.add(peer.toString());
                    }
                }

                LearnersOpResponse response = msgFactory().learnersOpResponse()
                    .oldLearnersList(oldLearnersList)
                    .newLearnersList(newLearnersList)
                    .build();

                done.sendResponse(response);
            }
        });

        return null;
    }

    @Override
    public String interest() {
        return RemoveLearnersRequest.class.getName();
    }

}
