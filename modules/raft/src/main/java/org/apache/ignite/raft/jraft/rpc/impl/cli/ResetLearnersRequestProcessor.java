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
import org.apache.ignite.raft.jraft.rpc.CliRequests.ResetLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;

import static java.util.stream.Collectors.toList;

/**
 * ResetLearners request processor.
 */
public class ResetLearnersRequestProcessor extends BaseCliRequestProcessor<ResetLearnersRequest> {

    public ResetLearnersRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final ResetLearnersRequest request) {
        return request.leaderId();
    }

    @Override
    protected String getGroupId(final ResetLearnersRequest request) {
        return request.groupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final ResetLearnersRequest request,
        final IgniteCliRpcRequestClosure done) {
        final List<PeerId> oldLearners = ctx.node.listLearners();
        final List<PeerId> newLearners = new ArrayList<>(request.learnersList().size());

        for (final String peerStr : request.learnersList()) {
            final PeerId peer = new PeerId();
            if (!peer.parse(peerStr)) {
                return RaftRpcFactory.DEFAULT
                    .newResponse(msgFactory(), RaftError.EINVAL, "Fail to parse peer id %s", peerStr);
            }
            newLearners.add(peer);
        }

        LOG.info("Receive ResetLearnersRequest to {} from {}, resetting into {}.", ctx.node.getNodeId(),
            done.getRpcCtx().getRemoteAddress(), newLearners);
        ctx.node.resetLearners(newLearners, status -> {
            if (!status.isOk()) {
                done.run(status);
            }
            else {
                LearnersOpResponse response = msgFactory().learnersOpResponse()
                    .oldLearnersList(oldLearners.stream().map(Object::toString).collect(toList()))
                    .newLearnersList(newLearners.stream().map(Object::toString).collect(toList()))
                    .build();

                done.sendResponse(response);
            }
        });

        return null;
    }

    @Override
    public String interest() {
        return ResetLearnersRequest.class.getName();
    }

}
