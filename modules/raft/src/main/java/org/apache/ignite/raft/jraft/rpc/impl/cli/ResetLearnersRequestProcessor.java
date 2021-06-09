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
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.CliRequests.LearnersOpResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ResetLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;

/**
 * ResetLearners request processor.
 */
public class ResetLearnersRequestProcessor extends BaseCliRequestProcessor<ResetLearnersRequest> {

    public ResetLearnersRequestProcessor(final Executor executor) {
        super(executor, LearnersOpResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final ResetLearnersRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final ResetLearnersRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final ResetLearnersRequest request,
        final RpcRequestClosure done) {
        final List<PeerId> oldLearners = ctx.node.listLearners();
        final List<PeerId> newLearners = new ArrayList<>(request.getLearnersCount());

        for (final String peerStr : request.getLearnersList()) {
            final PeerId peer = new PeerId();
            if (!peer.parse(peerStr)) {
                return RaftRpcFactory.DEFAULT
                    .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", peerStr);
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
                final LearnersOpResponse.Builder rb = LearnersOpResponse.newBuilder();

                for (final PeerId peer : oldLearners) {
                    rb.addOldLearners(peer.toString());
                }

                for (final PeerId peer : newLearners) {
                    rb.addNewLearners(peer.toString());
                }

                done.sendResponse(rb.build());
            }
        });

        return null;
    }

    @Override
    public String interest() {
        return ResetLearnersRequest.class.getName();
    }

}
