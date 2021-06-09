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
package org.apache.ignite.raft.jraft.rpc.impl.client;

import java.util.concurrent.Executor;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.client.message.RaftErrorResponseBuilder;
import org.apache.ignite.raft.client.message.SnapshotRequest;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;

/**
 * Process snapshot request.
 */
public class SnapshotRequestProcessor implements RpcProcessor<SnapshotRequest> {
    private final Executor executor;
    private final RaftClientMessagesFactory factory;

    public SnapshotRequestProcessor(Executor executor, RaftClientMessagesFactory factory) {
        this.executor = executor;
        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override public void handleRequest(RpcContext rpcCtx, SnapshotRequest request) {
        Node node = rpcCtx.getNodeManager().get(request.groupId(), PeerId.parsePeer(rpcCtx.getLocalAddress()));

        if (node == null) {
            rpcCtx.sendResponse(factory.raftErrorResponse().errorCode(RaftErrorCode.ILLEGAL_STATE).build());

            return;
        }

        node.snapshot(new Closure() {
            @Override public void run(Status status) {
                RaftErrorResponseBuilder resp = factory.raftErrorResponse();

                if (!status.isOk()) {
                    resp.errorCode(RaftErrorCode.SNAPSHOT).errorMessage(status.getErrorMsg());
                }

                rpcCtx.sendResponse(resp.build());
            }
        });
    }

    /** {@inheritDoc} */
    @Override public String interest() {
        return SnapshotRequest.class.getName();
    }

    /** {@inheritDoc} */
    @Override public Executor executor() {
        return executor;
    }
}
