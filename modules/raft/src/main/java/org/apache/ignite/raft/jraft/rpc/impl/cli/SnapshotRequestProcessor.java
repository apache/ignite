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
import org.apache.ignite.raft.jraft.rpc.CliRequests.SnapshotRequest;
import org.apache.ignite.raft.jraft.rpc.Message;

/**
 * Snapshot request processor.
 */
public class SnapshotRequestProcessor extends BaseCliRequestProcessor<SnapshotRequest> {

    public SnapshotRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final SnapshotRequest request) {
        return request.peerId();
    }

    @Override
    protected String getGroupId(final SnapshotRequest request) {
        return request.groupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final SnapshotRequest request,
        final IgniteCliRpcRequestClosure done) {
        LOG.info("Receive SnapshotRequest to {} from {}", ctx.node.getNodeId(), request.peerId());
        ctx.node.snapshot(done);
        return null;
    }

    @Override
    public String interest() {
        return SnapshotRequest.class.getName();
    }

}
