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
package org.apache.ignite.raft.jraft.rpc.impl.core;

import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;

public class InstallSnapshotRequestProcessorTest extends BaseNodeRequestProcessorTest<InstallSnapshotRequest> {
    private InstallSnapshotRequest request;

    @Override
    public InstallSnapshotRequest createRequest(String groupId, PeerId peerId) {
        request = msgFactory.installSnapshotRequest()
            .groupId(groupId)
            .serverId("localhost:8082")
            .peerId(peerId.toString())
            .term(0)
            .meta(msgFactory.snapshotMeta().lastIncludedIndex(1).lastIncludedTerm(1).build())
            .uri("test")
            .build();
        return request;
    }

    @Override
    public NodeRequestProcessor<InstallSnapshotRequest> newProcessor() {
        return new InstallSnapshotRequestProcessor(null, msgFactory);
    }

    @Override
    public void verify(String interest, RaftServerService service,
        NodeRequestProcessor<InstallSnapshotRequest> processor) {
        assertEquals(interest, InstallSnapshotRequest.class.getName());
        Mockito.verify(service).handleInstallSnapshot(eq(request), Mockito.any());
    }

}
