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

import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SnapshotRequest;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SnapshotRequestProcessorTest extends AbstractCliRequestProcessorTest<SnapshotRequest> {

    @Override
    public SnapshotRequest createRequest(String groupId, PeerId peerId) {
        return msgFactory.snapshotRequest()
            .groupId(groupId)
            .peerId(peerId.toString())
            .build();
    }

    @Override
    public BaseCliRequestProcessor<SnapshotRequest> newProcessor() {
        return new SnapshotRequestProcessor(null, msgFactory);
    }

    @Override
    public void verify(String interest, Node node, ArgumentCaptor<Closure> doneArg) {
        assertEquals(SnapshotRequest.class.getName(), interest);
        Mockito.verify(node).snapshot(doneArg.capture());
        assertNotNull(doneArg.getValue());
    }

}
