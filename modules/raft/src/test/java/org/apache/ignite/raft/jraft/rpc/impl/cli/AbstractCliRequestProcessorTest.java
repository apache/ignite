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
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.test.MockAsyncContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public abstract class AbstractCliRequestProcessorTest<T extends Message> {
    @Mock
    private Node node;
    private final String groupId = "test";
    private final String peerIdStr = "localhost:8081";
    protected MockAsyncContext asyncContext;
    protected NodeManager nodeManager = new NodeManager();
    protected RaftMessagesFactory msgFactory = new RaftMessagesFactory();

    public abstract T createRequest(String groupId, PeerId peerId);

    public abstract BaseCliRequestProcessor<T> newProcessor();

    public abstract void verify(String interest, Node node, ArgumentCaptor<Closure> doneArg);

    public void mockNodes(final int n) {
        ArrayList<PeerId> peers = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            peers.add(JRaftUtils.getPeerId("localhost:" + (8081 + i)));
        }
        List<PeerId> learners = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            learners.add(JRaftUtils.getPeerId("learner:" + (8081 + i)));
        }
        Mockito.lenient().when(this.node.listPeers()).thenReturn(peers);
        Mockito.lenient().when(this.node.listLearners()).thenReturn(learners);
    }

    @BeforeEach
    public void setup() {
        this.asyncContext = new MockAsyncContext();
    }

    @AfterEach
    public void teardown() {
        nodeManager.clear();
    }

    @Test
    public void testHandleRequest() {
        this.mockNodes(3);
        Mockito.when(this.node.getGroupId()).thenReturn(this.groupId);
        PeerId peerId = new PeerId();
        peerId.parse(this.peerIdStr);
        Mockito.when(this.node.getOptions()).thenReturn(new NodeOptions());
        Mockito.when(this.node.getNodeId()).thenReturn(new NodeId("test", peerId));
        if (asyncContext != null)
            asyncContext.getNodeManager().add(node);

        BaseCliRequestProcessor<T> processor = newProcessor();
        processor.handleRequest(this.asyncContext, createRequest(this.groupId, peerId));
        ArgumentCaptor<Closure> doneArg = ArgumentCaptor.forClass(Closure.class);
        verify(processor.interest(), this.node, doneArg);
    }
}
