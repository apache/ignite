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

import java.util.concurrent.ExecutorService;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.test.MockAsyncContext;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.concurrent.FixedThreadsExecutorGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public abstract class BaseNodeRequestProcessorTest<T extends Message> {
    @Mock(extraInterfaces = {RaftServerService.class})
    private Node node;
    protected final String groupId = "test";
    protected final String peerIdStr = "localhost:8081";
    protected MockAsyncContext asyncContext;
    protected RaftMessagesFactory msgFactory = new RaftMessagesFactory();
    private ExecutorService executor;
    private FixedThreadsExecutorGroup appendEntriesExecutor;

    public abstract T createRequest(String groupId, PeerId peerId);

    public abstract NodeRequestProcessor<T> newProcessor();

    public abstract void verify(String interest, RaftServerService service, NodeRequestProcessor<T> processor);

    @BeforeEach
    public void setup() {
        Mockito.lenient().when(node.getRaftOptions()).thenReturn(new RaftOptions());
        this.asyncContext = new MockAsyncContext();
    }

    @AfterEach
    public void teardown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
        if (appendEntriesExecutor != null)
            appendEntriesExecutor.shutdownGracefully();
    }

    @Test
    public void testHandleRequest() {
        final PeerId peerId = mockNode();

        final NodeRequestProcessor<T> processor = newProcessor();
        processor.handleRequest(asyncContext, createRequest(groupId, peerId));
        verify(processor.interest(), (RaftServerService) this.node, processor);
    }

    protected PeerId mockNode() {
        Mockito.when(node.getGroupId()).thenReturn(this.groupId);
        final PeerId peerId = new PeerId();
        peerId.parse(this.peerIdStr);
        Mockito.when(node.getNodeId()).thenReturn(new NodeId(groupId, peerId));

        NodeOptions nodeOptions = new NodeOptions();

        executor = JRaftUtils.createCommonExecutor(nodeOptions);
        nodeOptions.setCommonExecutor(executor);
        appendEntriesExecutor = JRaftUtils.createAppendEntriesExecutor(nodeOptions);
        nodeOptions.setStripedExecutor(appendEntriesExecutor);

        Mockito.lenient().when(node.getOptions()).thenReturn(nodeOptions);
        if (asyncContext != null)
            asyncContext.getNodeManager().add(node);
        return peerId;
    }
}
