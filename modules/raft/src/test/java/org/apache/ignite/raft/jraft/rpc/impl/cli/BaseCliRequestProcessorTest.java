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

import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.PingRequest;
import org.apache.ignite.raft.jraft.test.MockAsyncContext;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@ExtendWith(MockitoExtension.class)
public class BaseCliRequestProcessorTest {
    private static class MockCliRequestProcessor extends BaseCliRequestProcessor<PingRequest> {
        private String peerId;
        private String groupId;
        private RpcRequestClosure done;
        private CliRequestContext ctx;

        MockCliRequestProcessor(String peerId, String groupId) {
            super(null, null);
            this.peerId = peerId;
            this.groupId = groupId;
        }

        @Override
        protected String getPeerId(PingRequest request) {
            return this.peerId;
        }

        @Override
        protected String getGroupId(PingRequest request) {
            return this.groupId;
        }

        @Override
        protected Message processRequest0(CliRequestContext ctx, PingRequest request, RpcRequestClosure done) {
            this.ctx = ctx;
            this.done = done;
            return RaftRpcFactory.DEFAULT.newResponse(null, Status.OK());
        }

        @Override
        public String interest() {
            return PingRequest.class.getName();
        }

    }

    private MockCliRequestProcessor processor;
    private PeerId peer;
    private MockAsyncContext asyncContext;

    @BeforeEach
    public void setup() {
        this.asyncContext = new MockAsyncContext();
        this.peer = JRaftUtils.getPeerId("localhost:8081");
        this.processor = new MockCliRequestProcessor(this.peer.toString(), "test");
    }

    @AfterEach
    public void teardown() {
        // No-op.
    }

    @Test
    public void testOK() {
        Node node = mockNode(false);

        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(this.processor.done);
        assertSame(this.processor.ctx.node, node);
        assertNotNull(resp);
        assertEquals(0, resp.getErrorCode());
    }

    @Test
    public void testDisableCli() {
        mockNode(true);

        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(resp);
        assertEquals(RaftError.EACCES.getNumber(), resp.getErrorCode());
        assertEquals("Cli service is not allowed to access node <test/localhost:8081>", resp.getErrorMsg());
    }

    private Node mockNode(boolean disableCli) {
        Node node = Mockito.mock(Node.class);
        Mockito.when(node.getGroupId()).thenReturn("test");
        Mockito.when(node.getNodeId()).thenReturn(new NodeId("test", this.peer.copy()));
        NodeOptions opts = new NodeOptions();
        opts.setDisableCli(disableCli);
        Mockito.when(node.getOptions()).thenReturn(opts);
        this.asyncContext.getNodeManager().add(node);
        return node;
    }

    @Test
    public void testInvalidPeerId() {
        this.processor = new MockCliRequestProcessor("localhost", "test");
        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(resp);
        assertEquals(RaftError.EINVAL.getNumber(), resp.getErrorCode());
        assertEquals("Fail to parse peer: localhost", resp.getErrorMsg());
    }

    @Test
    public void testEmptyNodes() {
        this.processor = new MockCliRequestProcessor(null, "test");
        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(resp);
        assertEquals(RaftError.ENOENT.getNumber(), resp.getErrorCode());
        assertEquals("Empty nodes in group test", resp.getErrorMsg());
    }

    @Test
    public void testManyNodes() {
        Node node1 = Mockito.mock(Node.class);
        Mockito.when(node1.getGroupId()).thenReturn("test");
        Mockito.when(node1.getNodeId()).thenReturn(new NodeId("test", new PeerId("localhost", 8081)));
        this.asyncContext.getNodeManager().add(node1);

        Node node2 = Mockito.mock(Node.class);
        Mockito.when(node2.getGroupId()).thenReturn("test");
        Mockito.when(node2.getNodeId()).thenReturn(new NodeId("test", new PeerId("localhost", 8082)));
        this.asyncContext.getNodeManager().add(node2);

        this.processor = new MockCliRequestProcessor(null, "test");
        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(resp);
        assertEquals(RaftError.EINVAL.getNumber(), resp.getErrorCode());
        assertEquals("Peer must be specified since there're 2 nodes in group test", resp.getErrorMsg());
    }

    @Test
    public void testSingleNode() {
        Node node = this.mockNode(false);
        this.processor = new MockCliRequestProcessor(null, "test");
        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(resp);
        assertSame(this.processor.ctx.node, node);
        assertNotNull(resp);
        assertEquals(0, resp.getErrorCode());
    }

    @Test
    public void testPeerIdNotFound() {
        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(resp);
        assertEquals(RaftError.ENOENT.getNumber(), resp.getErrorCode());
        assertEquals("Fail to find node localhost:8081 in group test", resp.getErrorMsg());
    }
}
