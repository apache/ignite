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

import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.PingRequest;
import org.apache.ignite.raft.jraft.test.MockAsyncContext;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.withSettings;

public class NodeRequestProcessorTest {
    private static class MockRequestProcessor extends NodeRequestProcessor<PingRequest> {
        private String peerId;
        private String groupId;

        MockRequestProcessor(String peerId, String groupId) {
            super(null, new RaftMessagesFactory());
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
        protected Message processRequest0(RaftServerService serviceService, PingRequest request,
            RpcRequestClosure done) {
            return RaftRpcFactory.DEFAULT.newResponse(msgFactory(), Status.OK());
        }

        @Override
        public String interest() {
            return PingRequest.class.getName();
        }

    }

    private MockRequestProcessor processor;
    private MockAsyncContext asyncContext;

    @BeforeEach
    public void setup() {
        this.asyncContext = new MockAsyncContext();
        this.processor = new MockRequestProcessor("localhost:8081", "test");
    }

    @AfterEach
    public void teardown() {
        // No-op.
    }

    @Test
    public void testOK() {
        Node node = Mockito.mock(Node.class, withSettings().extraInterfaces(RaftServerService.class));

        Mockito.when(node.getGroupId()).thenReturn("test");
        PeerId peerId = new PeerId("localhost", 8081);
        Mockito.when(node.getNodeId()).thenReturn(new NodeId("test", peerId));

        asyncContext.getNodeManager().add(node);

        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(resp);
        assertEquals(0, resp.errorCode());
    }

    @Test
    public void testInvalidPeerId() {
        this.processor = new MockRequestProcessor("localhost", "test");
        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(resp);
        assertEquals(RaftError.EINVAL.getNumber(), resp.errorCode());
        assertEquals("Fail to parse peerId: localhost", resp.errorMsg());
    }

    @Test
    public void testPeerIdNotFound() {
        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(resp);
        assertEquals(RaftError.ENOENT.getNumber(), resp.errorCode());
        assertEquals("Peer id not found: localhost:8081, group: test", resp.errorMsg());
    }
}
