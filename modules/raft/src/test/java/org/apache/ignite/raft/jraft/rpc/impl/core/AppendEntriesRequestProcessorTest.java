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
import java.util.concurrent.Executors;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.PingRequest;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestProcessor.PeerPair;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestProcessor.PeerRequestContext;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;

public class AppendEntriesRequestProcessorTest extends BaseNodeRequestProcessorTest<AppendEntriesRequest> {

    private AppendEntriesRequest request;

    private final String serverId = "localhost:8082";

    private NodeManager nodeManager;

    @Override
    public AppendEntriesRequest createRequest(final String groupId, final PeerId peerId) {
        this.request = msgFactory.appendEntriesRequest()
            .committedIndex(0)
            .groupId(groupId)
            .peerId(peerId.toString())
            .serverId(this.serverId)
            .prevLogIndex(0)
            .term(0)
            .prevLogTerm(0)
            .build();
        return this.request;
    }

    @BeforeEach
    public void setupNodeManager() {
        this.nodeManager = this.asyncContext.getNodeManager();
    }

    private ExecutorService executor;

    @Override
    public NodeRequestProcessor<AppendEntriesRequest> newProcessor() {
        this.executor = Executors.newSingleThreadExecutor();
        return new AppendEntriesRequestProcessor(this.executor, msgFactory);
    }

    @AfterEach
    @Override public void teardown() {
        if (this.executor != null) {
            this.executor.shutdownNow();
        }
        super.teardown();
    }

    @Test
    public void testPairOf() {
        final AppendEntriesRequestProcessor processor = (AppendEntriesRequestProcessor) newProcessor();

        PeerPair pair = processor.pairOf(this.peerIdStr, this.serverId);
        assertEquals(pair.remote, this.serverId);
        assertEquals(pair.local, this.peerIdStr);

        // test constant pool
        assertSame(pair, processor.pairOf(this.peerIdStr, this.serverId));
        assertSame(pair, processor.pairOf(this.peerIdStr, this.serverId));
        assertEquals("PeerPair[localhost:8081 -> localhost:8082]", pair.toString());
    }

    @Test
    public void testOnClosed() {
        mockNode();
        final AppendEntriesRequestProcessor processor = (AppendEntriesRequestProcessor) newProcessor();

        PeerPair pair = processor.pairOf(this.peerIdStr, this.serverId);
        final PeerRequestContext ctx = processor.getOrCreatePeerRequestContext(this.groupId, pair, nodeManager);
        assertNotNull(ctx);
        assertSame(ctx, processor.getPeerRequestContext(this.groupId, pair));
        assertSame(ctx, processor.getOrCreatePeerRequestContext(this.groupId, pair, nodeManager));

        processor.onClosed(peerIdStr, this.serverId);
        assertNull(processor.getPeerRequestContext(this.groupId, pair));
        assertNotSame(ctx, processor.getOrCreatePeerRequestContext(this.groupId, pair, nodeManager));
    }

    @Override
    public void verify(final String interest, final RaftServerService service,
        final NodeRequestProcessor<AppendEntriesRequest> processor) {
        assertEquals(interest, AppendEntriesRequest.class.getName());
        Mockito.verify(service).handleAppendEntriesRequest(eq(this.request), Mockito.any());
        final PeerPair pair = ((AppendEntriesRequestProcessor) processor).pairOf(this.peerIdStr, this.serverId);
        final PeerRequestContext ctx = ((AppendEntriesRequestProcessor) processor).getOrCreatePeerRequestContext(
            this.groupId, pair, nodeManager);
        assertNotNull(ctx);
    }

    @Test
    public void testGetPeerRequestContextRemovePeerRequestContext() {
        mockNode();

        final AppendEntriesRequestProcessor processor = (AppendEntriesRequestProcessor) newProcessor();
        final PeerPair pair = processor.pairOf(this.peerIdStr, this.serverId);
        final PeerRequestContext ctx = processor.getOrCreatePeerRequestContext(this.groupId, pair, nodeManager);
        assertNotNull(ctx);
        assertSame(ctx, processor.getOrCreatePeerRequestContext(this.groupId, pair, nodeManager));
        assertEquals(0, ctx.getNextRequiredSequence());
        assertEquals(0, ctx.getAndIncrementSequence());
        assertEquals(1, ctx.getAndIncrementSequence());
        assertEquals(0, ctx.getAndIncrementNextRequiredSequence());
        assertEquals(1, ctx.getAndIncrementNextRequiredSequence());
        assertFalse(ctx.hasTooManyPendingResponses());

        processor.removePeerRequestContext(this.groupId, pair);
        final PeerRequestContext newCtx = processor.getOrCreatePeerRequestContext(this.groupId, pair, nodeManager);
        assertNotNull(newCtx);
        assertNotSame(ctx, newCtx);

        assertEquals(0, newCtx.getNextRequiredSequence());
        assertEquals(0, newCtx.getAndIncrementSequence());
        assertEquals(1, newCtx.getAndIncrementSequence());
        assertEquals(0, newCtx.getAndIncrementNextRequiredSequence());
        assertEquals(1, newCtx.getAndIncrementNextRequiredSequence());
        assertFalse(newCtx.hasTooManyPendingResponses());
    }

    @Test
    public void testSendSequenceResponse() {
        mockNode();
        final AppendEntriesRequestProcessor processor = (AppendEntriesRequestProcessor) newProcessor();
        final PeerPair pair = processor.pairOf(this.peerIdStr, this.serverId);
        processor.getOrCreatePeerRequestContext(this.groupId, pair, nodeManager);
        final PingRequest msg = TestUtils.createPingRequest();
        final RpcContext asyncContext = Mockito.mock(RpcContext.class);
        processor.sendSequenceResponse(this.groupId, pair, 1, asyncContext, msg);
        Mockito.verify(asyncContext, Mockito.never()).sendResponse(msg);

        processor.sendSequenceResponse(this.groupId, pair, 0, asyncContext, msg);
        Mockito.verify(asyncContext, Mockito.times(2)).sendResponse(msg);
    }

    @Test
    public void testTooManyPendingResponses() {
        final PeerId peer = mockNode();
        nodeManager.get(this.groupId, peer).getRaftOptions().setMaxReplicatorInflightMsgs(2);

        final RpcContext asyncContext = Mockito.mock(RpcContext.class);
        final AppendEntriesRequestProcessor processor = (AppendEntriesRequestProcessor) newProcessor();
        final PeerPair pair = processor.pairOf(this.peerIdStr, this.serverId);
        final PingRequest msg = TestUtils.createPingRequest();
        final PeerRequestContext ctx = processor.getOrCreatePeerRequestContext(this.groupId, pair, nodeManager);
        assertNotNull(ctx);
        processor.sendSequenceResponse(this.groupId, pair, 1, asyncContext, msg);
        processor.sendSequenceResponse(this.groupId, pair, 2, asyncContext, msg);
        processor.sendSequenceResponse(this.groupId, pair, 3, asyncContext, msg);
        Mockito.verify(asyncContext, Mockito.never()).sendResponse(msg);

        final PeerRequestContext newCtx = processor.getOrCreatePeerRequestContext(this.groupId, pair, nodeManager);
        assertNotNull(newCtx);
        assertNotSame(ctx, newCtx);
    }

}
