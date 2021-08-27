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
package org.apache.ignite.raft.jraft.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.CatchUpClosure;
import org.apache.ignite.raft.jraft.core.Replicator.RequestType;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RaftException;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.ReplicatorOptions;
import org.apache.ignite.raft.jraft.rpc.AppendEntriesRequestBuilder;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosureAdapter;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.ThreadId;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ReplicatorTest {

    private ThreadId id;
    private final RaftOptions raftOptions = new RaftOptions();
    private TimerManager timerManager;
    @Mock
    private RaftClientService rpcService;
    @Mock
    private NodeImpl node;
    @Mock
    private BallotBox ballotBox;
    @Mock
    private LogManager logManager;
    @Mock
    private SnapshotStorage snapshotStorage;
    private ReplicatorOptions opts;
    private final PeerId peerId = new PeerId("localhost", 8081);
    private ExecutorService executor;

    @BeforeEach
    public void setup() {
        this.timerManager = new TimerManager(5);
        this.opts = new ReplicatorOptions();
        this.opts.setRaftRpcService(this.rpcService);
        this.opts.setPeerId(this.peerId);
        this.opts.setBallotBox(this.ballotBox);
        this.opts.setGroupId("test");
        this.opts.setTerm(1);
        this.opts.setServerId(new PeerId("localhost", 8082));
        this.opts.setNode(this.node);
        this.opts.setSnapshotStorage(this.snapshotStorage);
        this.opts.setTimerManager(this.timerManager);
        this.opts.setLogManager(this.logManager);
        this.opts.setDynamicHeartBeatTimeoutMs(100);
        this.opts.setElectionTimeoutMs(1000);

        NodeOptions options = new NodeOptions();
        executor = JRaftUtils.createExecutor("test-executor-", Utils.cpus());
        options.setCommonExecutor(executor);

        Mockito.when(this.logManager.getLastLogIndex()).thenReturn(10L);
        Mockito.when(this.logManager.getTerm(10)).thenReturn(1L);
        Mockito.when(this.rpcService.connect(this.peerId.getEndpoint())).thenReturn(true);
        Mockito.when(this.node.getNodeMetrics()).thenReturn(new NodeMetrics(true));
        Mockito.when(this.node.getOptions()).thenReturn(options);
        // mock send empty entries
        mockSendEmptyEntries();

        this.id = Replicator.start(this.opts, this.raftOptions);
    }

    private void mockSendEmptyEntries() {
        this.mockSendEmptyEntries(false);
    }

    private void mockSendEmptyEntries(final boolean isHeartbeat) {
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest(isHeartbeat);
        Mockito.when(this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(request), eq(-1), Mockito.any()))
            .thenReturn(new CompletableFuture<>());
    }

    private RpcRequests.AppendEntriesRequest createEmptyEntriesRequest() {
        return this.createEmptyEntriesRequest(false);
    }

    private RpcRequests.AppendEntriesRequest createEmptyEntriesRequest(final boolean isHeartbeat) {
        AppendEntriesRequestBuilder rb = raftOptions.getRaftMessagesFactory().appendEntriesRequest()
            .groupId("test")
            .serverId(new PeerId("localhost", 8082).toString())
            .peerId(this.peerId.toString())
            .term(1)
            .prevLogIndex(10)
            .prevLogTerm(1)
            .committedIndex(0);
        if (!isHeartbeat) {
            rb.data(ByteString.EMPTY);
        }
        return rb.build();
    }

    @AfterEach
    public void teardown() {
        this.timerManager.shutdown();
        ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
    }

    @Test
    public void testStartDestroyJoin() throws Exception {
        assertNotNull(this.id);
        final Replicator r = getReplicator();
        assertNotNull(r);
        assertNotNull(r.getRpcInFly());
        assertEquals(Replicator.RunningState.APPENDING_ENTRIES, r.statInfo.runningState);
        assertSame(r.getOpts(), this.opts);
        this.id.unlock();
        assertEquals(0, Replicator.getNextIndex(this.id));
        assertNotNull(r.getHeartbeatTimer());
        r.destroy();
        Replicator.join(this.id);
        assertTrue(r.id.isDestroyed());
    }

    @Test
    public void testMetricRemoveOnDestroy() {
        assertNotNull(this.id);
        final Replicator r = getReplicator();
        assertNotNull(r);
        assertSame(r.getOpts(), this.opts);
        Set<String> metrics = this.opts.getNode().getNodeMetrics().getMetricRegistry().getNames();
        assertEquals(6, metrics.size());
        r.destroy();
        metrics = this.opts.getNode().getNodeMetrics().getMetricRegistry().getNames();
        assertEquals(1, metrics.size());
    }

    private Replicator getReplicator() {
        return (Replicator) this.id.lock();
    }

    @Test
    public void testOnRpcReturnedRpcError() {
        testRpcReturnedError();
    }

    private Replicator testRpcReturnedError() {
        final Replicator r = getReplicator();
        assertNull(r.getBlockTimer());
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = raftOptions.getRaftMessagesFactory()
            .appendEntriesResponse()
            .success(false)
            .lastLogIndex(12)
            .term(2)
            .build();
        this.id.unlock();

        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, new Status(-1, "test error"), request,
            response, 0, 0, Utils.monotonicMs());
        assertEquals(Replicator.RunningState.BLOCKING, r.statInfo.runningState);
        assertNotNull(r.getBlockTimer());
        return r;
    }

    @Test
    public void testOnRpcReturnedRpcContinuousError() throws Exception {
        Replicator r = testRpcReturnedError();
        ScheduledFuture<?> timer = r.getBlockTimer();
        assertNotNull(timer);

        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = raftOptions.getRaftMessagesFactory()
            .appendEntriesResponse()
            .success(false)
            .lastLogIndex(12)
            .term(2)
            .build();
        r.getInflights().add(new Replicator.Inflight(RequestType.AppendEntries, r.getNextSendIndex(), 0, 0, 1, null));
        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, new Status(-1, "test error"), request,
            response, 1, 1, Utils.monotonicMs());
        assertEquals(Replicator.RunningState.BLOCKING, r.statInfo.runningState);
        assertNotNull(r.getBlockTimer());
        // the same timer
        assertSame(timer, r.getBlockTimer());

        Thread.sleep(r.getOpts().getDynamicHeartBeatTimeoutMs() * 2);
        r.getInflights().add(new Replicator.Inflight(RequestType.AppendEntries, r.getNextSendIndex(), 0, 0, 1, null));
        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, new Status(-1, "test error"), request,
            response, 1, 2, Utils.monotonicMs());
        assertEquals(Replicator.RunningState.BLOCKING, r.statInfo.runningState);
        assertNotNull(r.getBlockTimer());
        // the same timer
        assertNotSame(timer, r.getBlockTimer());
    }

    @Test
    public void testOnRpcReturnedTermMismatch() {
        final Replicator r = getReplicator();
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = raftOptions.getRaftMessagesFactory()
            .appendEntriesResponse()
            .success(false)
            .lastLogIndex(12)
            .term(2)
            .build();
        this.id.unlock();

        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, Status.OK(), request, response, 0, 0,
            Utils.monotonicMs());
        Mockito.verify(this.node).increaseTermTo(
            2,
            new Status(RaftError.EHIGHERTERMRESPONSE, "Leader receives higher term heartbeat_response from peer:%s",
                this.peerId));
        assertTrue(r.id.isDestroyed());
    }

    @Test
    public void testOnRpcReturnedMoreLogs() {
        final Replicator r = getReplicator();
        assertEquals(11, r.getRealNextIndex());
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = raftOptions.getRaftMessagesFactory()
            .appendEntriesResponse()
            .success(false)
            .lastLogIndex(12)
            .term(1)
            .build();
        this.id.unlock();
        final Future<Message> rpcInFly = r.getRpcInFly();
        assertNotNull(rpcInFly);

        Mockito.when(this.logManager.getTerm(9)).thenReturn(1L);
        final RpcRequests.AppendEntriesRequest newReq = raftOptions.getRaftMessagesFactory()
            .appendEntriesRequest()
            .groupId("test")
            .serverId(new PeerId("localhost", 8082).toString())
            .peerId(this.peerId.toString())
            .term(1)
            .prevLogIndex(9)
            .data(ByteString.EMPTY)
            .prevLogTerm(1)
            .committedIndex(0)
            .build();
        Mockito.when(this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(newReq), eq(-1), Mockito.any()))
            .thenReturn(new CompletableFuture<>());

        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, Status.OK(), request, response, 0, 0,
            Utils.monotonicMs());

        assertNotNull(r.getRpcInFly());
        assertNotSame(r.getRpcInFly(), rpcInFly);
        assertEquals(Replicator.RunningState.APPENDING_ENTRIES, r.statInfo.runningState);
        this.id.unlock();
        assertEquals(0, Replicator.getNextIndex(this.id));
        assertEquals(10, r.getRealNextIndex());
    }

    @Test
    public void testOnRpcReturnedLessLogs() {
        final Replicator r = getReplicator();
        assertEquals(11, r.getRealNextIndex());
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = raftOptions.getRaftMessagesFactory()
            .appendEntriesResponse()
            .success(false)
            .lastLogIndex(8)
            .term(1)
            .build();
        this.id.unlock();
        final Future<Message> rpcInFly = r.getRpcInFly();
        assertNotNull(rpcInFly);

        Mockito.when(this.logManager.getTerm(8)).thenReturn(1L);
        final RpcRequests.AppendEntriesRequest newReq = raftOptions.getRaftMessagesFactory()
            .appendEntriesRequest()
            .groupId("test")
            .serverId(new PeerId("localhost", 8082).toString())
            .peerId(this.peerId.toString())
            .term(1)
            .prevLogIndex(8)
            .prevLogTerm(1)
            .data(ByteString.EMPTY)
            .committedIndex(0)
            .build();
        Mockito.when(this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(newReq), eq(-1), Mockito.any()))
            .thenReturn(new CompletableFuture<>());

        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, Status.OK(), request, response, 0, 0,
            Utils.monotonicMs());

        assertNotNull(r.getRpcInFly());
        assertNotSame(r.getRpcInFly(), rpcInFly);
        assertEquals(Replicator.RunningState.APPENDING_ENTRIES, r.statInfo.runningState);
        this.id.unlock();
        assertEquals(0, Replicator.getNextIndex(this.id));
        assertEquals(9, r.getRealNextIndex());
    }

    @Test
    public void testOnRpcReturnedWaitMoreEntries() throws Exception {
        final Replicator r = getReplicator();
        assertEquals(-1, r.getWaitId());

        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = raftOptions.getRaftMessagesFactory()
            .appendEntriesResponse()
            .success(true)
            .lastLogIndex(10)
            .term(1)
            .build();
        this.id.unlock();
        Mockito.when(this.logManager.wait(eq(10L), Mockito.any(), same(this.id))).thenReturn(99L);

        final CountDownLatch latch = new CountDownLatch(1);
        Replicator.waitForCaughtUp(this.id, 1, System.currentTimeMillis() + 5000, new CatchUpClosure() {
            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch.countDown();
            }
        }, node.getOptions().getCommonExecutor());

        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, Status.OK(), request, response, 0, 0,
            Utils.monotonicMs());

        assertEquals(Replicator.RunningState.IDLE, r.statInfo.runningState);
        this.id.unlock(); // TODO asch fix bad unlock IGNITE-14832
        assertEquals(11, Replicator.getNextIndex(this.id));
        assertEquals(99, r.getWaitId());
        latch.await(); //make sure catch up closure is invoked.
    }

    @Test
    public void testStop() {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertNotNull(r.getHeartbeatTimer());
        assertNotNull(r.getRpcInFly());
        Replicator.stop(this.id);
        assertTrue(r.id.isDestroyed());
        assertNull(r.getHeartbeatTimer());
        assertNull(r.getRpcInFly());
    }

    @Test
    public void testSetErrorStop() {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertNotNull(r.getHeartbeatTimer());
        assertNotNull(r.getRpcInFly());
        this.id.setError(RaftError.ESTOP.getNumber());
        this.id.unlock();
        assertTrue(r.id.isDestroyed());
        assertNull(r.getHeartbeatTimer());
        assertNull(r.getRpcInFly());
    }

    @Test
    public void testContinueSendingTimeout() throws Exception {
        testOnRpcReturnedWaitMoreEntries();
        final Replicator r = getReplicator();
        this.id.unlock();
        mockSendEmptyEntries();
        final Future<Message> rpcInFly = r.getRpcInFly();
        assertNotNull(rpcInFly);
        assertTrue(Replicator.continueSending(this.id, RaftError.ETIMEDOUT.getNumber()));
        assertNotNull(r.getRpcInFly());
        assertNotSame(rpcInFly, r.getRpcInFly());
    }

    @Test
    public void testContinueSendingEntries() throws Exception {
        testOnRpcReturnedWaitMoreEntries();
        final Replicator r = getReplicator();
        this.id.unlock();
        mockSendEmptyEntries();
        final Future<Message> rpcInFly = r.getRpcInFly();
        assertNotNull(rpcInFly);

        final AppendEntriesRequestBuilder rb = raftOptions.getRaftMessagesFactory()
            .appendEntriesRequest()
            .groupId("test")
            .serverId(new PeerId("localhost", 8082).toString())
            .peerId(this.peerId.toString())
            .term(1)
            .prevLogIndex(10)
            .prevLogTerm(1)
            .committedIndex(0);

        int totalDataLen = 0;
        List<RaftOutter.EntryMeta> entries = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            totalDataLen += i;
            final LogEntry value = new LogEntry();
            value.setData(ByteBuffer.allocate(i));
            value.setType(EnumOutter.EntryType.ENTRY_TYPE_DATA);
            value.setId(new LogId(11 + i, 1));
            Mockito.when(this.logManager.getEntry(11 + i)).thenReturn(value);
            entries.add(raftOptions.getRaftMessagesFactory()
                .entryMeta()
                .term(1)
                .type(EnumOutter.EntryType.ENTRY_TYPE_DATA)
                .dataLen(i)
                .build());
        }

        rb.entriesList(entries);

        rb.data(new ByteString(new byte[totalDataLen]));

        final RpcRequests.AppendEntriesRequest request = rb.build();
        Mockito.when(this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(request), eq(-1), Mockito.any()))
            .thenAnswer(new Answer<Future>() {
                @Override public Future answer(InvocationOnMock invocation) throws Throwable {
                    return new CompletableFuture<>();
                }
            });

        assertEquals(11, r.statInfo.firstLogIndex);
        assertEquals(10, r.statInfo.lastLogIndex);
        Mockito.when(this.logManager.getTerm(20)).thenReturn(1L);
        assertTrue(Replicator.continueSending(this.id, 0));
        assertNotNull(r.getRpcInFly());
        assertNotSame(rpcInFly, r.getRpcInFly());
        assertEquals(11, r.statInfo.firstLogIndex);
        assertEquals(20, r.statInfo.lastLogIndex);
        assertEquals(0, r.getWaitId());
        assertEquals(Replicator.RunningState.IDLE, r.statInfo.runningState);
    }

    @Test
    public void testSetErrorTimeout() throws Exception {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertNull(r.getHeartbeatInFly());
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest(true);
        Mockito.when(
            this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(request),
                eq(this.opts.getElectionTimeoutMs() / 2), Mockito.any())).thenReturn(new CompletableFuture<>());
        this.id.setError(RaftError.ETIMEDOUT.getNumber());
        Thread.sleep(this.opts.getElectionTimeoutMs() + 1000);
        assertNotNull(r.getHeartbeatInFly());
    }

    @Test
    public void testOnHeartbeatReturnedRpcError() {
        final Replicator r = getReplicator();
        this.id.unlock();
        final ScheduledFuture<?> timer = r.getHeartbeatTimer();
        assertNotNull(timer);
        Replicator.onHeartbeatReturned(this.id, new Status(-1, "test"), createEmptyEntriesRequest(), null,
            Utils.monotonicMs());
        assertNotNull(r.getHeartbeatTimer());
        assertNotSame(timer, r.getHeartbeatTimer());
    }

    @Test
    public void testOnHeartbeatReturnedOK() {
        final Replicator r = getReplicator();
        this.id.unlock();
        final ScheduledFuture<?> timer = r.getHeartbeatTimer();
        assertNotNull(timer);
        final RpcRequests.AppendEntriesResponse response = raftOptions.getRaftMessagesFactory()
            .appendEntriesResponse()
            .success(false)
            .lastLogIndex(10)
            .term(1)
            .build();
        Replicator
            .onHeartbeatReturned(this.id, Status.OK(), createEmptyEntriesRequest(), response, Utils.monotonicMs());
        assertNotNull(r.getHeartbeatTimer());
        assertNotSame(timer, r.getHeartbeatTimer());
    }

    @Test
    public void testOnHeartbeatReturnedTermMismatch() {
        final Replicator r = getReplicator();
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = raftOptions.getRaftMessagesFactory()
            .appendEntriesResponse()
            .success(false)
            .lastLogIndex(12)
            .term(2)
            .build();
        this.id.unlock();

        Replicator.onHeartbeatReturned(this.id, Status.OK(), request, response, Utils.monotonicMs());
        Mockito.verify(this.node).increaseTermTo(
            2,
            new Status(RaftError.EHIGHERTERMRESPONSE, "Leader receives higher term heartbeat_response from peer:%s",
                this.peerId));
        assertTrue(r.id.isDestroyed());
    }

    @Test
    public void testTransferLeadership() {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertEquals(0, r.getTimeoutNowIndex());
        assertTrue(Replicator.transferLeadership(this.id, 11));
        assertEquals(11, r.getTimeoutNowIndex());
        assertNull(r.getTimeoutNowInFly());
    }

    @Test
    public void testStopTransferLeadership() {
        testTransferLeadership();
        Replicator.stopTransferLeadership(this.id);
        final Replicator r = getReplicator();
        this.id.unlock();
        assertEquals(0, r.getTimeoutNowIndex());
        assertNull(r.getTimeoutNowInFly());
    }

    @Test
    public void testTransferLeadershipSendTimeoutNow() {
        final Replicator r = getReplicator();
        this.id.unlock();
        r.setHasSucceeded();
        assertEquals(0, r.getTimeoutNowIndex());
        assertNull(r.getTimeoutNowInFly());

        final RpcRequests.TimeoutNowRequest request = createTimeoutnowRequest();
        Mockito.when(
            this.rpcService.timeoutNow(eq(this.opts.getPeerId().getEndpoint()), eq(request), eq(-1),
                Mockito.any())).thenReturn(new CompletableFuture<>());

        assertTrue(Replicator.transferLeadership(this.id, 10));
        assertEquals(0, r.getTimeoutNowIndex());
        assertNotNull(r.getTimeoutNowInFly());
    }

    @Test
    public void testSendHeartbeat() {
        final Replicator r = getReplicator();
        this.id.unlock();

        assertNull(r.getHeartbeatInFly());
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest(true);
        Mockito.when(
            this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(request),
                eq(this.opts.getElectionTimeoutMs() / 2), Mockito.any())).thenAnswer(new Answer<Future>() {
            @Override public Future answer(InvocationOnMock invocation) throws Throwable {
                return new CompletableFuture<>();
            }
        });
        Replicator.sendHeartbeat(this.id, new RpcResponseClosureAdapter<RpcRequests.AppendEntriesResponse>() {
            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
            }
        }, node.getOptions().getCommonExecutor());

        assertNotNull(r.getHeartbeatInFly());

        assertSame(r, this.id.lock());
        this.id.unlock();
    }

    @Test
    public void testSendTimeoutNowAndStop() {
        final Replicator r = getReplicator();
        this.id.unlock();
        r.setHasSucceeded();
        assertEquals(0, r.getTimeoutNowIndex());
        assertNull(r.getTimeoutNowInFly());
        assertTrue(Replicator.sendTimeoutNowAndStop(this.id, 10));
        assertEquals(0, r.getTimeoutNowIndex());
        assertNull(r.getTimeoutNowInFly());
        final RpcRequests.TimeoutNowRequest request = createTimeoutnowRequest();
        Mockito.verify(this.rpcService).timeoutNow(eq(this.opts.getPeerId().getEndpoint()), eq(request),
            eq(10), Mockito.any());
    }

    private RpcRequests.TimeoutNowRequest createTimeoutnowRequest() {
        return raftOptions.getRaftMessagesFactory()
            .timeoutNowRequest()
            .term(this.opts.getTerm())
            .groupId(this.opts.getGroupId())
            .serverId(this.opts.getServerId().toString())
            .peerId(this.opts.getPeerId().toString())
            .build();
    }

    @Test
    public void testOnTimeoutNowReturnedRpcErrorAndStop() {
        final Replicator r = getReplicator();
        final RpcRequests.TimeoutNowRequest request = createTimeoutnowRequest();
        this.id.unlock();

        Replicator.onTimeoutNowReturned(this.id, new Status(-1, "test"), request, null, true);
        assertTrue(r.id.isDestroyed());
    }

    @Test
    public void testInstallSnapshotNoReader() {
        final Replicator r = getReplicator();
        this.id.unlock();

        final Future<Message> rpcInFly = r.getRpcInFly();
        assertNotNull(rpcInFly);
        r.installSnapshot();
        final ArgumentCaptor<RaftException> errArg = ArgumentCaptor.forClass(RaftException.class);
        Mockito.verify(this.node).onError(errArg.capture());
        assertEquals(RaftError.EIO, errArg.getValue().getStatus().getRaftError());
        assertEquals("Fail to open snapshot", errArg.getValue().getStatus().getErrorMsg());
    }

    @Test
    public void testInstallSnapshot() {
        final Replicator r = getReplicator();
        this.id.unlock();

        final Future<Message> rpcInFly = r.getRpcInFly();
        assertNotNull(rpcInFly);
        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);
        Mockito.when(this.snapshotStorage.open()).thenReturn(reader);
        final String uri = "remote://localhost:8081/99";
        Mockito.when(reader.generateURIForCopy()).thenReturn(uri);
        final RaftOutter.SnapshotMeta meta = raftOptions.getRaftMessagesFactory().snapshotMeta()
            .lastIncludedIndex(11)
            .lastIncludedTerm(1)
            .build();
        Mockito.when(reader.load()).thenReturn(meta);

        assertEquals(0, r.statInfo.lastLogIncluded);
        assertEquals(0, r.statInfo.lastTermIncluded);

        final RpcRequests.InstallSnapshotRequest req = raftOptions.getRaftMessagesFactory()
            .installSnapshotRequest()
            .term(this.opts.getTerm())
            .groupId(this.opts.getGroupId())
            .serverId(this.opts.getServerId().toString())
            .peerId(this.opts.getPeerId().toString())
            .meta(meta)
            .uri(uri)
            .build();

        Mockito.when(
            this.rpcService.installSnapshot(eq(this.opts.getPeerId().getEndpoint()), eq(req),
                Mockito.any())).thenReturn(new CompletableFuture<>());

        r.installSnapshot();
        assertNotNull(r.getRpcInFly());
        assertNotSame(r.getRpcInFly(), rpcInFly);
        assertEquals(Replicator.RunningState.INSTALLING_SNAPSHOT, r.statInfo.runningState);
        assertEquals(11, r.statInfo.lastLogIncluded);
        assertEquals(1, r.statInfo.lastTermIncluded);
    }

    @Test
    public void testOnTimeoutNowReturnedTermMismatch() {
        final Replicator r = getReplicator();
        this.id.unlock();
        final RpcRequests.TimeoutNowRequest request = createTimeoutnowRequest();
        final RpcRequests.TimeoutNowResponse response = raftOptions.getRaftMessagesFactory()
            .timeoutNowResponse()
            .success(false)
            .term(12)
            .build();
        this.id.unlock();

        Replicator.onTimeoutNowReturned(this.id, Status.OK(), request, response, false);
        Mockito.verify(this.node).increaseTermTo(
            12,
            new Status(RaftError.EHIGHERTERMRESPONSE, "Leader receives higher term timeout_now_response from peer:%s",
                this.peerId));
        assertTrue(r.id.isDestroyed());
    }

    @Test
    public void testOnInstallSnapshotReturned() {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertNull(r.getBlockTimer());

        final RpcRequests.InstallSnapshotRequest request = createInstallSnapshotRequest();
        final RpcRequests.InstallSnapshotResponse response = raftOptions.getRaftMessagesFactory()
            .installSnapshotResponse()
            .success(true)
            .term(1)
            .build();
        assertEquals(-1, r.getWaitId());
        Mockito.when(this.logManager.getTerm(11)).thenReturn(1L);
        Replicator.onRpcReturned(this.id, Replicator.RequestType.Snapshot, Status.OK(), request, response, 0, 0, -1);
        assertNull(r.getBlockTimer());
        assertEquals(0, r.getWaitId());
    }

    @Test
    public void testOnInstallSnapshotReturnedRpcError() {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertNull(r.getBlockTimer());

        final RpcRequests.InstallSnapshotRequest request = createInstallSnapshotRequest();
        final RpcRequests.InstallSnapshotResponse response = raftOptions.getRaftMessagesFactory()
            .installSnapshotResponse()
            .success(true)
            .term(1)
            .build();
        assertEquals(-1, r.getWaitId());
        Mockito.lenient().when(this.logManager.getTerm(11)).thenReturn(1L);
        Replicator.onRpcReturned(this.id, Replicator.RequestType.Snapshot, new Status(-1, "test"), request, response,
            0, 0, -1);
        assertNotNull(r.getBlockTimer());
        assertEquals(-1, r.getWaitId());
    }

    @Test
    public void testOnInstallSnapshotReturnedFailure() {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertNull(r.getBlockTimer());

        final RpcRequests.InstallSnapshotRequest request = createInstallSnapshotRequest();
        final RpcRequests.InstallSnapshotResponse response = raftOptions.getRaftMessagesFactory()
            .installSnapshotResponse()
            .success(false)
            .term(1)
            .build();
        assertEquals(-1, r.getWaitId());
        Mockito.lenient().when(this.logManager.getTerm(11)).thenReturn(1L);
        Replicator.onRpcReturned(this.id, Replicator.RequestType.Snapshot, Status.OK(), request, response, 0, 0, -1);
        assertNotNull(r.getBlockTimer());
        assertEquals(-1, r.getWaitId());
    }

    @Test
    public void testOnRpcReturnedOutOfOrder() {
        final Replicator r = getReplicator();
        assertEquals(-1, r.getWaitId());

        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = raftOptions.getRaftMessagesFactory()
            .appendEntriesResponse()
            .success(true)
            .lastLogIndex(10)
            .term(1)
            .build();
        assertNull(r.getBlockTimer());
        this.id.unlock();

        assertTrue(r.getPendingResponses().isEmpty());
        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, Status.OK(), request, response, 1, 0,
            Utils.monotonicMs());
        assertEquals(1, r.getPendingResponses().size());
        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, Status.OK(), request, response, 0, 0,
            Utils.monotonicMs());
        assertTrue(r.getPendingResponses().isEmpty());
        assertEquals(0, r.getWaitId());
        assertEquals(11, r.getRealNextIndex());
        assertEquals(1, r.getRequiredNextSeq());
    }

    private void mockSendEntries(@SuppressWarnings("SameParameterValue") final int n) {
        final RpcRequests.AppendEntriesRequest request = createEntriesRequest(n);
        Mockito.lenient().when(this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(request), eq(-1), Mockito.any()))
            .thenReturn(new CompletableFuture<>());
    }

    private RpcRequests.AppendEntriesRequest createEntriesRequest(final int n) {
        final AppendEntriesRequestBuilder rb = raftOptions.getRaftMessagesFactory()
            .appendEntriesRequest()
            .groupId("test")
            .serverId(new PeerId("localhost", 8082).toString())
            .peerId(this.peerId.toString())
            .term(1)
            .prevLogIndex(10)
            .prevLogTerm(1)
            .committedIndex(0);

        List<RaftOutter.EntryMeta> entries = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            final LogEntry log = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_DATA);
            log.setData(ByteBuffer.wrap(new byte[i]));
            log.setId(new LogId(i + 11, 1));
            Mockito.when(this.logManager.getEntry(i + 11)).thenReturn(log);
            Mockito.when(this.logManager.getTerm(i + 11)).thenReturn(1L);
            entries.add(raftOptions.getRaftMessagesFactory()
                .entryMeta()
                .dataLen(i)
                .term(1)
                .type(EnumOutter.EntryType.ENTRY_TYPE_DATA)
                .build());
        }

        rb.entriesList(entries);

        return rb.build();
    }

    @Test
    public void testGetNextSendIndex() {
        final Replicator r = getReplicator();
        assertEquals(-1, r.getNextSendIndex());
        r.resetInflights();
        assertEquals(11, r.getNextSendIndex());
        mockSendEntries(3);
        r.sendEntries();
        assertEquals(14, r.getNextSendIndex());
    }

    private RpcRequests.InstallSnapshotRequest createInstallSnapshotRequest() {
        final String uri = "remote://localhost:8081/99";
        final RaftOutter.SnapshotMeta meta = raftOptions.getRaftMessagesFactory()
            .snapshotMeta()
            .lastIncludedIndex(11)
            .lastIncludedTerm(1)
            .build();
        return raftOptions.getRaftMessagesFactory().installSnapshotRequest()
            .term(this.opts.getTerm())
            .groupId(this.opts.getGroupId())
            .serverId(this.opts.getServerId().toString())
            .peerId(this.opts.getPeerId().toString())
            .meta(meta)
            .uri(uri)
            .build();
    }
}
