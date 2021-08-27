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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.raft.jraft.FSMCaller;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.ReadIndexClosure;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.ReadIndexState;
import org.apache.ignite.raft.jraft.entity.ReadIndexStatus;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.ReadOnlyServiceOptions;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexRequest;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosure;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Bytes;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ReadOnlyServiceTest {
    private ReadOnlyServiceImpl readOnlyServiceImpl;

    private RaftMessagesFactory msgFactory;

    @Mock
    private NodeImpl node;

    @Mock
    private FSMCaller fsmCaller;

    /** Disruptor for this service test. */
    private StripedDisruptor disruptor;

    private List<ExecutorService> executors = new ArrayList<>();

    private Scheduler scheduler;

    @BeforeEach
    public void setup() {
        this.readOnlyServiceImpl = new ReadOnlyServiceImpl();
        RaftOptions raftOptions = new RaftOptions();
        this.msgFactory = raftOptions.getRaftMessagesFactory();
        final ReadOnlyServiceOptions opts = new ReadOnlyServiceOptions();
        opts.setFsmCaller(this.fsmCaller);
        opts.setNode(this.node);
        opts.setRaftOptions(raftOptions);
        opts.setGroupId("TestSrv");
        opts.setReadOnlyServiceDisruptor(disruptor = new StripedDisruptor<>("TestReadOnlyServiceDisruptor",
            1024,
            () -> new ReadOnlyServiceImpl.ReadIndexEvent(),
            1));
        NodeOptions nodeOptions = new NodeOptions();
        ExecutorService executor = JRaftUtils.createExecutor("test-executor", Utils.cpus());
        executors.add(executor);
        nodeOptions.setCommonExecutor(executor);
        ExecutorService clientExecutor = JRaftUtils.createClientExecutor(nodeOptions, "unittest");
        executors.add(clientExecutor);
        nodeOptions.setClientExecutor(clientExecutor);
        Scheduler scheduler = JRaftUtils.createScheduler(nodeOptions);
        this.scheduler = scheduler;
        nodeOptions.setScheduler(scheduler);
        Mockito.when(this.node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
        Mockito.when(this.node.getGroupId()).thenReturn("test");
        Mockito.when(this.node.getTimerManager()).thenReturn(nodeOptions.getScheduler());
        Mockito.when(this.node.getOptions()).thenReturn(nodeOptions);
        Mockito.when(this.node.getNodeId()).thenReturn(new NodeId("test", new PeerId("localhost:8081", 0)));
        Mockito.when(this.node.getServerId()).thenReturn(new PeerId("localhost:8081", 0));
        assertTrue(this.readOnlyServiceImpl.init(opts));
    }

    @AfterEach
    public void teardown() throws Exception {
        this.readOnlyServiceImpl.shutdown();
        this.readOnlyServiceImpl.join();
        disruptor.shutdown();
        executors.forEach(ExecutorServiceHelper::shutdownAndAwaitTermination);
        scheduler.shutdown();
    }

    @Test
    public void testAddRequest() throws Exception {
        final byte[] requestContext = TestUtils.getRandomBytes();
        this.readOnlyServiceImpl.addRequest(requestContext, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {

            }
        });
        this.readOnlyServiceImpl.flush();
        Mockito.verify(this.node).handleReadIndexRequest(Mockito.argThat(new ArgumentMatcher<ReadIndexRequest>() {
            @Override public boolean matches(ReadIndexRequest argument) {
                if (argument != null) {
                    final ReadIndexRequest req = (ReadIndexRequest) argument;
                    return "test".equals(req.groupId()) && "localhost:8081:0".equals(req.serverId())
                        && Utils.size(req.entriesList()) == 1
                        && Arrays.equals(requestContext, req.entriesList().get(0).toByteArray());
                }
                return false;
            }

        }), Mockito.any());
    }

    @Test
    public void testAddRequestOnResponsePending() throws Exception {
        final byte[] requestContext = TestUtils.getRandomBytes();
        final CountDownLatch latch = new CountDownLatch(1);
        this.readOnlyServiceImpl.addRequest(requestContext, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                assertTrue(status.isOk());
                assertEquals(index, 1);
                assertArrayEquals(reqCtx, requestContext);
                latch.countDown();
            }
        });
        this.readOnlyServiceImpl.flush();

        final ArgumentCaptor<RpcResponseClosure> closureCaptor = ArgumentCaptor.forClass(RpcResponseClosure.class);

        Mockito.verify(this.node).handleReadIndexRequest(Mockito.argThat(new ArgumentMatcher<ReadIndexRequest>() {
            @Override public boolean matches(ReadIndexRequest argument) {
                if (argument != null) {
                    final ReadIndexRequest req = (ReadIndexRequest) argument;
                    return "test".equals(req.groupId()) && "localhost:8081:0".equals(req.serverId())
                        && Utils.size(req.entriesList()) == 1
                        && Arrays.equals(requestContext, req.entriesList().get(0).toByteArray());
                }
                return false;
            }

        }), closureCaptor.capture());

        final RpcResponseClosure closure = closureCaptor.getValue();

        assertNotNull(closure);

        closure.setResponse(msgFactory.readIndexResponse().index(1).success(true).build());
        assertTrue(this.readOnlyServiceImpl.getPendingNotifyStatus().isEmpty());
        closure.run(Status.OK());
        assertEquals(this.readOnlyServiceImpl.getPendingNotifyStatus().size(), 1);
        this.readOnlyServiceImpl.onApplied(2);
        latch.await();
    }

    @Test
    public void testAddRequestOnResponseFailure() throws Exception {
        Mockito.lenient().when(this.fsmCaller.getLastAppliedIndex()).thenReturn(2L);

        final byte[] requestContext = TestUtils.getRandomBytes();
        final CountDownLatch latch = new CountDownLatch(1);
        this.readOnlyServiceImpl.addRequest(requestContext, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                assertFalse(status.isOk());
                assertEquals(index, -1);
                assertArrayEquals(reqCtx, requestContext);
                latch.countDown();
            }
        });
        this.readOnlyServiceImpl.flush();

        final ArgumentCaptor<RpcResponseClosure> closureCaptor = ArgumentCaptor.forClass(RpcResponseClosure.class);

        Mockito.verify(this.node).handleReadIndexRequest(Mockito.argThat(new ArgumentMatcher<ReadIndexRequest>() {
            @Override public boolean matches(ReadIndexRequest argument) {
                if (argument != null) {
                    final ReadIndexRequest req = (ReadIndexRequest) argument;
                    return "test".equals(req.groupId()) && "localhost:8081:0".equals(req.serverId())
                        && Utils.size(req.entriesList()) == 1
                        && Arrays.equals(requestContext, req.entriesList().get(0).toByteArray());
                }
                return false;
            }

        }), closureCaptor.capture());

        final RpcResponseClosure closure = closureCaptor.getValue();

        assertNotNull(closure);

        closure.setResponse(msgFactory.readIndexResponse().index(1).success(true).build());
        closure.run(new Status(-1, "test"));
        latch.await();
    }

    @Test
    public void testAddRequestOnResponseSuccess() throws Exception {

        Mockito.when(this.fsmCaller.getLastAppliedIndex()).thenReturn(2L);

        final byte[] requestContext = TestUtils.getRandomBytes();
        final CountDownLatch latch = new CountDownLatch(1);
        this.readOnlyServiceImpl.addRequest(requestContext, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                assertTrue(status.isOk());
                assertEquals(index, 1);
                assertArrayEquals(reqCtx, requestContext);
                latch.countDown();
            }
        });
        this.readOnlyServiceImpl.flush();

        final ArgumentCaptor<RpcResponseClosure> closureCaptor = ArgumentCaptor.forClass(RpcResponseClosure.class);

        Mockito.verify(this.node).handleReadIndexRequest(Mockito.argThat(new ArgumentMatcher<ReadIndexRequest>() {
            @Override public boolean matches(ReadIndexRequest argument) {
                if (argument != null) {
                    final ReadIndexRequest req = (ReadIndexRequest) argument;
                    return "test".equals(req.groupId()) && "localhost:8081:0".equals(req.serverId())
                        && Utils.size(req.entriesList()) == 1
                        && Arrays.equals(requestContext, req.entriesList().get(0).toByteArray());
                }
                return false;
            }

        }), closureCaptor.capture());

        final RpcResponseClosure closure = closureCaptor.getValue();

        assertNotNull(closure);

        closure.setResponse(msgFactory.readIndexResponse().index(1).success(true).build());
        closure.run(Status.OK());
        latch.await();
    }

    @Test
    public void testOnApplied() throws Exception {
        final ArrayList<ReadIndexState> states = new ArrayList<>();
        final byte[] reqContext = TestUtils.getRandomBytes();
        final CountDownLatch latch = new CountDownLatch(1);
        final ReadIndexState state = new ReadIndexState(new Bytes(reqContext), new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                assertTrue(status.isOk());
                assertEquals(index, 1);
                assertArrayEquals(reqCtx, reqContext);
                latch.countDown();
            }
        }, Utils.monotonicMs());
        state.setIndex(1);
        states.add(state);
        final ReadIndexStatus readIndexStatus = new ReadIndexStatus(states, null, 1);
        this.readOnlyServiceImpl.getPendingNotifyStatus().put(1L, Arrays.asList(readIndexStatus));

        this.readOnlyServiceImpl.onApplied(2);
        latch.await();
        assertTrue(this.readOnlyServiceImpl.getPendingNotifyStatus().isEmpty());
    }
}
