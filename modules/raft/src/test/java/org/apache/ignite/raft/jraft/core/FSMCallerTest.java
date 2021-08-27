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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.raft.jraft.Iterator;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.StateMachine;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.ClosureQueueImpl;
import org.apache.ignite.raft.jraft.closure.LoadSnapshotClosure;
import org.apache.ignite.raft.jraft.closure.SaveSnapshotClosure;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.apache.ignite.raft.jraft.entity.EnumOutter.EntryType;
import org.apache.ignite.raft.jraft.entity.EnumOutter.ErrorType;
import org.apache.ignite.raft.jraft.entity.LeaderChangeContext;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RaftException;
import org.apache.ignite.raft.jraft.option.FSMCallerOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class FSMCallerTest {
    private FSMCallerImpl fsmCaller;
    private FSMCallerOptions opts;
    @Mock
    private NodeImpl node;
    @Mock
    private StateMachine fsm;
    @Mock
    private LogManager logManager;
    private ClosureQueueImpl closureQueue;

    /** Disruptor for this service test. */
    private StripedDisruptor disruptor;

    private ExecutorService executor;

    @BeforeEach
    public void setup() {
        this.fsmCaller = new FSMCallerImpl();
        NodeOptions options = new NodeOptions();
        executor = JRaftUtils.createExecutor("test-executor-", Utils.cpus());
        options.setCommonExecutor(executor);
        this.closureQueue = new ClosureQueueImpl(options);
        opts = new FSMCallerOptions();
        Mockito.when(this.node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
        Mockito.when(this.node.getOptions()).thenReturn(options);
        opts.setNode(this.node);
        opts.setFsm(this.fsm);
        opts.setLogManager(this.logManager);
        opts.setBootstrapId(new LogId(10, 1));
        opts.setClosureQueue(this.closureQueue);
        opts.setRaftMessagesFactory(new RaftMessagesFactory());
        opts.setGroupId("TestSrv");
        opts.setfSMCallerExecutorDisruptor(disruptor = new StripedDisruptor<>("TestFSMDisruptor",
            1024,
            () -> new FSMCallerImpl.ApplyTask(),
            1));
        assertTrue(this.fsmCaller.init(opts));
    }

    @AfterEach
    public void teardown() throws Exception {
        if (this.fsmCaller != null) {
            this.fsmCaller.shutdown();
            this.fsmCaller.join();
            disruptor.shutdown();
        }
        ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
    }

    @Test
    public void testShutdownJoin() throws Exception {
        this.fsmCaller.shutdown();
        this.fsmCaller.join();
        this.fsmCaller = null;
    }

    @Test
    public void testOnCommittedError() throws Exception {
        Mockito.when(this.logManager.getTerm(10)).thenReturn(1L);
        Mockito.when(this.logManager.getEntry(11)).thenReturn(null);

        assertTrue(this.fsmCaller.onCommitted(11));

        this.fsmCaller.flush();
        assertEquals(10, this.fsmCaller.getLastAppliedIndex());
        Mockito.verify(this.logManager).setAppliedId(new LogId(10, 1));
        assertFalse(this.fsmCaller.getError().getStatus().isOk());
        assertEquals("Fail to get entry at index=11 while committed_index=11", this.fsmCaller.getError().getStatus()
            .getErrorMsg());
    }

    @Test
    public void testOnCommitted() throws Exception {
        final LogEntry log = new LogEntry(EntryType.ENTRY_TYPE_DATA);
        log.getId().setIndex(11);
        log.getId().setTerm(1);
        Mockito.when(this.logManager.getTerm(11)).thenReturn(1L);
        Mockito.when(this.logManager.getEntry(11)).thenReturn(log);
        final ArgumentCaptor<Iterator> itArg = ArgumentCaptor.forClass(Iterator.class);

        assertTrue(this.fsmCaller.onCommitted(11));

        this.fsmCaller.flush();
        assertEquals(this.fsmCaller.getLastAppliedIndex(), 11);
        Mockito.verify(this.fsm).onApply(itArg.capture());
        final Iterator it = itArg.getValue();
        assertFalse(it.hasNext());
        assertEquals(it.getIndex(), 12);
        Mockito.verify(this.logManager).setAppliedId(new LogId(11, 1));
        assertTrue(this.fsmCaller.getError().getStatus().isOk());
    }

    @Test
    public void testOnSnapshotLoad() throws Exception {
        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);

        final SnapshotMeta meta = opts.getRaftMessagesFactory()
            .snapshotMeta()
            .lastIncludedIndex(12)
            .lastIncludedTerm(1)
            .build();
        Mockito.when(reader.load()).thenReturn(meta);
        Mockito.when(this.fsm.onSnapshotLoad(reader)).thenReturn(true);
        final CountDownLatch latch = new CountDownLatch(1);
        this.fsmCaller.onSnapshotLoad(new LoadSnapshotClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch.countDown();
            }

            @Override
            public SnapshotReader start() {
                return reader;
            }
        });
        latch.await();
        assertEquals(this.fsmCaller.getLastAppliedIndex(), 12);
        Mockito.verify(this.fsm).onConfigurationCommitted(Mockito.any());
    }

    @Test
    public void testOnSnapshotLoadFSMError() throws Exception {
        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);

        final SnapshotMeta meta = opts.getRaftMessagesFactory()
            .snapshotMeta()
            .lastIncludedIndex(12)
            .lastIncludedTerm(1)
            .build();
        Mockito.when(reader.load()).thenReturn(meta);
        Mockito.when(this.fsm.onSnapshotLoad(reader)).thenReturn(false);
        final CountDownLatch latch = new CountDownLatch(1);
        this.fsmCaller.onSnapshotLoad(new LoadSnapshotClosure() {

            @Override
            public void run(final Status status) {
                assertFalse(status.isOk());
                assertEquals(-1, status.getCode());
                assertEquals("StateMachine onSnapshotLoad failed", status.getErrorMsg());
                latch.countDown();
            }

            @Override
            public SnapshotReader start() {
                return reader;
            }
        });
        latch.await();
        assertEquals(this.fsmCaller.getLastAppliedIndex(), 10);
    }

    @Test
    public void testOnSnapshotSaveEmptyConf() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        this.fsmCaller.onSnapshotSave(new SaveSnapshotClosure() {

            @Override
            public void run(final Status status) {
                assertFalse(status.isOk());
                assertEquals("Empty conf entry for lastAppliedIndex=10", status.getErrorMsg());
                latch.countDown();
            }

            @Override
            public SnapshotWriter start(final SnapshotMeta meta) {
                return null;
            }
        });
        latch.await();
    }

    @Test
    public void testOnSnapshotSave() throws Exception {
        final SnapshotWriter writer = Mockito.mock(SnapshotWriter.class);
        Mockito.when(this.logManager.getConfiguration(10)).thenReturn(
            TestUtils.getConfEntry("localhost:8081,localhost:8082,localhost:8083", "localhost:8081"));
        final SaveSnapshotClosure done = new SaveSnapshotClosure() {

            @Override
            public void run(final Status status) {

            }

            @Override
            public SnapshotWriter start(final SnapshotMeta meta) {
                assertEquals(10, meta.lastIncludedIndex());
                return writer;
            }
        };
        this.fsmCaller.onSnapshotSave(done);
        this.fsmCaller.flush();
        Mockito.verify(this.fsm).onSnapshotSave(writer, done);
    }

    @Test
    public void testOnLeaderStartStop() throws Exception {
        this.fsmCaller.onLeaderStart(11);
        this.fsmCaller.flush();
        Mockito.verify(this.fsm).onLeaderStart(11);

        final Status status = new Status(-1, "test");
        this.fsmCaller.onLeaderStop(status);
        this.fsmCaller.flush();
        Mockito.verify(this.fsm).onLeaderStop(status);
    }

    @Test
    public void testOnStartStopFollowing() throws Exception {
        final LeaderChangeContext ctx = new LeaderChangeContext(null, 11, Status.OK());
        this.fsmCaller.onStartFollowing(ctx);
        this.fsmCaller.flush();
        Mockito.verify(this.fsm).onStartFollowing(ctx);

        this.fsmCaller.onStopFollowing(ctx);
        this.fsmCaller.flush();
        Mockito.verify(this.fsm).onStopFollowing(ctx);
    }

    @Test
    public void testOnError() throws Exception {
        this.fsmCaller.onError(new RaftException(ErrorType.ERROR_TYPE_LOG, new Status(-1, "test")));
        this.fsmCaller.flush();
        assertFalse(this.fsmCaller.getError().getStatus().isOk());
        assertEquals(ErrorType.ERROR_TYPE_LOG, this.fsmCaller.getError().getType());
        Mockito.verify(this.node).onError(Mockito.any());
        Mockito.verify(this.fsm).onError(Mockito.any());
    }

    @Test
    public void testOnSnapshotLoadStale() throws Exception {
        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);

        final SnapshotMeta meta = opts.getRaftMessagesFactory()
            .snapshotMeta()
            .lastIncludedIndex(5)
            .lastIncludedTerm(1)
            .build();
        Mockito.when(reader.load()).thenReturn(meta);

        final CountDownLatch latch = new CountDownLatch(1);
        this.fsmCaller.onSnapshotLoad(new LoadSnapshotClosure() {

            @Override
            public void run(final Status status) {
                assertFalse(status.isOk());
                assertEquals(RaftError.ESTALE, status.getRaftError());
                latch.countDown();
            }

            @Override
            public SnapshotReader start() {
                return reader;
            }
        });
        latch.await();
        assertEquals(this.fsmCaller.getLastAppliedIndex(), 10);
    }

}
