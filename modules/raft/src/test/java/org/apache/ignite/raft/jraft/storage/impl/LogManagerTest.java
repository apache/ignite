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
package org.apache.ignite.raft.jraft.storage.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.raft.jraft.FSMCaller;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.core.NodeMetrics;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.option.LogManagerOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.BaseStorageTest;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class LogManagerTest extends BaseStorageTest {
    private LogManagerImpl logManager;

    private ConfigurationManager confManager;

    private RaftOptions raftOptions;

    @Mock
    private FSMCaller fsmCaller;

    @Mock
    private Node node;

    private LogStorage logStorage;

    /** Disruptor for this service test. */
    private StripedDisruptor disruptor;

    private ExecutorService executor;

    @BeforeEach
    public void setup() throws Exception {
        this.confManager = new ConfigurationManager();
        this.raftOptions = new RaftOptions();
        this.logStorage = newLogStorage(raftOptions);
        this.logManager = new LogManagerImpl();
        final LogManagerOptions opts = new LogManagerOptions();

        NodeOptions nodeOptions = new NodeOptions();
        executor = JRaftUtils.createExecutor("test-executor", Utils.cpus());
        nodeOptions.setCommonExecutor(executor);
        Mockito.when(node.getOptions()).thenReturn(nodeOptions);

        opts.setConfigurationManager(this.confManager);
        opts.setLogEntryCodecFactory(LogEntryV1CodecFactory.getInstance());
        opts.setFsmCaller(this.fsmCaller);
        opts.setNode(node);
        opts.setNodeMetrics(new NodeMetrics(false));
        opts.setLogStorage(this.logStorage);
        opts.setRaftOptions(raftOptions);
        opts.setGroupId("TestSrv");
        opts.setLogManagerDisruptor(disruptor = new StripedDisruptor<>("TestLogManagerDisruptor",
            1024,
            () -> new LogManagerImpl.StableClosureEvent(),
            1));
        assertTrue(this.logManager.init(opts));
    }

    protected LogStorage newLogStorage(final RaftOptions raftOptions) {
        return new LocalLogStorage(this.path.toString(), raftOptions);
    }

    @AfterEach
    public void teardown() throws Exception {
        this.logStorage.shutdown();
        disruptor.shutdown();
        ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
    }

    @Test
    public void testEmptyState() {
        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(0, this.logManager.getLastLogIndex());
        assertNull(this.logManager.getEntry(1));
        assertEquals(0, this.logManager.getLastLogIndex(true));
        LogId lastLogId = this.logManager.getLastLogId(true);
        assertEquals(0, lastLogId.getIndex());
        lastLogId = this.logManager.getLastLogId(false);
        assertEquals(0, lastLogId.getIndex());
        assertTrue(this.logManager.checkConsistency().isOk());
    }

    @Test
    public void testAppendOneEntry() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final LogEntry entry = TestUtils.mockEntry(1, 1);
        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);
        this.logManager.appendEntries(entries, new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch.countDown();
            }
        });
        latch.await();

        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(1, this.logManager.getLastLogIndex());
        assertEquals(entry, this.logManager.getEntry(1));
        assertEquals(1, this.logManager.getLastLogIndex(true));
        LogId lastLogId = this.logManager.getLastLogId(true);
        assertEquals(1, lastLogId.getIndex());
        lastLogId = this.logManager.getLastLogId(false);
        assertEquals(1, lastLogId.getIndex());
        assertTrue(this.logManager.checkConsistency().isOk());
    }

    @Test
    public void testAppendEntries() throws Exception {
        final List<LogEntry> mockEntries = mockAddEntries();

        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(10, this.logManager.getLastLogIndex());
        for (int i = 0; i < 10; i++) {
            assertEquals(mockEntries.get(i), this.logManager.getEntry(i + 1));
        }
        assertEquals(10, this.logManager.getLastLogIndex(true));
        LogId lastLogId = this.logManager.getLastLogId(true);
        assertEquals(10, lastLogId.getIndex());
        lastLogId = this.logManager.getLastLogId(false);
        assertEquals(10, lastLogId.getIndex());
        assertTrue(this.logManager.checkConsistency().isOk());
    }

    @Test
    public void testAppendEntriesBeforeAppliedIndex() throws Exception {
        //Append 0-10
        List<LogEntry> mockEntries = TestUtils.mockEntries(10);
        for (int i = 0; i < 10; i++) {
            mockEntries.get(i).getId().setTerm(1);
        }
        final CountDownLatch latch1 = new CountDownLatch(1);
        this.logManager.appendEntries(new ArrayList<>(mockEntries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch1.countDown();
            }
        });
        latch1.await();

        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(10, this.logManager.getLastLogIndex());
        Thread.sleep(200); // waiting for setDiskId()
        this.logManager.setAppliedId(new LogId(9, 1));

        for (int i = 0; i < 10; i++) {
            assertNull(this.logManager.getEntryFromMemory(i));
        }

        // append 1-10 again, already applied, returns OK.
        final CountDownLatch latch2 = new CountDownLatch(1);
        mockEntries = TestUtils.mockEntries(10);
        mockEntries.remove(0);
        this.logManager.appendEntries(new ArrayList<>(mockEntries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch2.countDown();
            }
        });
        latch2.await();
        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(10, this.logManager.getLastLogIndex());
    }

    @Test
    public void testAppendEntresConflicts() throws Exception {
        //Append 0-10
        List<LogEntry> mockEntries = TestUtils.mockEntries(10);
        for (int i = 0; i < 10; i++) {
            mockEntries.get(i).getId().setTerm(1);
        }
        final CountDownLatch latch1 = new CountDownLatch(1);
        this.logManager.appendEntries(new ArrayList<>(mockEntries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch1.countDown();
            }
        });
        latch1.await();

        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(10, this.logManager.getLastLogIndex());

        //Append 11-20
        final CountDownLatch latch2 = new CountDownLatch(1);
        mockEntries = TestUtils.mockEntries(10);
        for (int i = 0; i < 10; i++) {
            mockEntries.get(i).getId().setIndex(11 + i);
            mockEntries.get(i).getId().setTerm(1);
        }
        this.logManager.appendEntries(new ArrayList<>(mockEntries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch2.countDown();
            }
        });
        latch2.await();

        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(20, this.logManager.getLastLogIndex());

        //Re-adds 11-30, but 15 has different term, it will truncate [14,lastIndex] logs
        mockEntries = TestUtils.mockEntries(20);
        for (int i = 0; i < 20; i++) {
            if (11 + i >= 15) {
                mockEntries.get(i).getId().setTerm(2);
            }
            else {
                mockEntries.get(i).getId().setTerm(1);
            }
            mockEntries.get(i).getId().setIndex(11 + i);
        }
        final CountDownLatch latch3 = new CountDownLatch(1);
        this.logManager.appendEntries(new ArrayList<>(mockEntries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch3.countDown();
            }
        });
        latch3.await();
        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(30, this.logManager.getLastLogIndex());

        for (int i = 0; i < 30; i++) {
            final LogEntry entry = (this.logManager.getEntry(i + 1));
            assertEquals(i + 1, entry.getId().getIndex());
            if (i + 1 >= 15) {
                assertEquals(2, entry.getId().getTerm());
            }
            else {
                assertEquals(1, entry.getId().getTerm());
            }
        }
    }

    @Test
    public void testGetConfiguration() throws Exception {
        assertTrue(this.logManager.getConfiguration(1).isEmpty());
        final List<LogEntry> entries = new ArrayList<>(2);
        final LogEntry confEntry1 = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        confEntry1.setId(new LogId(0, 1));
        confEntry1.setPeers(JRaftUtils.getConfiguration("localhost:8081,localhost:8082").listPeers());

        final LogEntry confEntry2 = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        confEntry2.setId(new LogId(0, 2));
        confEntry2.setPeers(JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083").listPeers());
        confEntry2.setOldPeers(confEntry1.getPeers());

        entries.add(confEntry1);
        entries.add(confEntry2);

        final CountDownLatch latch = new CountDownLatch(1);
        this.logManager.appendEntries(new ArrayList<>(entries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch.countDown();
            }
        });
        latch.await();
        ConfigurationEntry entry = this.logManager.getConfiguration(1);
        assertEquals("localhost:8081,localhost:8082", entry.getConf().toString());
        assertTrue(entry.getOldConf().isEmpty());

        entry = this.logManager.getConfiguration(2);
        assertEquals("localhost:8081,localhost:8082,localhost:8083", entry.getConf().toString());
        assertEquals("localhost:8081,localhost:8082", entry.getOldConf().toString());
    }

    @Test
    public void testSetAppliedId() throws Exception {
        final List<LogEntry> mockEntries = mockAddEntries();

        for (int i = 0; i < 10; i++) {
            // it's in memory
            assertEquals(mockEntries.get(i), this.logManager.getEntryFromMemory(i + 1));
        }
        Thread.sleep(200); // waiting for setDiskId()
        this.logManager.setAppliedId(new LogId(10, 10));
        for (int i = 0; i < 10; i++) {
            assertNull(this.logManager.getEntryFromMemory(i + 1));
            assertEquals(mockEntries.get(i), this.logManager.getEntry(i + 1));
        }
    }

    @Test
    public void testSetAppliedId2() throws Exception {
        final List<LogEntry> mockEntries = mockAddEntries();

        for (int i = 0; i < 10; i++) {
            // it's in memory
            assertEquals(mockEntries.get(i), this.logManager.getEntryFromMemory(i + 1));
        }
        Thread.sleep(200); // waiting for setDiskId()
        this.logManager.setAppliedId(new LogId(10, 10));
        for (int i = 0; i < 10; i++) {
            assertNull(this.logManager.getEntryFromMemory(i + 1));
            assertEquals(mockEntries.get(i), this.logManager.getEntry(i + 1));
        }
    }

    private List<LogEntry> mockAddEntries() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final List<LogEntry> mockEntries = TestUtils.mockEntries(10);
        this.logManager.appendEntries(new ArrayList<>(mockEntries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch.countDown();
            }
        });
        latch.await();
        return mockEntries;
    }

    @Test
    public void testSetSnapshot() throws Exception {
        final List<LogEntry> entries = mockAddEntries();
        RaftOutter.SnapshotMeta meta = raftOptions.getRaftMessagesFactory().snapshotMeta()
            .lastIncludedIndex(3)
            .lastIncludedTerm(2)
            .peersList(List.of("localhost:8081"))
            .build();
        this.logManager.setSnapshot(meta);
        //Still valid
        for (int i = 0; i < 10; i++) {
            assertEquals(entries.get(i), this.logManager.getEntry(i + 1));
        }
        meta = raftOptions.getRaftMessagesFactory().snapshotMeta()
            .lastIncludedIndex(5)
            .lastIncludedTerm(4)
            .peersList(List.of("localhost:8081"))
            .build();
        this.logManager.setSnapshot(meta);

        Thread.sleep(1000);
        for (int i = 0; i < 10; i++) {
            if (i > 2) {
                assertEquals(entries.get(i), this.logManager.getEntry(i + 1));
            }
            else {
                //before index=3 logs were dropped.
                assertNull(this.logManager.getEntry(i + 1));
            }
        }
        assertTrue(this.logManager.checkConsistency().isOk());
    }

    @Test
    public void testWaiter() throws Exception {
        mockAddEntries();
        final Object theArg = new Object();
        final CountDownLatch latch = new CountDownLatch(1);
        final long waitId = this.logManager.wait(10, (arg, errorCode) -> {
            assertSame(arg, theArg);
            assertEquals(0, errorCode);
            latch.countDown();
            return true;
        }, theArg);
        assertEquals(1, waitId);
        mockAddEntries();
        latch.await();
        assertFalse(this.logManager.removeWaiter(waitId));
    }

    @Test
    public void testCheckAndSetConfiguration() throws Exception {
        assertNull(this.logManager.checkAndSetConfiguration(null));
        final ConfigurationEntry entry = new ConfigurationEntry();
        entry.setId(new LogId(0, 1));
        entry.setConf(JRaftUtils.getConfiguration("localhost:8081,localhost:8082"));
        assertSame(entry, this.logManager.checkAndSetConfiguration(entry));

        testGetConfiguration();
        final ConfigurationEntry lastEntry = this.logManager.checkAndSetConfiguration(entry);
        assertNotSame(entry, lastEntry);
        assertEquals("localhost:8081,localhost:8082,localhost:8083", lastEntry.getConf().toString());
        assertEquals("localhost:8081,localhost:8082", lastEntry.getOldConf().toString());
    }

}
