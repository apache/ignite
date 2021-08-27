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

import java.util.concurrent.ExecutorService;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.FSMCaller;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.ClosureQueueImpl;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.BallotBoxOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(MockitoExtension.class)
public class BallotBoxTest {
    private BallotBox box;
    @Mock
    private FSMCaller waiter;
    private ClosureQueueImpl closureQueue;
    private ExecutorService executor;

    @BeforeEach
    public void setup() {
        BallotBoxOptions opts = new BallotBoxOptions();
        NodeOptions options = new NodeOptions();
        executor = JRaftUtils.createExecutor("test-executor-", Utils.cpus());
        options.setCommonExecutor(executor);
        this.closureQueue = new ClosureQueueImpl(options);
        opts.setClosureQueue(this.closureQueue);
        opts.setWaiter(this.waiter);
        box = new BallotBox();
        assertTrue(box.init(opts));
    }

    @AfterEach
    public void teardown() {
        box.shutdown();
        ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
    }

    @Test
    public void testResetPendingIndex() {
        assertEquals(0, closureQueue.getFirstIndex());
        assertEquals(0, box.getPendingIndex());
        assertTrue(box.resetPendingIndex(1));
        assertEquals(1, closureQueue.getFirstIndex());
        assertEquals(1, box.getPendingIndex());
    }

    @Test
    public void testAppendPendingTask() {
        assertTrue(this.box.getPendingMetaQueue().isEmpty());
        assertTrue(this.closureQueue.getQueue().isEmpty());
        assertFalse(this.box.appendPendingTask(
            JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083"),
            JRaftUtils.getConfiguration("localhost:8081"), new Closure() {

                @Override
                public void run(Status status) {

                }
            }));
        assertTrue(box.resetPendingIndex(1));
        assertTrue(this.box.appendPendingTask(
            JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083"),
            JRaftUtils.getConfiguration("localhost:8081"), new Closure() {

                @Override
                public void run(Status status) {

                }
            }));

        assertEquals(1, this.box.getPendingMetaQueue().size());
        assertEquals(1, this.closureQueue.getQueue().size());
    }

    @Test
    public void testClearPendingTasks() {
        testAppendPendingTask();
        this.box.clearPendingTasks();
        assertTrue(this.box.getPendingMetaQueue().isEmpty());
        assertTrue(this.closureQueue.getQueue().isEmpty());
        assertEquals(0, closureQueue.getFirstIndex());
    }

    @Test
    public void testCommitAt() {
        assertFalse(this.box.commitAt(1, 3, new PeerId("localhost", 8081)));
        assertTrue(box.resetPendingIndex(1));
        assertTrue(this.box.appendPendingTask(
            JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083"),
            JRaftUtils.getConfiguration("localhost:8081"), new Closure() {
                @Override
                public void run(Status status) {

                }
            }));
        assertEquals(0, this.box.getLastCommittedIndex());
        try {
            this.box.commitAt(1, 3, new PeerId("localhost", 8081));
            fail();
        }
        catch (ArrayIndexOutOfBoundsException e) {
            // No-op.
        }
        assertTrue(this.box.commitAt(1, 1, new PeerId("localhost", 8081)));
        assertEquals(0, this.box.getLastCommittedIndex());
        assertEquals(1, this.box.getPendingIndex());
        assertTrue(this.box.commitAt(1, 1, new PeerId("localhost", 8082)));
        assertEquals(1, this.box.getLastCommittedIndex());
        assertEquals(2, this.box.getPendingIndex());
        Mockito.verify(this.waiter, Mockito.only()).onCommitted(1);
    }

    @Test
    public void testSetLastCommittedIndexHasPending() {
        assertTrue(box.resetPendingIndex(1));
        assertThrows(IllegalArgumentException.class, () -> this.box.setLastCommittedIndex(1));
    }

    @Test
    public void testSetLastCommittedIndexLessThan() {
        assertFalse(this.box.setLastCommittedIndex(-1));
    }

    @Test
    public void testSetLastCommittedIndex() {
        assertEquals(0, this.box.getLastCommittedIndex());
        assertTrue(this.box.setLastCommittedIndex(1));
        assertEquals(1, this.box.getLastCommittedIndex());
        Mockito.verify(this.waiter, Mockito.only()).onCommitted(1);
    }
}
