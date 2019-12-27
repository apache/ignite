/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exchange;

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import org.apache.ignite.internal.processors.query.calcite.exec.AbstractNode;
import org.apache.ignite.internal.processors.query.calcite.exec.EndMarker;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.Outbox;
import org.apache.ignite.internal.processors.query.calcite.exec.Sink;
import org.apache.ignite.internal.processors.query.calcite.exec.StripedExecutor;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.trait.AllNodes;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunction;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class OutboxTest extends GridCommonAbstractTest {
    /** */
    private static UUID nodeId;

    /** */
    private static DestinationFunction func;

    /** */
    private Outbox<Object[]> outbox;

    /** */
    private TestNode input;

    /** */
    private TestExchangeService exch;

    /** */
    private TestExecutor exec;

    /** */
    @BeforeClass
    public static void setupClass() {
        nodeId = UUID.randomUUID();
        func = new AllNodes(Collections.singletonList(nodeId));
    }

    /** */
    @Before
    public void setUp() {
        exec = new TestExecutor();
        exch = new TestExchangeService();

        PlannerContext ctx = PlannerContext.builder()
            .localNodeId(nodeId)
            .exchangeProcessor(exch)
            .executor(exec)
            .build();

        ExecutionContext ectx = new ExecutionContext(UUID.randomUUID(), ctx, ImmutableMap.of());

        input = new TestNode(ectx);
        outbox = new Outbox<>(ectx, 0, input, func);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBasicOps() throws Exception {
        outbox.request();

        assertFalse(exec.taskQueue.isEmpty());

        exec.execute();

        assertTrue(exch.registered);

        assertTrue(input.signal);

        input.signal = false;

        int maxRows = ExchangeProcessor.BATCH_SIZE * (ExchangeProcessor.PER_NODE_BATCH_COUNT + 1);
        int rows = 0;

        while (input.push(new Object[]{new Object()})) {
            rows++;

            assertFalse(rows > maxRows);
        }

        assertEquals(maxRows, rows);

        assertFalse(exch.ids.isEmpty());

        assertEquals(ExchangeProcessor.PER_NODE_BATCH_COUNT, exch.ids.size());

        assertFalse(input.push(new Object[]{new Object()}));

        assertTrue(exec.taskQueue.isEmpty());

        assertFalse(input.signal);

        outbox.onAcknowledge(nodeId, exch.ids.remove(0));

        assertFalse(exec.taskQueue.isEmpty());

        exec.execute();

        assertTrue(input.signal);

        input.signal = false;

        outbox.onAcknowledge(nodeId, exch.ids.remove(0));

        assertTrue(exec.taskQueue.isEmpty());

        assertFalse(input.signal);

        assertTrue(input.push(new Object[]{new Object()}));

        input.end();

        assertTrue(exch.unregistered);

        assertEquals(EndMarker.INSTANCE, F.last(exch.lastBatch));
    }

    /** */
    private static class TestExchangeService implements ExchangeProcessor {
        /** */
        private boolean registered;

        /** */
        private boolean unregistered;

        /** */
        private List<Integer> ids = new ArrayList<>();

        /** */
        private List<?> lastBatch;

        /** {@inheritDoc} */
        @Override public void register(Outbox<?> outbox) {
            registered = true;
        }

        /** {@inheritDoc} */
        @Override public Inbox<?> register(Inbox<?> inbox) {
            throw new AssertionError();
        }

        @Override public void unregister(Outbox<?> outbox) {
            unregistered = true;
        }

        @Override public void unregister(Inbox<?> inbox) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void sendBatch(Outbox<?> sender, UUID nodeId, UUID queryId, long exchangeId, int batchId, List<?> rows) {
            ids.add(batchId);

            lastBatch = rows;
        }

        /** {@inheritDoc} */
        @Override public void sendAcknowledgment(Inbox<?> sender, UUID nodeId, UUID queryId, long exchangeId, int batchId) {
            throw new AssertionError();
        }
    }

    /** */
    private static class TestNode extends AbstractNode<Object[]> {
        /** */
        private boolean signal;

        /** */
        private TestNode(ExecutionContext ctx) {
            super(ctx);
        }

        /** */
        public boolean push(Object[] row) {
            return target().push(row);
        }

        /** */
        public void end() {
            target().end();
        }

        /** {@inheritDoc} */
        @Override public void request() {
            signal = true;
        }

        /** {@inheritDoc} */
        @Override public Sink<Object[]> sink(int idx) {
            throw new AssertionError();
        }
    }

    /** */
    private static class TestExecutor implements StripedExecutor {
        /** */
        private Queue<Runnable> taskQueue = new ArrayDeque<>();

        /** {@inheritDoc} */
        @Override public Future<Void> execute(Runnable task, Serializable taskId) {
            FutureTask<Void> res = new FutureTask<>(task, null);

            taskQueue.offer(res);

            return res;
        }

        /** */
        private void execute() {
            while (true) {
                Runnable poll = taskQueue.poll();

                if (poll == null)
                    break;

                poll.run();
            }
        }
    }
}
