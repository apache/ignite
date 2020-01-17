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

package org.apache.ignite.internal.processors.query.calcite.exec;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;
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
    @BeforeClass
    public static void setupClass() {
        nodeId = UUID.randomUUID();
        func = new AllNodes(Collections.singletonList(nodeId));
    }

    /** */
    @Before
    public void setUp() {
        exch = new TestExchangeService();

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .mailboxRegistry(new TestRegistry())
            .localNodeId(nodeId)
            .exchangeService(exch)
            .build();

        ExecutionContext ectx = new ExecutionContext(ctx, UUID.randomUUID(), 0, null, ImmutableMap.of());

        input = new TestNode(ectx);
        outbox = new Outbox<>(ectx, 0, ectx.fragmentId(), input, func);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBasicOps() throws Exception {
        outbox.request();

        assertTrue(input.signal);

        input.signal = false;

        int maxRows = ExchangeService.BATCH_SIZE * (ExchangeService.PER_NODE_BATCH_COUNT + 1);
        int rows = 0;

        while (input.push(new Object[]{new Object()})) {
            rows++;

            assertFalse(rows > maxRows);
        }

        assertEquals(maxRows, rows);

        assertFalse(exch.ids.isEmpty());

        assertEquals(ExchangeService.PER_NODE_BATCH_COUNT, exch.ids.size());

        assertFalse(input.push(new Object[]{new Object()}));

        assertFalse(input.signal);

        outbox.onAcknowledge(nodeId, exch.ids.remove(0));

        assertTrue(input.signal);

        input.signal = false;

        outbox.onAcknowledge(nodeId, exch.ids.remove(0));

        assertFalse(input.signal);

        assertTrue(input.push(new Object[]{new Object()}));

        input.end();

        assertEquals(EndMarker.INSTANCE, F.last(exch.lastBatch));
    }

    /** */
    private static class TestExchangeService implements ExchangeService {
        /** */
        private List<Integer> ids = new ArrayList<>();

        /** */
        private List<?> lastBatch;

        /** {@inheritDoc} */
        @Override public void sendBatch(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId, List<?> rows) {
            ids.add(batchId);

            lastBatch = rows;
        }

        /** {@inheritDoc} */
        @Override public void acknowledge(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void cancel(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void onCancel(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void onAcknowledge(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void onBatchReceived(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId, List<?> rows) {
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
    private static class TestRegistry implements MailboxRegistry {
        /** {@inheritDoc} */
        @Override public Inbox<?> register(Inbox<?> inbox) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void unregister(Inbox<?> inbox) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void register(Outbox<?> outbox) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void unregister(Outbox<?> outbox) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Outbox<?> outbox(UUID queryId, long exchangeId) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public Inbox<?> inbox(UUID queryId, long exchangeId) {
            throw new AssertionError();
        }
    }
}
