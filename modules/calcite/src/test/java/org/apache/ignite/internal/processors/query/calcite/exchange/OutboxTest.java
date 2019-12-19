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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.calcite.exec.Sink;
import org.apache.ignite.internal.processors.query.calcite.exec.Source;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
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
    private static GridCacheVersion queryId;

    /** */
    private static DestinationFunction func;

    /** */
    private Outbox<Object[]> outbox;

    /** */
    @BeforeClass
    public static void setupClass() {
        nodeId = UUID.randomUUID();
        queryId = new GridCacheVersion(0, 0, 0, 0);

        NodesMapping mapping = new NodesMapping(Collections.singletonList(nodeId), null, NodesMapping.DEDUPLICATED);

        func = DistributionFunction.SingletonDistribution.INSTANCE.toDestination(null, mapping, ImmutableIntList.of());
    }

    /** */
    @Before
    public void setUp() {
        outbox = new Outbox<>(queryId, 0, func);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBasicOps() throws Exception {
        TestSource source = new TestSource();
        TestExchangeService exch = new TestExchangeService();

        Sink<Object[]> sink = outbox.sink();

        outbox.source(source);
        outbox.init(exch);

        assertTrue(exch.registered);
        assertTrue(source.signal);

        source.signal = false;

        int maxRows = ExchangeProcessor.BATCH_SIZE * (ExchangeProcessor.PER_NODE_BATCH_COUNT + 1);
        int rows = 0;

        while (sink.push(new Object[]{new Object()})) {
            rows++;

            assertFalse(rows > maxRows);
        }

        assertEquals(maxRows, rows);

        assertFalse(exch.ids.isEmpty());

        assertEquals(ExchangeProcessor.PER_NODE_BATCH_COUNT, exch.ids.size());

        assertFalse(sink.push(new Object[]{new Object()}));

        assertFalse(source.signal);

        outbox.acknowledge(nodeId, exch.ids.remove(0));

        assertTrue(source.signal);

        source.signal = false;

        outbox.acknowledge(nodeId, exch.ids.remove(0));

        assertFalse(source.signal);

        assertTrue(sink.push(new Object[]{new Object()}));

        sink.end();

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
        @Override public <T> Outbox<T> register(Outbox<T> outbox) {
            registered = true;

            return outbox;
        }

        /** {@inheritDoc} */
        @Override public <T> void unregister(Outbox<T> outbox) {
            unregistered = true;
        }

        /** {@inheritDoc} */
        @Override public void send(GridCacheVersion queryId, long exchangeId, UUID nodeId, int batchId, List<?> rows) {
            ids.add(batchId);

            lastBatch = rows;
        }

        /** {@inheritDoc} */
        @Override public <T> Inbox<T> register(Inbox<T> inbox) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public <T> void unregister(Inbox<T> inbox) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void acknowledge(GridCacheVersion queryId, long exchangeId, UUID nodeId, int batchId) {
            throw new AssertionError();
        }
    }

    /** */
    private static class TestSource implements Source {
        /** */
        boolean signal;

        /** {@inheritDoc} */
        @Override public void signal() {
            signal = true;
        }
    }
}
