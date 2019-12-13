package org.apache.ignite.internal.processors.query.calcite.exchange;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.calcite.exec.Sink;
import org.apache.ignite.internal.processors.query.calcite.exec.Source;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.SingleTargetFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class OutboxTest extends GridCommonAbstractTest {
    private static UUID nodeId;
    private static GridCacheVersion queryId;
    private static DestinationFunction func;
    private static Collection<UUID> targets;

    private Outbox<Object[]> outbox;

    @BeforeClass
    public static void setupClass() {
        nodeId = UUID.randomUUID();
        queryId = new GridCacheVersion(0, 0, 0, 0);

        NodesMapping mapping = new NodesMapping(Collections.singletonList(nodeId), null, NodesMapping.DEDUPLICATED);

        targets = mapping.nodes();
        func = SingleTargetFactory.INSTANCE.create(null, mapping, ImmutableIntList.of());
    }


    @Before
    public void setUp() {
        outbox = new Outbox<>(queryId, 0, targets, func);
    }

    @Test
    public void testBasicOps() {
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

    private static class TestExchangeService implements ExchangeProcessor {
        private boolean registered;
        private boolean unregistered;
        private List<Integer> ids = new ArrayList<>();

        private List<?> lastBatch;


        @Override public <T> Outbox<T> register(Outbox<T> outbox) {
            registered = true;

            return outbox;
        }

        @Override public <T> void unregister(Outbox<T> outbox) {
            unregistered = true;
        }

        @Override public void send(GridCacheVersion queryId, long exchangeId, UUID nodeId, int batchId, List<?> rows) {
            ids.add(batchId);

            lastBatch = rows;
        }

        @Override public <T> Inbox<T> register(Inbox<T> inbox) {
            throw new AssertionError();
        }

        @Override public <T> void unregister(Inbox<T> inbox) {
            throw new AssertionError();
        }

        @Override public void acknowledge(GridCacheVersion queryId, long exchangeId, UUID nodeId, int batchId) {
            throw new AssertionError();
        }
    }

    private static class TestSource implements Source {
        boolean signal;

        @Override public void signal() {
            signal = true;
        }
    }
}