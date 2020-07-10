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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.CacheQueryEntryEvent;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateRequest;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.systemview.view.ContinuousQueryView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEventBuffer.MAX_PENDING_BUFF_SIZE;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheContinuousQueryBufferLimitTest extends GridCommonAbstractTest {
    /** Cache partitions count. */
    private static final int PARTS = 1;

    /** Total number of cache keys. */
    private static final int TOTAL_KEYS = 1024;

    /** Maximum of keys processed by CQ to check buffer being overflowed. */
    private static final long OVERFLOW_KEYS_COUNT = MAX_PENDING_BUFF_SIZE * 3;

    /** Default remote no-op filter. */
    private static final CacheEntryEventSerializableFilter<Integer, Integer> RMT_FILTER = e -> true;

    /** Counter of cache messages being send. */
    private final AtomicInteger msgCntr = new AtomicInteger();

    /** Cache mode. */
    @Parameterized.Parameter(0)
    public CacheMode cacheMode;

    /** Cache atomicity mode. */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicityMode;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "cacheMode={0}, atomicityMode={1}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {
            {REPLICATED, ATOMIC},
            {PARTITIONED, ATOMIC},
            {REPLICATED, TRANSACTIONAL},
            {PARTITIONED, TRANSACTIONAL}
        });
    }

    /**
     * Local pending limit for this test is less than MAX_PENDING_BUFF_SIZE,
     * so pending entries must be cleaned prior to reaching it.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testContinuousQueryBatchSwitchOnAck() throws Exception {
        doTestContinuousQueryPendingBufferLimit((n, msg) ->
            cachePutOperationRequestMessage(msg) && msgCntr.getAndIncrement() == 10, MAX_PENDING_BUFF_SIZE / 10);
    }

    /**
     * The test blocks switching current CacheContinuousQueryEventBuffer.Batch to the new one, so
     * pending entries will be processed (dropped on backups and send to the client on primaries)
     * when the MAX_PENDING_BUFF_SIZE is reached.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testContinuousQueryPendingBufferLimit() throws Exception {
        doTestContinuousQueryPendingBufferLimit((n, msg) ->
            (cachePutOperationRequestMessage(msg) && msgCntr.getAndIncrement() == 10) ||
                msg instanceof CacheContinuousQueryBatchAck, (int)(MAX_PENDING_BUFF_SIZE * 1.2));
    }

    /** @throws Exception If fails. */
    @Test
    public void testPendingSendToClientOnLimitReached() throws Exception {
        AtomicInteger keys = new AtomicInteger();
        AtomicReference<String> err = new AtomicReference<>();

        IgniteEx srv = startGrids(2);
        IgniteEx clnt = startClientGrid();

        IgniteCache<Integer, Integer> cache = clnt.cache(DEFAULT_CACHE_NAME);
        CacheEntryEventSerializableFilter<Integer, Integer> filter = evt -> evt.getKey() % 2 == 0;

        ContinuousQuery<Integer, Integer> cq = new ContinuousQuery<>();
        cq.setRemoteFilterFactory(FactoryBuilder.factoryOf(filter));
        cq.setLocalListener((events) -> events.forEach(e -> {
            if (!filter.evaluate(e))
                err.compareAndSet(null, "Key must be filtered [e=" + e + ']');
        }));
        cq.setLocal(false);

        spi(srv).blockMessages((nodeId, msg) -> (cachePutOperationRequestMessage(msg) && msgCntr.getAndIncrement() == 7) ||
            msg instanceof CacheContinuousQueryBatchAck);

        IgniteInternalFuture<?> loadFut = null;

        try (QueryCursor<?> qry = cache.query(cq)) {
            awaitPartitionMapExchange();

            loadFut = GridTestUtils.runMultiThreadedAsync(() -> {
                while (!Thread.currentThread().isInterrupted())
                    cache.put(keys.incrementAndGet(), 0);
            }, 6, "cq-put-");

            // Entries are checked by CacheEntryUpdatedListener which has been set to CQ. Check that all
            // entries greater than pending limit filtered correctly (entries are sent to client on buffer overflow).
            boolean await = waitForCondition(() -> keys.get() > OVERFLOW_KEYS_COUNT, 30_000);

            assertTrue("Number of keys to put must reach the limit [keys=" + keys.get() +
                ", limit=" + OVERFLOW_KEYS_COUNT + ']', await);
        }
        finally {
            TestRecordingCommunicationSpi.stopBlockAll();

            if (loadFut != null)
                loadFut.cancel();
        }

        if (err.get() != null)
            throw new Exception(err.get());
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(atomicityMode)
                .setCacheMode(cacheMode)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS)));
    }

    /** */
    @Before
    public void resetMessageCounter() {
        msgCntr.set(0);
    }

    /** */
    @After
    public void stopAllInstances() {
        stopAllGrids();
    }

    /**
     * @param locBlockPred Block predicate on local node to emulate message delivery issues.
     * @param pendingLimit Test limit of pending entries.
     * @throws Exception If fails.
     */
    private void doTestContinuousQueryPendingBufferLimit(
        IgniteBiPredicate<ClusterNode, Message> locBlockPred,
        int pendingLimit
    ) throws Exception
    {
        AtomicInteger keys = new AtomicInteger();

        IgniteEx locIgnite = startGrid(0);
        IgniteEx rmtIgnite = startGrid(1);

        IgniteCache<Integer, Integer> cache = locIgnite.cache(DEFAULT_CACHE_NAME);
        CacheConfiguration<Integer, Integer> ccfg = cache.getConfiguration(CacheConfiguration.class);

        for (int i = 0; i < TOTAL_KEYS; i++)
            cache.put(i, i);

        assertEquals(PARTS, ccfg.getAffinity().partitions());

        GridAtomicLong lastAcked = new GridAtomicLong();

        ContinuousQuery<Integer, Integer> cq = new ContinuousQuery<>();
        cq.setRemoteFilterFactory(FactoryBuilder.factoryOf(RMT_FILTER));
        cq.setLocalListener((events) ->
            events.forEach(e ->
                lastAcked.setIfGreater(((CacheQueryEntryEvent<?, ?>)e).getPartitionUpdateCounter())));
        cq.setLocal(false);

        IgniteInternalFuture<?> updFut = null;

        try (QueryCursor<?> qry = locIgnite.cache(DEFAULT_CACHE_NAME).query(cq)) {
            awaitPartitionMapExchange();

            // Partition Id, Update Counter, Continuous Entry.
            ConcurrentMap<Long, CacheContinuousQueryEntry> pending =
                getContinuousQueryPendingBuffer(rmtIgnite, CU.cacheId(DEFAULT_CACHE_NAME), 0);

            spi(locIgnite).blockMessages(locBlockPred);

            updFut = GridTestUtils.runMultiThreadedAsync(() -> {
                while (keys.get() <= OVERFLOW_KEYS_COUNT)
                    cache.put(keys.incrementAndGet(), 0);
            }, 3, "cq-put-");

            assertNotNull("Partition remote buffers must be inited", pending);

            log.warning("Waiting for pending buffer being overflowed within " + OVERFLOW_KEYS_COUNT +
                " number of keys.");

            boolean await = waitForCondition(() -> pending.size() > pendingLimit, () -> keys.get() <= OVERFLOW_KEYS_COUNT);

            assertFalse("Pending buffer exceeded the limit despite entries have been acked " +
                    "[lastAcked=" + lastAcked + ", pending=" + S.compact(pending.keySet(), i -> i + 1) + ']',
                await);
        }
        finally {
            spi(locIgnite).stopBlock();

            if (updFut != null)
                updFut.cancel();
        }
    }

    /**
     * @param msg Cache message.
     * @return {@code true} if message is initial for cache operation.
     */
    private boolean cachePutOperationRequestMessage(Message msg) {
        switch (atomicityMode) {
            case ATOMIC:
                return msg instanceof GridDhtAtomicUpdateRequest;

            case TRANSACTIONAL:
                return msg instanceof GridDhtTxPrepareRequest;

            default:
                throw new IgniteException("Unsupported atomicity mode: " + atomicityMode);
        }
    }

    /**
     * @param ignite Ignite remote instance.
     * @param routineId Routine id.
     * @return Registered handler.
     */
    private static <K, V> CacheContinuousQueryHandler<K, V> getRemoteContinuousQueryHandler(
        IgniteEx ignite,
        UUID routineId
    ) {
        GridContinuousProcessor contProc = ignite.context().continuous();

        ConcurrentMap<UUID, GridContinuousProcessor.RemoteRoutineInfo> rmtInfos =
            getFieldValue(contProc, GridContinuousProcessor.class, "rmtInfos");

        return rmtInfos.get(routineId) == null ?
            null : (CacheContinuousQueryHandler<K, V>)rmtInfos.get(routineId).handler();
    }

    /**
     * @param ignite Ignite remote instance.
     * @param cacheId Cache id.
     * @param partId Partition id.
     * @return Map of pending entries.
     */
    private static ConcurrentMap<Long, CacheContinuousQueryEntry> getContinuousQueryPendingBuffer(
        IgniteEx ignite,
        int cacheId,
        int partId
    ) {
        SystemView<ContinuousQueryView> rmtQryView = ignite.context().systemView().view(CQ_SYS_VIEW);
        assertEquals(1, rmtQryView.size());

        UUID routineId = rmtQryView.iterator().next().routineId();

        CacheContinuousQueryHandler<?, ?> hnd = getRemoteContinuousQueryHandler(ignite, routineId);
        GridCacheContext<?, ?> cctx = ignite.context().cache().context().cacheContext(cacheId);
        CacheContinuousQueryEventBuffer buff = hnd.partitionBuffer(cctx, partId);

        return getFieldValue(buff, CacheContinuousQueryEventBuffer.class, "pending");
    }
}
