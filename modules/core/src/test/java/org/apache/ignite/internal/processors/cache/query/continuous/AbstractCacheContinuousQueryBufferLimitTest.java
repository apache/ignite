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

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.IgniteCache;
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
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.systemview.view.ContinuousQueryView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public abstract class AbstractCacheContinuousQueryBufferLimitTest extends GridCommonAbstractTest {
    /** Cache partitions count. */
    private static final int PARTS = 1;

    /** Total number of cache keys. */
    private static final int TOTAL_KEYS = 1024;

    /** Number of pending entries.  */
    private static final int PENDING_LIMIT = 1010;

    /** Timeout to wait for pending buffer overflow. */
    private static final long OVERFLOW_TIMEOUT_MS = 15_000L;

    /** Default remote no-op filter. */
    private static final CacheEntryEventSerializableFilter<Integer, Integer> RMT_FILTER = e -> true;

    /** Counter of cache messages being send. */
    private final AtomicInteger msgCntr = new AtomicInteger();

    /** @throws Exception If fails. */
    @Test
    public void testContinuousQueryBatchSwitchOnAck() throws Exception {
        doTestContinuousQueryPendingBufferLimit((n, msg) ->
            msg instanceof GridCacheIdMessage && msgCntr.getAndIncrement() == 10);
    }

    /** @throws Exception If fails. */
    @Test
    @WithSystemProperty(key = "IGNITE_CONTINUOUS_QUERY_LISTENER_MAX_BUFFER_SIZE", value = "1000")
    public void testContinuousQueryPendingBufferLimit() throws Exception {
        doTestContinuousQueryPendingBufferLimit((n, msg) ->
            (msg instanceof GridCacheIdMessage && msgCntr.getAndIncrement() == 10) ||
                msg instanceof CacheContinuousQueryBatchAck);
    }


    /** {@inheritDoc} */
    @Override public IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(atomicityMode())
                .setCacheMode(cacheMode())
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
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Cache atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @param locBlockPred Block predicate on local node to emulate message delivery issues.
     * @throws Exception If fails.
     */
    private void doTestContinuousQueryPendingBufferLimit(
        IgniteBiPredicate<ClusterNode, Message> locBlockPred
    ) throws Exception
    {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        IgniteEx locIgnite = startGrid(0);
        IgniteEx rmtIgnite = startGrid(1);

        IgniteCache<Integer, Integer> cache = locIgnite.cache(DEFAULT_CACHE_NAME);
        CacheConfiguration<Integer, Integer> ccfg = cache.getConfiguration(CacheConfiguration.class);

        for (int i = 0; i < TOTAL_KEYS; i++)
            cache.put(i, i);

        assertEquals(PARTS, ccfg.getAffinity().partitions());

        AtomicLong lastAcked = new AtomicLong();

        ContinuousQuery<Integer, Integer> cq = new ContinuousQuery<>();
        cq.setRemoteFilterFactory(FactoryBuilder.factoryOf(RMT_FILTER));
        cq.setLocalListener((events) ->
            events.forEach(e ->
                lastAcked.getAndUpdate(c ->
                    Math.max(c, ((CacheQueryEntryEvent<?, ?>)e).getPartitionUpdateCounter()))));
        cq.setLocal(false);

        IgniteInternalFuture<?> updFut = null;

        try (QueryCursor<?> qry = locIgnite.cache(DEFAULT_CACHE_NAME).query(cq)) {
            awaitPartitionMapExchange();

            for (int j = 0; j < TOTAL_KEYS; j++)
                putX2Value(cache, rnd.nextInt(TOTAL_KEYS));

            SystemView<ContinuousQueryView> rmtQryView = rmtIgnite.context().systemView().view(CQ_SYS_VIEW);
            assertEquals(1, rmtQryView.size());

            UUID routineId = rmtQryView.iterator().next().routineId();

            // Partition Id, Update Counter, Continuous Entry.
            ConcurrentMap<Long, CacheContinuousQueryEntry> pending =
                getContinuousQueryPendingBuffer(rmtIgnite, routineId, CU.cacheId(DEFAULT_CACHE_NAME), 0);

            spi(locIgnite).blockMessages(locBlockPred);

            updFut = GridTestUtils.runMultiThreadedAsync(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    putX2Value(cache, rnd.nextInt(TOTAL_KEYS));
                }
            }, 1, "cq-put-");

            assertNotNull("Partition remote buffers must be inited", pending);

            log.warning("Waiting for pending buffer being overflowed within " + OVERFLOW_TIMEOUT_MS + " ms.");

            boolean await = waitForCondition(() -> pending.size() > PENDING_LIMIT, OVERFLOW_TIMEOUT_MS);

            spi(locIgnite).stopBlock();

            assertFalse("Pending buffer exceeded the limit despite entries have been acked " +
                    "[lastAcked=" + lastAcked + ", pending=" + S.compact(pending.keySet(), i -> i + 1) + ']',
                await);
        }
        finally {
            if (updFut != null)
                updFut.cancel();
        }
    }

    /**
     * @param cache Ignite cache.
     * @param key Key to change.
     */
    private static void putX2Value(IgniteCache<Integer, Integer> cache, int key) {
        cache.put(key, key * 2);
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
     * @param routineId Continuous query id.
     * @param cacheId Cache id.
     * @param partId Partition id.
     * @return Map of pending entries.
     */
    private static ConcurrentMap<Long, CacheContinuousQueryEntry> getContinuousQueryPendingBuffer(
        IgniteEx ignite,
        UUID routineId,
        int cacheId,
        int partId
    ) {
        CacheContinuousQueryHandler<?, ?> hnd = getRemoteContinuousQueryHandler(ignite, routineId);
        GridCacheContext<?, ?> cctx = ignite.context().cache().context().cacheContext(cacheId);
        CacheContinuousQueryEventBuffer buff = hnd.partitionBuffer(cctx, partId);

        return getFieldValue(buff, CacheContinuousQueryEventBuffer.class, "pending");
    }
}
