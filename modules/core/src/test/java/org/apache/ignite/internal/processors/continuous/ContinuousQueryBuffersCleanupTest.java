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

package org.apache.ignite.internal.processors.continuous;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEntry;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEventBuffer;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryHandler.DFLT_CONTINUOUS_QUERY_BACKUP_ACK_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.query.continuous.TestCacheConrinuousQueryUtils.backupQueueSize;
import static org.apache.ignite.internal.processors.cache.query.continuous.TestCacheConrinuousQueryUtils.bufferedEntries;
import static org.apache.ignite.internal.processors.cache.query.continuous.TestCacheConrinuousQueryUtils.maxReceivedBackupAcknowledgeUpdateCounter;
import static org.apache.ignite.internal.processors.cache.query.continuous.TestCacheConrinuousQueryUtils.partitionContinuesQueryEntryBuffers;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class ContinuousQueryBuffersCleanupTest extends GridCommonAbstractTest {
    /** */
    private final AtomicInteger keyCntr = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi()
                .setAckSendThreshold(1));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testBackupUpdateAndBackupContinuusQueryAcknowledgmentReordered() throws Exception {
        startGrids(2);

        IgniteCache<Object, Object> cache = grid(0).createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction().setPartitions(2))
            .setName(DEFAULT_CACHE_NAME)
            .setBackups(1));

        CountDownLatch cqListenerNotifiedLatch = new CountDownLatch(1);

        Query<?> qry = new ContinuousQuery<>()
            .setLocalListener(events -> cqListenerNotifiedLatch.countDown())
            .setPageSize(1);

        cache.query(qry);

        cache.put(keyForNode(grid(0)), 0);

        // Here we are waiting for initialization of CQ entry buffers on backup node, otherwise backup node will ignore
        // acknowledgements.
        assertTrue(waitForCondition(() -> !continuosQueryEntryBuffers(grid(1)).isEmpty(), getTestTimeout()));

        spi(grid(0)).blockMessages(GridDhtAtomicSingleUpdateRequest.class, grid(1).name());

        int key = keyForNode(grid(0));

        int part = grid(0).affinity(DEFAULT_CACHE_NAME).partition(key);

        cache.put(keyForNode(grid(0)), 0);

        spi(grid(0)).waitForBlocked();

        assertTrue(cqListenerNotifiedLatch.await(getTestTimeout(), MILLISECONDS));

        CacheContinuousQueryEventBuffer buf = continuosQueryEntryBuffers(grid(1)).get(part);

        assertTrue(waitForCondition(() -> maxReceivedBackupAcknowledgeUpdateCounter(buf).get() == 2, getTestTimeout()));

        spi(grid(0)).stopBlock();

        assertTrue(waitForCondition(() -> G.allGrids().stream().allMatch(this::isContinuesQueryBufferEmpty), getTestTimeout()));
    }

    /** */
    @Test
    public void testContinuousQueryBuffersCleanup() throws Exception {
        startGrids(2);

        IgniteEx cli = startClientGrid(3);

        try (IgniteClient thinCli = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))) {
            grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                .setAffinity(new RendezvousAffinityFunction().setPartitions(2))
                .setName(DEFAULT_CACHE_NAME)
                .setWriteSynchronizationMode(FULL_SYNC)
                .setBackups(1));

            for (int primaryNodeIdx = 0; primaryNodeIdx < 2; primaryNodeIdx++) {
                checkBuffersCleared(primaryNodeIdx, qry -> grid(0).cache(DEFAULT_CACHE_NAME).query(qry));
                checkBuffersCleared(primaryNodeIdx, qry -> grid(1).cache(DEFAULT_CACHE_NAME).query(qry));
                checkBuffersCleared(primaryNodeIdx, qry -> cli.cache(DEFAULT_CACHE_NAME).query(qry));
                checkBuffersCleared(primaryNodeIdx, qry -> thinCli.cache(DEFAULT_CACHE_NAME).query(qry));
            }
        }
    }

    /** */
    private void checkBuffersCleared(int primaryNodeIdx, Function<Query<?>, QueryCursor<?>> queryExecutor) throws Exception {
        IgniteEx primary = grid(primaryNodeIdx);

        int cacheOpRounds = Math.round(DFLT_CONTINUOUS_QUERY_BACKUP_ACK_THRESHOLD * 0.3f);

        // We repeatedly perform 5 cache operations that raise CREATE, UPDATE, REMOVED, EXPIRIED events.
        // The total number of events is selected in a such way as to check for a backup notification due to a
        // buffer overflow, and then by timeout.
        int expEvtsCnt = cacheOpRounds * 5;

        CountDownLatch cqListenerNotifiedLatch = new CountDownLatch(expEvtsCnt);

        Query<?> qry = new ContinuousQuery<>()
            .setLocalListener(events -> cqListenerNotifiedLatch.countDown())
            .setPageSize(1)
            .setIncludeExpired(true);

        try (QueryCursor<?> ignored = queryExecutor.apply(qry)) {
            for (int i = 0; i < cacheOpRounds; i++) {
                int testKey = keyForNode(primary);

                primary.cache(DEFAULT_CACHE_NAME).put(testKey, 0);
                primary.cache(DEFAULT_CACHE_NAME).put(testKey, 1);
                primary.cache(DEFAULT_CACHE_NAME).remove(testKey);
                primary.cache(DEFAULT_CACHE_NAME)
                    .withExpiryPolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 100)))
                    .put(keyForNode(primary), 0);
            }

            assertTrue(cqListenerNotifiedLatch.await(getTestTimeout(), MILLISECONDS));

            assertTrue(waitForCondition(() -> G.allGrids().stream().allMatch(this::isContinuesQueryBufferEmpty), getTestTimeout()));
        }
    }

    /** */
    private boolean isContinuesQueryBufferEmpty(Ignite ignite) {
        GridContinuousProcessor contProc = ((IgniteEx)ignite).context().continuous();

        Collection<CacheContinuousQueryHandler<?, ?>> cqHandlers = new ArrayList<>();

        contProc.remoteRoutineInfos().forEach((routineId, rmtRoutineInfo) ->
            cqHandlers.add((CacheContinuousQueryHandler<?, ?>)rmtRoutineInfo.handler()));

        contProc.localRoutineInfos().forEach((routineId, locRoutineInfo) ->
            cqHandlers.add((CacheContinuousQueryHandler<?, ?>)locRoutineInfo.handler()));

        contProc.clientRoutineInfos().forEach((nodeId, rmtRoutineInfos) ->
            rmtRoutineInfos.forEach((routineId, locRoutineInfo) ->
                cqHandlers.add((CacheContinuousQueryHandler<?, ?>)locRoutineInfo.handler())));

        for (CacheContinuousQueryHandler<?, ?> cqHnd : cqHandlers) {
            for (CacheContinuousQueryEventBuffer evtBuf : partitionContinuesQueryEntryBuffers(cqHnd).values()) {
                if (backupQueueSize(evtBuf) != 0)
                    return false;

                CacheContinuousQueryEntry[] entries = bufferedEntries(evtBuf);

                if (!F.isEmpty(entries)) {
                    for (CacheContinuousQueryEntry entry : entries) {
                        if (entry != null)
                            return false;
                    }
                }
            }
        }

        return true;
    }

    /** */
    private Map<Integer, CacheContinuousQueryEventBuffer> continuosQueryEntryBuffers(IgniteEx ignite) {
        GridContinuousProcessor contProc = ignite.context().continuous();

        return partitionContinuesQueryEntryBuffers(
            (CacheContinuousQueryHandler<?, ?>)contProc.remoteRoutineInfos().values().iterator().next().handler()
        );
    }

    /** */
    private int keyForNode(Ignite ignite) {
        return keyForNode(grid(0).affinity(DEFAULT_CACHE_NAME), keyCntr, ignite.cluster().localNode());
    }
}
