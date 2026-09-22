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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Checks that a near-write {@link GridDhtTxPrepareRequest} targeting a cache incarnation older than the receiver's
 * current one is answered with the key in {@code nearEvicted}, so the reader is dropped instead of keeping a stale
 * near entry.
 */
public class GridNearTxRecreateEvictionTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "recreate-near-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /** */
    private CacheConfiguration<Integer, Integer> cacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>(CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setNearConfiguration(new NearCacheConfiguration<>());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** @throws Exception If failed. */
    @Test
    public void testStaleNearWriteReportedEvictedAfterRecreate() throws Exception {
        IgniteEx prim = startGrid(0);
        IgniteEx reader = startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache = prim.cache(CACHE_NAME);

        Integer key = primaryKey(cache);

        cache.put(key, 1);

        // The get makes the second node a near reader, so a write below sends it a near-write prepare.
        assertEquals((Integer)1, reader.<Integer, Integer>cache(CACHE_NAME).get(key));

        TestRecordingCommunicationSpi primSpi = TestRecordingCommunicationSpi.spi(prim);
        TestRecordingCommunicationSpi readerSpi = TestRecordingCommunicationSpi.spi(reader);

        primSpi.blockMessages((node, msg) -> msg instanceof GridDhtTxPrepareRequest);

        // A bounded timeout rolls the transaction back and releases partitions, so the destroy below can proceed
        // while the captured prepare stays queued.
        IgniteInternalFuture<?> txFut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = prim.transactions().txStart(OPTIMISTIC, READ_COMMITTED, 3000, 0)) {
                cache.put(key, 2);

                tx.commit();
            }
            catch (Exception ignored) {
                // Rolled back on timeout while the prepare is blocked.
            }
        });

        primSpi.waitForBlocked();

        txFut.get(getTestTimeout());

        prim.destroyCache(CACHE_NAME);

        prim.getOrCreateCache(cacheConfiguration());

        awaitPartitionMapExchange();

        readerSpi.record(GridDhtTxPrepareResponse.class);

        // Deliver the prepare of the destroyed incarnation to the reader.
        primSpi.stopBlock();

        List<Object> resps = new ArrayList<>();

        assertTrue("Stale near write of the previous cache incarnation must be reported back as evicted.",
            GridTestUtils.waitForCondition(() -> {
                resps.addAll(readerSpi.recordedMessages(false));

                return resps.stream().anyMatch(msg -> !F.isEmpty(((GridDhtTxPrepareResponse)msg).nearEvicted()));
            }, 10_000));
    }
}
