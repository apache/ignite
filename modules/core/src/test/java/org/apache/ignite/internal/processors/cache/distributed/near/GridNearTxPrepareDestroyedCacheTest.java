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
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheObjectNotResolvedException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Checks the receiver's behaviour when a tx prepare of a destroyed cache cannot be unmarshalled: without the cache
 * there is no cache object context, so the keys stay unresolved and rebuilding the DHT-version map hashes such a key
 * and throws {@link CacheObjectNotResolvedException}. The receiver must convert it to an error response for the
 * sender and stay alive instead of going down through the failure handler.
 */
public class GridNearTxPrepareDestroyedCacheTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "destroy-unmarshal-cache";

    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** */
    private final AtomicBoolean failure = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setGridLogger(listeningLog);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new AbstractFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                failure.set(true);

                return false;
            }
        };
    }

    /** */
    private CacheConfiguration<Integer, Integer> cacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>(CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** @throws Exception If failed. */
    @Test
    public void testPrepareOfDestroyedCacheAnsweredWithErrorResponse() throws Exception {
        IgniteEx prim = startGrid(0);

        // The client's exchange does not wait for its transactions to release partitions, so the destroy below can
        // proceed while the client keeps an active transaction with the captured prepare.
        IgniteEx near = startClientGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache = near.cache(CACHE_NAME);

        Integer key = primaryKey(prim.cache(CACHE_NAME));

        cache.put(key, 1);

        TestRecordingCommunicationSpi nearSpi = TestRecordingCommunicationSpi.spi(near);
        TestRecordingCommunicationSpi primSpi = TestRecordingCommunicationSpi.spi(prim);

        nearSpi.blockMessages((node, msg) -> msg instanceof GridNearTxPrepareRequest);

        IgniteInternalFuture<?> txFut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = near.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
                // A transform entry adds its key to the prepare's DHT-version map, so the receiver hashes the key
                // while rebuilding the map.
                cache.invoke(key, new SetValueProcessor());

                tx.commit();
            }
            catch (Exception ignored) {
                // Rolled back by the cache stop while the prepare is blocked.
            }
        });

        nearSpi.waitForBlocked();

        prim.destroyCache(CACHE_NAME);

        awaitPartitionMapExchange();

        txFut.get(getTestTimeout());

        primSpi.record(GridNearTxPrepareResponse.class);

        LogListener resDelivered = LogListener.matches("Failed to find future for near prepare response").build();

        listeningLog.registerListener(resDelivered);

        // Deliver the prepare of the destroyed cache to the primary.
        nearSpi.stopBlock();

        List<Object> resps = new ArrayList<>();

        assertTrue("The prepare of the destroyed cache must be answered with a response.",
            GridTestUtils.waitForCondition(() -> {
                resps.addAll(primSpi.recordedMessages(false));

                return !resps.isEmpty();
            }, 10_000));

        Throwable err = ((GridNearTxPrepareResponse)resps.get(0)).error();

        assertNotNull("The response must carry the unmarshalling error.", err);

        CacheObjectNotResolvedException cause = X.cause(err, CacheObjectNotResolvedException.class);

        assertNotNull("Unexpected response error: " + X.getFullStackTrace(err), cause);

        assertTrue(cause.getMessage(), cause.getMessage().contains("Cache object is not deserialized"));

        // The transaction had been rolled back by the cache stop before the prepare was delivered, so the arrived
        // error response finds no future to complete.
        assertTrue("The sender must receive the error response.",
            GridTestUtils.waitForCondition(resDelivered::check, 10_000));

        assertFalse("The failure handler must not be triggered by the stale prepare.", failure.get());

        // The receiver is fully operational: it serves the recreated cache.
        prim.getOrCreateCache(cacheConfiguration()).put(key, 2);

        assertEquals((Integer)2, near.<Integer, Integer>cache(CACHE_NAME).get(key));
    }

    /** */
    private static class SetValueProcessor implements CacheEntryProcessor<Integer, Integer, Void> {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Integer> entry, Object... args) {
            entry.setValue(2);

            return null;
        }
    }
}
