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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheStartWithLoadTest extends GridCommonAbstractTest {
    /** */
    static final String CACHE_NAME = "tx_repl";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration().setName(CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.REPLICATED)
            .setDataRegionName("ds")
            .setAffinity(new RendezvousAffinityFunction(false, 32));

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDataRegionConfigurations(
                new DataRegionConfiguration()
                    .setName("ds")
                    .setPersistenceEnabled(true)
                    .setMaxSize(1024 * 1024 * 1024)
            );

        cfg.setDataStorageConfiguration(dsCfg);
        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testNoRebalanceDuringCacheStart() throws Exception {
        IgniteEx crd = (IgniteEx)startGrids(4);

        crd.cluster().active(true);

        AtomicBoolean txLoadStop = new AtomicBoolean();

        AtomicInteger txLoaderNo = new AtomicInteger(0);

        IgniteInternalFuture txLoadFuture = GridTestUtils.runMultiThreadedAsync(() -> {
            Ignite node = grid(txLoaderNo.getAndIncrement());
            IgniteCache<Object, Object> cache = node.cache(CACHE_NAME);
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            final int keys = 5;
            final int keysSpace = 10_000;

            while (!txLoadStop.get()) {
                try (Transaction tx = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    for (int it = 0; it < keys; it++) {
                        int key = rnd.nextInt(keysSpace);
                        byte[] value = new byte[2048];

                        cache.put(key, value);
                    }
                    tx.commit();

                    U.sleep(10);
                }
                catch (Throwable t) {
                    log.warning("Unexpected exception during tx load.", t);
                }
            }
        }, 4, "tx-loader");

        AtomicBoolean hasRebalance = new AtomicBoolean();

        AtomicBoolean cacheRestartStop = new AtomicBoolean();

        IgniteInternalFuture cacheRestartFuture = GridTestUtils.runAsync(() -> {
            Ignite node = grid(0);

            final String tmpCacheName = "tmp";

            while (!cacheRestartStop.get()) {
                try {
                    node.getOrCreateCache(tmpCacheName);

                    boolean hasMoving = false;

                    for (int i = 0; i < 4; i++)
                        hasMoving |= grid(i).cachex(CACHE_NAME).context().topology().hasMovingPartitions();

                    if (hasMoving) {
                        log.error("Cache restarter has been stopped because rebalance is triggered for stable caches.");

                        hasRebalance.set(true);

                        return;
                    }

                    node.destroyCache(tmpCacheName);

                    U.sleep(1_000);
                }
                catch (Throwable t) {
                    log.warning("Unexpected exception during caches restart.", t);
                }
            }
        });

        U.sleep(SF.applyLB(60_000, 5_000));

        cacheRestartStop.set(true);
        txLoadStop.set(true);

        cacheRestartFuture.get();
        txLoadFuture.get();

        Assert.assertFalse(hasRebalance.get());
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000;
    }
}
