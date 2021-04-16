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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

/**
 *  Tests transactions closes correctly while other caches start and stop.
 *  Tests possible {@link NullPointerException} in {@link TransactionProxyImpl#leave} due to race while
 *  {@link org.apache.ignite.internal.processors.cache.GridCacheTtlManager} initializes (IGNITE-7972).
 */
public class TxOnCachesStartTest extends GridCommonAbstractTest {
    /** */
    private static int NUM_CACHES = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransactionCloseOnCachesStartAndStop() throws Exception {
        Ignite srv = startGrids(5);

        IgniteEx client1 = startClientGrid(getConfiguration("client-1"));

        srv.cluster().active(true);

        CountDownLatch latch = new CountDownLatch(1);

        AtomicReference<NullPointerException> ex = new AtomicReference<>(null);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < NUM_CACHES; i++) {
                    IgniteCache cache = client1.getOrCreateCache(testCacheConfiguration(DEFAULT_CACHE_NAME + i));

                    try {
                        U.sleep(100);
                    }
                    catch (Exception e) {
                        //Ignore.
                    }

                    cache.destroy();
                }
            }
        }, 1, "tx-thread");

        GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                while (true) {
                    try (Transaction tx = client1.transactions().txStart()) {
                        /** Empty transaction, just testing {@link TransactionProxyImpl#leave} */
                    }
                    catch (NullPointerException e) {
                        e.printStackTrace();

                        ex.compareAndSet(null, e);

                        latch.countDown();

                        break;
                    }
                }
            }
        }, 1, "tx-thread");

        latch.await(5, TimeUnit.SECONDS);

        fut.cancel();

        assertNull("NullPointerException thrown while closing transaction", ex.get());
    }

    /**
     * Get cache configuration for tests.
     *
     * @param name Name.
     */
    private CacheConfiguration testCacheConfiguration(String name) {
        return new CacheConfiguration()
                .setGroupName("default-group")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                   .setName(name);
    }
}
