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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.R1;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Multithreaded reduce query tests with lots of data.
 */
public class GridCacheReduceQueryMultithreadedSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 5;

    /** */
    private static final int TEST_TIMEOUT = 2 * 60 * 1000;

    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration<?,?> cfg = super.cacheConfiguration(gridName);

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setIndexedTypes(
            String.class, Integer.class
        );

        return cfg;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testReduceQuery() throws Exception {
        final int keyCnt = 5000;
        final int logFreq = 500;

        final GridCacheAdapter<String, Integer> c = internalCache(jcache());

        final CountDownLatch startLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new Callable() {
            @Override public Object call() throws Exception {
                for (int i = 1; i < keyCnt; i++) {
                    c.getAndPut(String.valueOf(i), i);

                    startLatch.countDown();

                    if (i % logFreq == 0)
                        info("Stored entries: " + i);
                }

                return null;
            }
        }, 1);

        // Create query.
        final CacheQuery<List<?>> sumQry = c.context().queries().
            createSqlFieldsQuery("select _val from Integer", false).timeout(TEST_TIMEOUT);

        final R1<List<?>, Integer> rmtRdc = new R1<List<?>, Integer>() {
            /** */
            private AtomicInteger sum = new AtomicInteger();

            @Override public boolean collect(List<?> e) {
                sum.addAndGet((Integer)e.get(0));

                return true;
            }

            @Override public Integer reduce() {
                return sum.get();
            }
        };

        final AtomicBoolean stop = new AtomicBoolean();

        startLatch.await();

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Callable() {
            @Override public Object call() throws Exception {
                int cnt = 0;

                while (!stop.get()) {
                    Collection<Integer> res = sumQry.execute(rmtRdc).get();

                    int sum = F.sumInt(res);

                    cnt++;

                    assertTrue(sum > 0);

                    if (cnt % logFreq == 0) {
                        info("Reduced value: " + sum);
                        info("Executed queries: " + cnt);
                    }
                }

                return null;
            }
        }, 1);

        fut1.get();

        stop.set(true);

        fut2.get();
    }
}