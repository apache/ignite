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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheConnectionRecoveryTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 5;

    /** */
    private static final int CLIENTS = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setCacheConfiguration(
            cacheConfiguration("cache1", TRANSACTIONAL),
            cacheConfiguration("cache2", TRANSACTIONAL_SNAPSHOT),
            cacheConfiguration("cache3", ATOMIC));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);

        startClientGridsMultiThreaded(SRVS, CLIENTS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testConnectionRecovery() throws Exception {
        final Map<Integer, Integer> data = new TreeMap<>();

        for (int i = 0; i < 500; i++)
            data.put(i, i);

        final AtomicInteger idx = new AtomicInteger();

        final long stopTime = U.currentTimeMillis() + 30_000;

        final AtomicReference<CyclicBarrier> barrierRef = new AtomicReference<>();

        final int TEST_THREADS = (CLIENTS + SRVS) * 2;

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int idx0 = idx.getAndIncrement();
                Ignite node = ignite(idx0 % (SRVS + CLIENTS));

                Thread.currentThread().setName("test-thread-" + idx0 + "-" + node.name());

                IgniteCache[] caches = {
                    node.cache("cache1"),
                    node.cache("cache2"),
                    node.cache("cache3")};

                int iter = 0;

                while (U.currentTimeMillis() < stopTime) {
                    try {
                        for (IgniteCache cache : caches) {
                            while (true) {
                                try {
                                    cache.putAllAsync(data).get(15, SECONDS);

                                    break;
                                }
                                catch (Exception e) {
                                    MvccFeatureChecker.assertMvccWriteConflict(e);
                                }
                            }
                        }

                        CyclicBarrier b = barrierRef.get();

                        if (b != null)
                            b.await(15, SECONDS);
                    }
                    catch (Exception e) {
                        synchronized (IgniteCacheConnectionRecoveryTest.class) {
                            log.error("Failed to execute update, will dump debug information" +
                                " [err=" + e + ", iter=" + iter + ']', e);

                            List<Ignite> nodes = IgnitionEx.allGridsx();

                            for (Ignite node0 : nodes)
                                ((IgniteKernal)node0).dumpDebugInfo();

                            U.dumpThreads(log);
                        }

                        throw e;
                    }
                }

                return null;
            }
        }, TEST_THREADS, "test-thread");

        while (System.currentTimeMillis() < stopTime) {
            boolean closed = false;

            for (Ignite node : G.allGrids()) {
                if (IgniteCacheMessageRecoveryAbstractTest.closeSessions(node))
                    closed = true;
            }

            if (closed) {
                CyclicBarrier b = new CyclicBarrier(TEST_THREADS + 1, new Runnable() {
                    @Override public void run() {
                        barrierRef.set(null);
                    }
                });

                barrierRef.set(b);

                b.await();
            }

            U.sleep(50);
        }

        fut.get();
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Cache atomicity mode.
     * @return Configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(name);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(REPLICATED);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }
}
