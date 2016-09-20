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

package org.apache.ignite.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 */
public class IgniteCacheShakeExchangeTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Additional node */
    private static final int ITERATIONS = 13;

    /** Partitioned cache name. */
    private static final String CACHE_NAME_DHT_PARTITIONED = "cacheP";

    /** Replicated cache name. */
    private static final String CACHE_NAME_DHT_REPLICATED = "cacheR";

    /** Ignite 1. */
    private static Ignite ignite1;

    /** Ignite 2. */
    private static Ignite ignite2;

    /** Ignite 3. */
    private static Ignite ignite3;

    /** catch Exception. */
    private static final AtomicReference<Throwable> exc = new AtomicReference<>();

    /** Thread dump in catch exception moment. */
    private static final AtomicReference<GridStringBuilder> dump = new AtomicReference<>();

    /** Thread witch catch exception. */
    private static final AtomicReference<Thread> thr = new AtomicReference<>();

    /**
     * @return Affinity function to test.
     */
    private AffinityFunction affinityFunction() {
        return new RendezvousAffinityFunction();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite1 = startGrid(0);
        ignite2 = startGrid(1);
        ignite3 = startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setIpFinder(ipFinder);
        iCfg.setRebalanceThreadPoolSize(2);

        return iCfg;
    }

    /**
     * Main goal this test, it create more exchange messages between nodes, and stop
     * nodes before all exchange future completed. There are case when cleanUp method
     * cleaned reference before future completed. This test checking that it isn't happened.
     */
    public void testShakeExchange() throws Exception {

        final int delta = 5;

        // add handler for all exchange workers
        for (Thread worker : getExchangeWorkerThread()) {
            worker.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override public void uncaughtException(Thread t, Throwable e) {
                    dump.compareAndSet(null, U.dumpThreads());
                    exc.compareAndSet(null, e);
                    thr.compareAndSet(null, t);
                }
            });
        }

        // start async shake create/put/destroy cache
        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {

                for (int start = 0; Ignition.allGrids().contains(ignite1); start += delta) {
                    fillWithCache(ignite2, delta, start, affinityFunction());

                    for (String victim : ignite2.cacheNames())
                        ignite2.getOrCreateCache(victim).put(start, delta);

                    for (String victim : ignite1.cacheNames())
                        ignite1.destroyCache(victim);
                }
                return null;
            }
        }, "CacheShaker");

        for (int i = delta; i < ITERATIONS + delta; i++)
            startGrid(i);

        U.sleep(500);

        for (int i = delta; i < ITERATIONS + delta; i++)
            stopGrid(i);

        // check if was catch exception from exchange-worker thread
        if (exc.get() != null) {
            log.info(dump.toString());

            log.info(thr.get().getName());

            Throwable e = exc.get();
            log.error(e.getMessage(), e);

            exc.set(null);
            dump.set(null);
            thr.set(null);

            fail("see all log");
        }
    }

    /**
     * Get all exchange-worker threads
     */
    private Iterable<Thread> getExchangeWorkerThread() {
        Collection<Thread> exhcWorkers = new ArrayList<>();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains("exchange-worker"))
                exhcWorkers.add(t);
        }
        return exhcWorkers;
    }

    /** Put 2 * {@code iterations} caches inside ignite. */
    private  void fillWithCache(Ignite ignite, int iterations, int start, AffinityFunction affinityFunction) {
        for (int i = start; i < iterations + start; i++) {
            CacheConfiguration<Integer, Integer> cachePCfg = new CacheConfiguration<>();

            cachePCfg.setName(CACHE_NAME_DHT_PARTITIONED + i);
            cachePCfg.setCacheMode(CacheMode.PARTITIONED);
            cachePCfg.setBackups(1);
            cachePCfg.setAffinity(affinityFunction);

            ignite.getOrCreateCache(cachePCfg);

            CacheConfiguration<Integer, Integer> cacheRCfg = new CacheConfiguration<>();

            cacheRCfg.setName(CACHE_NAME_DHT_REPLICATED + i);
            cacheRCfg.setCacheMode(CacheMode.REPLICATED);
            cachePCfg.setBackups(0);
            cachePCfg.setAffinity(affinityFunction);

            ignite.getOrCreateCache(cacheRCfg);
        }
    }
}
