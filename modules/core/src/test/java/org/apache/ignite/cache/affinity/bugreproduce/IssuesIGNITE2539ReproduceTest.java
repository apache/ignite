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

package org.apache.ignite.cache.affinity.bugreproduce;

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
 * Created by dgovorukhin on 01.09.2016.
 */
public class IssuesIGNITE2539ReproduceTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int ITERATIONS = 10;

    /** partitioned cache name. */
    private static final String CACHE_NAME_DHT_PARTITIONED = "cacheP";

    /** replicated cache name. */
    private static final String CACHE_NAME_DHT_REPLICATED = "cacheR";

    /** Ignite. */
    private static Ignite ignite1;

    /** Ignite. */
    private static Ignite ignite2;

    /** Ignite. */
    private static Ignite ignite3;

    private static final AtomicReference<Throwable> exc = new AtomicReference<>();

    private static final AtomicReference<GridStringBuilder> dump = new AtomicReference<>();

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
     *
     */
    public void testCacheStopping() throws Exception {

        final int delta = 5;

        for (Thread worker : getExchangeWorkerThread()) {
            worker.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override public void uncaughtException(Thread t, Throwable e) {
                    dump.compareAndSet(null, U.dumpThreads());
                    exc.compareAndSet(null, e);
                    thr.compareAndSet(null, t);
                }
            });
        }

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
        }, "CacheSerialKiller");

        for (int i = delta; i < ITERATIONS + delta; i++)
            startGrid(i);

        U.sleep(500);

        for (int i = delta; i < ITERATIONS + delta; i++)
            stopGrid(i);

        if (exc.get() != null) {
            log.info(thr.get().getName());

            Throwable e = exc.get();
            log.error(e.getMessage(), e);

            log.info(dump.toString());

            exc.set(null);
            dump.set(null);
            thr.set(null);

            fail("see all log");
        }
    }

    /**
     *
     */
    public void testCacheStopping2() throws Exception {

        final int delta = 5;

        int itr = 20;

        for (int j = 0; j < itr; j++) {

            final AtomicReference<Throwable> exc = new AtomicReference<>();

            final AtomicBoolean asyncRun = new AtomicBoolean(true);

            for (Thread worker : getExchangeWorkerThread()) {
                worker.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override public void uncaughtException(Thread t, Throwable e) {
                        exc.set(e);
                    }
                });
            }

            Thread thread = new Thread(new Runnable() {
                @Override public void run() {
                    for (int start = 0; asyncRun.get(); start += delta) {
                        fillWithCache(ignite2, delta, start, affinityFunction());

                        for (String victim : ignite2.cacheNames())
                            ignite2.getOrCreateCache(victim).put(start, delta);

                        for (String victim : ignite1.cacheNames())
                            ignite1.destroyCache(victim);
                    }
                }
            });
            thread.start();

            for (int i = delta; i < ITERATIONS + delta; i++)
                startGrid(i);

            U.sleep(500);

            for (int i = delta; i < ITERATIONS + delta; i++)
                stopGrid(i);

            asyncRun.set(false);

            thread.join();

            if (exc.get() != null)
                fail(exc.get().getMessage());

        }
    }

    /**
     *
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
    private static void fillWithCache(Ignite ignite, int iterations, int start, AffinityFunction affinityFunction) {
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
