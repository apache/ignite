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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * Test for distributed queries with node restarts.
 */
public class IgniteCacheQueryNodeRestartSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 3;

    /** */
    private static final int KEY_CNT = 1000;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 90 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration<?, ?> cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setRebalanceMode(SYNC);
        cc.setAffinity(new RendezvousAffinityFunction(false, 15));
        cc.setIndexedTypes(
            Integer.class, Integer.class
        );

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testRestarts() throws Exception {
        int duration = 60 * 1000;
        int qryThreadNum = 10;
        final long nodeLifeTime = 2 * 1000;
        final int logFreq = 50;

        final IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        assert cache != null;

        for (int i = 0; i < KEY_CNT; i++)
            cache.put(i, i);

        assertEquals(KEY_CNT, cache.size());

        final AtomicInteger qryCnt = new AtomicInteger();

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                while (!done.get()) {
                    Collection<Cache.Entry<Integer, Integer>> res =
                        cache.query(new SqlQuery<Integer, Integer>(Integer.class, "true")).getAll();

                    Set<Integer> keys = new HashSet<>();

                    for (Cache.Entry<Integer,Integer> entry : res)
                        keys.add(entry.getKey());

                    if (KEY_CNT > keys.size()) {
                        for (int i = 0; i < KEY_CNT; i++) {
                            if (!keys.contains(i))
                                assertEquals(Integer.valueOf(i), cache.get(i));
                        }

                        fail("res size: " + res.size());
                    }

                    assertEquals(KEY_CNT, keys.size());

                    int c = qryCnt.incrementAndGet();

                    if (c % logFreq == 0)
                        info("Executed queries: " + c);
                }
            }
        }, qryThreadNum);

        final AtomicInteger restartCnt = new AtomicInteger();

        CollectingEventListener lsnr = new CollectingEventListener();

        for (int i = 0; i < GRID_CNT; i++)
            grid(i).events().localListen(lsnr, EventType.EVT_CACHE_REBALANCE_STOPPED);

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Callable<Object>() {
            @SuppressWarnings({"BusyWait"})
            @Override public Object call() throws Exception {
                while (!done.get()) {
                    int idx = GRID_CNT;

                    startGrid(idx);

                    Thread.sleep(nodeLifeTime);

                    stopGrid(idx);

                    int c = restartCnt.incrementAndGet();

                    if (c % logFreq == 0)
                        info("Node restarts: " + c);
                }

                return true;
            }
        }, 1);

        Thread.sleep(duration);

        info("Stopping..");

        done.set(true);

        fut2.get();

        info("Restarts stopped.");

        fut1.get();

        info("Queries stopped.");


        info("Awaiting rebalance events [restartCnt=" + restartCnt.get() + ']');

        boolean success = lsnr.awaitEvents(GRID_CNT * 2 * restartCnt.get(), 15000);

        for (int i = 0; i < GRID_CNT; i++)
            grid(i).events().stopLocalListen(lsnr, EventType.EVT_CACHE_REBALANCE_STOPPED);

        assert success;
    }

    /** Listener that will wait for specified number of events received. */
    private class CollectingEventListener implements IgnitePredicate<Event> {
        /** Registered events count. */
        private int evtCnt;

        /** {@inheritDoc} */
        @Override public synchronized boolean apply(Event evt) {
            evtCnt++;

            info("Processed event [evt=" + evt + ", evtCnt=" + evtCnt + ']');

            notifyAll();

            return true;
        }

        /**
         * Waits until total number of events processed is equal or greater then argument passed.
         *
         * @param cnt Number of events to wait.
         * @param timeout Timeout to wait.
         * @return {@code True} if successfully waited, {@code false} if timeout happened.
         * @throws InterruptedException If thread is interrupted.
         */
        public synchronized boolean awaitEvents(int cnt, long timeout) throws InterruptedException {
            long start = U.currentTimeMillis();

            long now = start;

            while (start + timeout > now) {
                if (evtCnt >= cnt)
                    return true;

                wait(start + timeout - now);

                now = U.currentTimeMillis();
            }

            return false;
        }
    }
}