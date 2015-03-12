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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 * Test for distributed queries with node restarts.
 */
public class GridCacheQueryNodeRestartSelfTest extends GridCacheAbstractSelfTest {
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

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setDistributionMode(NEAR_PARTITIONED);

        CacheQueryConfiguration qcfg = new CacheQueryConfiguration();

        qcfg.setIndexPrimitiveKey(true);

        cc.setQueryConfiguration(qcfg);

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
        final int logFreq = 20;

        final GridCache<Integer, Integer> cache = ((IgniteKernal)grid(0)).cache(null);

        assert cache != null;

        for (int i = 0; i < KEY_CNT; i++)
            cache.put(i, i);

        assertEquals(KEY_CNT, cache.size());

        final AtomicInteger qryCnt = new AtomicInteger();

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                while (!done.get()) {
                    CacheQuery<Map.Entry<Integer, Integer>> qry =
                        cache.queries().createSqlQuery(Integer.class, "_val >= 0");

                    qry.includeBackups(true);
                    qry.keepAll(true);

                    assertFalse(qry.execute().get().isEmpty());

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

        done.set(true);

        fut1.get();
        fut2.get();

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
