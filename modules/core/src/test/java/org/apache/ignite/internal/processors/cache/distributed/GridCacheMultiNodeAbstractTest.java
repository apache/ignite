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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Multi-node cache test.
 */
public abstract class GridCacheMultiNodeAbstractTest extends GridCommonAbstractTest {
    /** Grid 1. */
    private static Ignite ignite1;

    /** Grid 2. */
    private static Ignite ignite2;

    /** Grid 3. */
    private static Ignite ignite3;

    /** Cache 1. */
    private static GridCache<Integer, String> cache1;

    /** Cache 2. */
    private static GridCache<Integer, String> cache2;

    /** Cache 3. */
    private static GridCache<Integer, String> cache3;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Listeners. */
    private static Collection<CacheEventListener> lsnrs = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite1 = startGrid(1);
        ignite2 = startGrid(2);
        ignite3 = startGrid(3);

        cache1 = ignite1.cache(null);
        cache2 = ignite2.cache(null);
        cache3 = ignite3.cache(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cache1 = null;
        cache2 = null;
        cache3 = null;

        ignite1 = null;
        ignite2 = null;
        ignite3 = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        removeListeners(ignite1);
        removeListeners(ignite2);
        removeListeners(ignite3);

        lsnrs.clear();
    }

    /**
     * @param ignite Grid to remove listeners from.
     */
    private void removeListeners(Ignite ignite) {
        if (ignite != null)
            for (CacheEventListener lsnr : lsnrs) {
                assert lsnr.latch.getCount() == 0;

                ignite.events().stopLocalListen(lsnr);
            }
    }

    /**
     *
     * @param ignite Grid.
     * @param lsnr Listener.
     * @param type Event types.
     */
    private void addListener(Ignite ignite, CacheEventListener lsnr, int... type) {
        if (!lsnrs.contains(lsnr))
            lsnrs.add(lsnr);

        ignite.events().localListen(lsnr, type.length == 0 ? EVTS_CACHE : type);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testBasicPut() throws Exception {
        checkPuts(3, ignite1);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiNodePut() throws Exception {
        checkPuts(1, ignite1, ignite2, ignite3);
        checkPuts(1, ignite2, ignite1, ignite3);
        checkPuts(1, ignite3, ignite1, ignite2);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiValuePut() throws Exception {
        checkPuts(1, ignite1);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiValueMultiNodePut() throws Exception {
        checkPuts(3, ignite1, ignite2, ignite3);
        checkPuts(3, ignite2, ignite1, ignite3);
        checkPuts(3, ignite3, ignite1, ignite2);
    }

    /**
     * Checks cache puts.
     *
     * @param cnt Count of puts.
     * @param ignites Grids.
     * @throws Exception If check fails.
     */
    private void checkPuts(int cnt, Ignite... ignites) throws Exception {
        CountDownLatch latch = new CountDownLatch(ignites.length * cnt);

        CacheEventListener lsnr = new CacheEventListener(latch, EVT_CACHE_OBJECT_PUT);

        for (Ignite ignite : ignites)
            addListener(ignite, lsnr);

        GridCache<Integer, String> cache1 = ignites[0].cache(null);

        for (int i = 1; i <= cnt; i++)
            cache1.put(i, "val" + i);

        for (int i = 1; i <= cnt; i++) {
            String v = cache1.get(i);

            assert v != null;
            assert v.equals("val" + i);
        }

        latch.await(10, SECONDS);

        for (Ignite ignite : ignites) {
            GridCache<Integer, String> cache = ignite.cache(null);

            if (cache == cache1)
                continue;

            for (int i = 1; i <= cnt; i++) {
                String v = cache.get(i);

                assert v != null;
                assert v.equals("val" + i);
            }
        }

        assert !cache1.isLocked(1);
        assert !cache1.isLocked(2);
        assert !cache1.isLocked(3);

        for (Ignite ignite : ignites)
            ignite.events().stopLocalListen(lsnr);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLockUnlock() throws Exception {
        CacheEventListener lockLsnr1 = new CacheEventListener(ignite1, new CountDownLatch(1), EVT_CACHE_OBJECT_LOCKED);

        addListener(ignite1, lockLsnr1, EVT_CACHE_OBJECT_LOCKED);

        CacheEventListener unlockLsnr = new CacheEventListener(new CountDownLatch(3), EVT_CACHE_OBJECT_UNLOCKED);

        addListener(ignite1, unlockLsnr, EVT_CACHE_OBJECT_UNLOCKED);
        addListener(ignite2, unlockLsnr, EVT_CACHE_OBJECT_UNLOCKED);
        addListener(ignite3, unlockLsnr, EVT_CACHE_OBJECT_UNLOCKED);

        IgniteInternalFuture<Boolean> f1 = cache1.lockAsync(1, 0L);

        assert f1.get(10000);

        assert cache1.isLocked(1);
        assert cache2.isLocked(1);
        assert cache3.isLocked(1);

        assert cache1.isLockedByThread(1);
        assert !cache2.isLockedByThread(1);
        assert !cache3.isLockedByThread(1);

        info("Acquired lock for cache1.");

        cache1.unlockAll(F.asList(1));

        Thread.sleep(50);

        unlockLsnr.latch.await(10, SECONDS);

        assert !cache1.isLocked(1);
        assert !cache2.isLocked(2);
        assert !cache3.isLocked(3);

        assert !cache1.isLockedByThread(1);
        assert !cache2.isLockedByThread(1);
        assert !cache3.isLockedByThread(1);
    }

    /**
     * Concurrent test for asynchronous locks.
     *
     * @throws Exception If test fails.
     */
    @SuppressWarnings({"BusyWait"})
    public void testConcurrentLockAsync() throws Exception {
        CacheEventListener unlockLsnr = new CacheEventListener(new CountDownLatch(9), EVT_CACHE_OBJECT_UNLOCKED);

        addListener(ignite1, unlockLsnr, EVT_CACHE_OBJECT_UNLOCKED);
        addListener(ignite2, unlockLsnr, EVT_CACHE_OBJECT_UNLOCKED);
        addListener(ignite3, unlockLsnr, EVT_CACHE_OBJECT_UNLOCKED);

        IgniteInternalFuture<Boolean> f1 = cache1.lockAsync(1, 0L);
        IgniteInternalFuture<Boolean> f2 = cache2.lockAsync(1, 0L);
        IgniteInternalFuture<Boolean> f3 = cache3.lockAsync(1, 0L);

        boolean l1 = false;
        boolean l2 = false;
        boolean l3 = false;

        int cnt = 0;

        while (!l1 || !l2 || !l3) {
            if (!l1 && f1.isDone()) {
                assert cache1.isLocked(1);
                assert cache2.isLocked(1);
                assert cache3.isLocked(1);

                assert cache1.isLockedByThread(1);
                assert !cache2.isLockedByThread(1);
                assert !cache3.isLockedByThread(1);

                info("Acquired lock for cache1.");

                cache1.unlockAll(F.asList(1));

                l1 = true;
            }

            if (!l2 && f2.isDone()) {
                assert cache1.isLocked(1);
                assert cache2.isLocked(1);
                assert cache3.isLocked(1);

                assert !cache1.isLockedByThread(1);
                assert cache2.isLockedByThread(1);
                assert !cache3.isLockedByThread(1);

                info("Acquired lock for cache2.");

                cache2.unlockAll(F.asList(1));

                l2 = true;
            }

            if (!l3 && f3.isDone()) {
                assert cache1.isLocked(1);
                assert cache2.isLocked(1);
                assert cache3.isLocked(1);

                assert !cache1.isLockedByThread(1);
                assert !cache2.isLockedByThread(1);
                assert cache3.isLockedByThread(1);

                info("Acquired lock for cache3.");

                cache3.unlockAll(F.asList(1));

                l3 = true;
            }

            info("Acquired locks [cnt=" + ++cnt + ", l1=" + l1 + ", l2=" + l2 + ", l3=" + l3 + ']');

            Thread.sleep(50);
        }

        unlockLsnr.latch.await(10, SECONDS);

        assert !cache1.isLocked(1);
        assert !cache2.isLocked(2);
        assert !cache3.isLocked(3);

        assert !cache1.isLockedByThread(1);
        assert !cache2.isLockedByThread(1);
        assert !cache3.isLockedByThread(1);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testConcurrentPutAsync() throws Exception {
        CountDownLatch latch = new CountDownLatch(9);

        CacheEventListener lsnr = new CacheEventListener(latch, EVT_CACHE_OBJECT_PUT);

        addListener(ignite1, lsnr);
        addListener(ignite2, lsnr);
        addListener(ignite3, lsnr);

        IgniteInternalFuture<String> f1 = cache1.putAsync(2, "val1");
        IgniteInternalFuture<String> f2 = cache2.putAsync(2, "val2");
        IgniteInternalFuture<String> f3 = cache3.putAsync(2, "val3");

        String v1 = f1.get(20000);

        info("Got v1 from future1: " + v1);

        String v2 = f2.get(20000);

        info("Got v2 from future2: " + v2);

        String v3 = f3.get(20000);

        info("Got v3 from future3: " + v3);

        latch.await(60, SECONDS);

        info("Woke up from latch: " + latch);

        v1 = cache1.get(1);
        v2 = cache2.get(1);
        v3 = cache3.get(1);

        info("Cache1 value for key 1: " + v1);
        info("Cache2 value for key 1: " + v2);
        info("Cache3 value for key 1: " + v3);

        assert v1 != null;
        assert v2 != null;
        assert v3 != null;

        assert v1.equals(v2) : "Mismatch [v1=" + v1 + ", v2=" + v2 + ']';
        assert v1.equals(v3) : "Mismatch [v1=" + v1 + ", v3=" + v3 + ']';
    }

    /**
     * @throws Exception If test failed.
     */
    public void testGlobalClearAll() throws Exception {
        cache1.put(1, "val1");
        cache2.put(2, "val2");
        cache3.put(3, "val3");

        assert cache1.size() == 3;
        assert cache2.size() == 3;
        assert cache3.size() == 3;

        cache1.globalClearAll();

        assert cache1.isEmpty();
        assert cache2.isEmpty();
        assert cache3.isEmpty();
    }

    /**
     * Event listener.
     */
    private class CacheEventListener implements IgnitePredicate<IgniteEvent> {
        /** */
        @GridToStringExclude
        private final Ignite ignite;

        /** Wait latch. */
        @GridToStringExclude
        private CountDownLatch latch;

        /** Events to accept. */
        private final List<Integer> evts;

        /**
         * @param latch Wait latch.
         * @param evts Events.
         */
        CacheEventListener(CountDownLatch latch, Integer... evts) {
            this.latch = latch;

            ignite = null;

            assert evts.length > 0;

            this.evts = Arrays.asList(evts);
        }

        /**
         * @param ignite Grid.
         * @param latch Wait latch.
         * @param evts Events.
         */
        CacheEventListener(Ignite ignite, CountDownLatch latch, Integer... evts) {
            this.ignite = ignite;
            this.latch = latch;

            assert evts.length > 0;

            this.evts = Arrays.asList(evts);
        }

        /**
         * @param latch New latch.
         */
        void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(IgniteEvent evt) {
            info("Grid cache event [type=" + evt.type() + ", latch=" + latch.getCount() + ", evt=" + evt + ']');

            if (evts.contains(evt.type()))
                if (ignite == null || evt.node().id().equals(ignite.cluster().localNode().id())) {
                    if (latch.getCount() > 0)
                        latch.countDown();
                    else
                        info("Received unexpected cache event: " + evt);
                }

            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheEventListener.class, this, "latchCount", latch.getCount(),
                "grid", ignite != null ? ignite.name() : "N/A", "evts", evts);
        }
    }
}
