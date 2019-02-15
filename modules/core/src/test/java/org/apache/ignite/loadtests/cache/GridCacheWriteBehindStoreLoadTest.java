/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.loadtests.cache;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Basic store test.
 */
@RunWith(JUnit4.class)
public class GridCacheWriteBehindStoreLoadTest extends GridCommonAbstractTest {
    /** Flush frequency. */
    private static final int WRITE_FROM_BEHIND_FLUSH_FREQUENCY = 1000;

    /** Run time is 24 hours. */
    private static final long runTime = 24L * 60 * 60 * 60 * 1000;

    /** Specify if test keys should be randomly generated. */
    private boolean rndKeys;

    /** Number of distinct keys if they are generated randomly. */
    private int keysCnt = 20 * 1024;

    /** Number of threads that concurrently update cache. */
    private int threadCnt;

    /** No-op cache store. */
    private static final CacheStore store = new CacheStoreAdapter() {
        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(javax.cache.Cache.Entry e) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    };

    /**
     * Constructor
     */
    public GridCacheWriteBehindStoreLoadTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgniteCache<Object, Object> cache = jcache();

        if (cache != null)
            cache.clear();
    }

    /**
     * @return Caching mode.
     */
    protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override protected final IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration c = super.getConfiguration();

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setCacheStoreFactory(singletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        cc.setWriteBehindEnabled(true);
        cc.setWriteBehindFlushFrequency(WRITE_FROM_BEHIND_FLUSH_FREQUENCY);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadCacheSequentialKeys() throws Exception {
        rndKeys = false;

        threadCnt = 10;

        loadCache();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadCacheRandomKeys() throws Exception {
        rndKeys = true;

        threadCnt = 10;

        loadCache();
    }

    /**
     * @throws Exception If failed.
     */
    private void loadCache() throws Exception {
        final AtomicBoolean running = new AtomicBoolean(true);

        final IgniteCache<Object, Object> cache = jcache();

        final AtomicLong keyCntr = new AtomicLong();

        long start = System.currentTimeMillis();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @SuppressWarnings({"NullableProblems"})
            @Override public void run() {

                Random rnd = new Random();

                while (running.get()) {
                    long putNum = keyCntr.incrementAndGet();

                    long key = rndKeys ? rnd.nextInt(keysCnt) : putNum;

                    cache.put(key, "val" + key);
                }
            }
        }, threadCnt, "put");

        long prevPutCnt = 0;

        while (System.currentTimeMillis() - start < runTime) {
            // Print stats every minute.
            U.sleep(60 * 1000);

            long cnt = keyCntr.get();
            long secondsElapsed = (System.currentTimeMillis() - start) / 1000;

            info(">>> Running for " + secondsElapsed + " seconds");
            info(">>> Puts: [total=" + cnt + ", avg=" + (cnt / secondsElapsed) + " (ops/sec), lastMinute=" +
                ((cnt - prevPutCnt) / 60) + "(ops/sec)]");

            prevPutCnt = cnt;
        }

        running.set(false);

        fut.get();
    }

    /**
     * @return Will return 0 to disable timeout.
     */
    @Override protected long getTestTimeout() {
        return 0;
    }
}
