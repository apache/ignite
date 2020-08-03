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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheNoSyncForGetTest extends GridCommonAbstractTest {
    /** */
    private static volatile CountDownLatch processorStartLatch;

    /** */
    private static volatile CountDownLatch hangLatch;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        startClientGrid(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicGet() throws Exception {
        getTest(ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxGet() throws Exception {
        getTest(TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMvccTxGet() throws Exception {
        getTest(TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    private void getTest(CacheAtomicityMode atomicityMode) throws Exception {
        boolean getAll[] = {true, false};
        boolean cfgExpiryPlc[] = {false};
        boolean withExpiryPlc[] = {false};
        boolean heapCache[] = {false};

        for (boolean getAll0 : getAll) {
            for (boolean expiryPlc0 : cfgExpiryPlc) {
                for (boolean withExpiryPlc0 : withExpiryPlc) {
                    for (boolean heapCache0 : heapCache)
                        doGet(atomicityMode, heapCache0, getAll0, expiryPlc0, withExpiryPlc0);
                }
            }
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param heapCache Heap cache flag.
     * @param getAll Test getAll flag.
     * @param cfgExpiryPlc Configured expiry policy flag.
     * @param withExpiryPlc Custom expiry policy flag.
     * @throws Exception If failed.
     */
    private void doGet(CacheAtomicityMode atomicityMode,
        boolean heapCache,
        final boolean getAll,
        final boolean cfgExpiryPlc,
        final boolean withExpiryPlc) throws Exception {
        log.info("Test get [getAll=" + getAll + ", cfgExpiryPlc=" + cfgExpiryPlc + ']');

        Ignite srv = ignite(0);

        Ignite client = ignite(1);

        final IgniteCache cache = client.createCache(cacheConfiguration(atomicityMode, heapCache, cfgExpiryPlc));

        final Map<Object, Object> data = new HashMap<>();

        data.put(1, 1);
        data.put(2, 2);

        try {
            // Get from compute closure.
            {
                cache.putAll(data);

                hangLatch = new CountDownLatch(1);
                processorStartLatch = new CountDownLatch(1);

                IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        if (getAll)
                            cache.invokeAll(data.keySet(), new HangEntryProcessor());
                        else
                            cache.invoke(1, new HangEntryProcessor());

                        return null;
                    }
                });

                try {
                    boolean wait = processorStartLatch.await(30, TimeUnit.SECONDS);

                    assertTrue(wait);

                    if (getAll) {
                        assertEquals(data, client.compute().affinityCall(cache.getName(), 1,
                            new GetAllClosure(data.keySet(), cache.getName(), withExpiryPlc)));
                    }
                    else {
                        assertEquals(1, client.compute().affinityCall(cache.getName(), 1,
                            new GetClosure(1, cache.getName(), withExpiryPlc)));
                    }

                    hangLatch.countDown();

                    fut.get();
                }
                finally {
                    hangLatch.countDown();
                }
            }

            // Local get.
            {
                cache.putAll(data);

                hangLatch = new CountDownLatch(1);
                processorStartLatch = new CountDownLatch(1);

                IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        if (getAll)
                            cache.invokeAll(data.keySet(), new HangEntryProcessor());
                        else
                            cache.invoke(1, new HangEntryProcessor());

                        return null;
                    }
                });

                try {
                    boolean wait = processorStartLatch.await(30, TimeUnit.SECONDS);

                    assertTrue(wait);

                    IgniteCache srvCache = srv.cache(cache.getName());

                    if (withExpiryPlc)
                        srvCache = srvCache.withExpiryPolicy(ModifiedExpiryPolicy.factoryOf(Duration.FIVE_MINUTES).create());

                    if (getAll) {
                        assertEquals(data, srvCache.getAll(data.keySet()));
                        assertEquals(data.size(), srvCache.getEntries(data.keySet()).size());
                    }
                    else {
                        assertEquals(1, srvCache.get(1));
                        assertEquals(1, srvCache.getEntry(1).getValue());
                    }

                    hangLatch.countDown();

                    fut.get();
                }
                finally {
                    hangLatch.countDown();
                }
            }
        }
        finally {
            client.destroyCache(cache.getName());
        }
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @param heapCache Heap cache flag.
     * @param expiryPlc Expiry policy flag.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CacheAtomicityMode atomicityMode,
        boolean heapCache,
        boolean expiryPlc) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setOnheapCacheEnabled(heapCache);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setName("testCache");

        if (expiryPlc)
            ccfg.setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(Duration.FIVE_MINUTES));

        return ccfg;
    }

    /**
     *
     */
    static class HangEntryProcessor implements CacheEntryProcessor {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry entry, Object... arguments) {
            assert processorStartLatch != null;
            assert hangLatch != null;

            try {
                processorStartLatch.countDown();

                if (!hangLatch.await(60, TimeUnit.SECONDS))
                    throw new RuntimeException("Failed to wait for latch");
            }
            catch (Exception e) {
                System.out.println("Unexpected error: " + e);

                throw new EntryProcessorException(e);
            }

            entry.setValue(U.currentTimeMillis());

            return null;
        }
    }

    /**
     *
     */
    public static class GetClosure implements IgniteCallable<Object> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private final int key;

        /** */
        private final String cacheName;

        /** */
        private final boolean withExpiryPlc;

        /**
         * @param key Key.
         * @param cacheName Cache name.
         * @param withExpiryPlc Custom expiry policy flag.
         */
        GetClosure(int key, String cacheName, boolean withExpiryPlc) {
            this.key = key;
            this.cacheName = cacheName;
            this.withExpiryPlc = withExpiryPlc;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            IgniteCache cache = ignite.cache(cacheName);

            if (withExpiryPlc)
                cache = cache.withExpiryPolicy(ModifiedExpiryPolicy.factoryOf(Duration.FIVE_MINUTES).create());

            Object val = cache.get(key);

            CacheEntry e = cache.getEntry(key);

            assertEquals(val, e.getValue());

            return val;
        }
    }

    /**
     *
     */
    public static class GetAllClosure implements IgniteCallable<Object> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private final Set<Object> keys;

        /** */
        private final String cacheName;

        /** */
        private final boolean withExpiryPlc;

        /**
         * @param keys Keys.
         * @param cacheName Cache name.
         * @param withExpiryPlc Custom expiry policy flag.
         */
        GetAllClosure(Set<Object> keys, String cacheName, boolean withExpiryPlc) {
            this.keys = keys;
            this.cacheName = cacheName;
            this.withExpiryPlc = withExpiryPlc;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            IgniteCache cache = ignite.cache(cacheName);

            if (withExpiryPlc)
                cache = cache.withExpiryPolicy(ModifiedExpiryPolicy.factoryOf(Duration.FIVE_MINUTES).create());

            Map vals = cache.getAll(keys);

            Collection<CacheEntry> entries = cache.getEntries(keys);

            assertEquals(vals.size(), entries.size());

            for (CacheEntry entry : entries) {
                Object val = vals.get(entry.getKey());

                assertEquals(val, entry.getValue());
            }

            return vals;
        }
    }
}
