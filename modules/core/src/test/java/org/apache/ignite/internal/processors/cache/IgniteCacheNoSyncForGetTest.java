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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 *
 */
public class IgniteCacheNoSyncForGetTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static volatile CountDownLatch processorStartLatch;

    /** */
    private static volatile CountDownLatch hangLatch;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        client = true;

        startGrid(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicGet() throws Exception {
        doGet(ATOMIC, ONHEAP_TIERED, false);

        doGet(ATOMIC, ONHEAP_TIERED, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicGetOffheap() throws Exception {
        doGet(ATOMIC, OFFHEAP_TIERED, false);

        doGet(ATOMIC, OFFHEAP_TIERED, true);
    }

    /**
     * @throws Exception If failed.
     */
    private void doGet(CacheAtomicityMode atomicityMode,
        CacheMemoryMode memoryMode,
        final boolean getAll) throws Exception {
        Ignite srv = ignite(0);

        Ignite client = ignite(1);

        final IgniteCache cache = client.createCache(cacheConfiguration(atomicityMode, memoryMode));

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
                            new GetAllClosure(data.keySet(), cache.getName())));
                    }
                    else {
                        assertEquals(1, client.compute().affinityCall(cache.getName(), 1,
                            new GetClosure(1, cache.getName())));
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

                    if (getAll)
                        assertEquals(data, srv.cache(cache.getName()).getAll(data.keySet()));
                    else
                        assertEquals(1, srv.cache(cache.getName()).get(1));

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
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CacheAtomicityMode atomicityMode, CacheMemoryMode memoryMode) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setMemoryMode(memoryMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setName("testCache");

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

        /**
         * @param key Key.
         */
        public GetClosure(int key, String cacheName) {
            this.key = key;
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return ignite.cache(cacheName).get(key);
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

        /**
         * @param keys Keys.
         */
        public GetAllClosure(Set<Object> keys, String cacheName) {
            this.keys = keys;
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return ignite.cache(cacheName).getAll(keys);
        }
    }
}
