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

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Basic get and transform store test.
 */
public abstract class GridCacheGetAndTransformStoreAbstractTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache store. */
    private static final GridCacheTestStore store = new GridCacheTestStore();

    /**
     *
     */
    protected GridCacheGetAndTransformStoreAbstractTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        store.resetTimestamp();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        jcache().clear();

        store.reset();
    }

    /** @return Caching mode. */
    protected abstract CacheMode cacheMode();

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(atomicityMode());
        cc.setRebalanceMode(SYNC);

        cc.setCacheStoreFactory(singletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndTransform() throws Exception {
        final AtomicBoolean finish = new AtomicBoolean();

        try {
            startGrid(0);
            startGrid(1);
            startGrid(2);

            final Processor entryProcessor = new Processor();

            IgniteInternalFuture<?> fut = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        IgniteCache<Integer, String> c = jcache(ThreadLocalRandom.current().nextInt(3));

                        while (!finish.get() && !Thread.currentThread().isInterrupted()) {
                            c.get(ThreadLocalRandom.current().nextInt(100));

                            c.put(ThreadLocalRandom.current().nextInt(100), "s");

                            c.invoke(
                                ThreadLocalRandom.current().nextInt(100),
                                entryProcessor);
                        }

                        return null;
                    }
                },
                20);

            Thread.sleep(15_000);

            finish.set(true);

            fut.get();
        }
        finally {
            stopGrid(0);
            stopGrid(1);
            stopGrid(2);

            while (jcache().localSize() != 0)
                jcache().clear();
        }
    }

    /**
     *
     */
    private static class Processor implements EntryProcessor<Integer, String, Void>, Serializable {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, String> e, Object... args) {
            e.setValue("str");

            return null;
        }
    }
}