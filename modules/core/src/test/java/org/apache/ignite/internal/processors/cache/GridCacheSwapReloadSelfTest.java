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

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.swapspace.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.*;
import javax.cache.configuration.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.IgniteEventType.*;
import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test that swap is released after entry is reloaded.
 */
public class GridCacheSwapReloadSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        FileSwapSpaceSpi swap = new FileSwapSpaceSpi();

        swap.setWriteBufferSize(1);

        cfg.setSwapSpaceSpi(swap);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(new TestStore()));
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReload() throws Exception {
        final CountDownLatch swapLatch = new CountDownLatch(1);
        final CountDownLatch unswapLatch = new CountDownLatch(1);

        grid().events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                switch (evt.type()) {
                    case EVT_SWAP_SPACE_DATA_STORED:
                        swapLatch.countDown();

                        break;

                    case EVT_SWAP_SPACE_DATA_REMOVED:
                        unswapLatch.countDown();

                        break;

                    case EVT_SWAP_SPACE_DATA_EVICTED:
                        assert false : "Data eviction happened.";

                        break;

                    default:
                        assert false;
                }

                return true;
            }
        }, EVT_SWAP_SPACE_DATA_STORED, EVT_SWAP_SPACE_DATA_REMOVED, EVT_SWAP_SPACE_DATA_EVICTED);

        assert swap() != null;

        assert cache().putx("key", "val");

        assert swap().size(spaceName()) == 0;

        assert cache().evict("key");

        assert swapLatch.await(1, SECONDS);
        Thread.sleep(100);

        assert swap().count(spaceName()) == 1;
        assert swap().size(spaceName()) > 0;

        assert "val".equals(cache().reload("key"));

        assert unswapLatch.await(1, SECONDS);

        assert swap().count(spaceName()) == 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReloadAll() throws Exception {
        final CountDownLatch swapLatch = new CountDownLatch(2);
        final CountDownLatch unswapLatch = new CountDownLatch(2);

        grid().events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                switch (evt.type()) {
                    case EVT_SWAP_SPACE_DATA_STORED:
                        swapLatch.countDown();

                        break;

                    case EVT_SWAP_SPACE_DATA_REMOVED:
                        unswapLatch.countDown();

                        break;

                    default:
                        assert false;
                }

                return true;
            }
        }, EVT_SWAP_SPACE_DATA_STORED, EVT_SWAP_SPACE_DATA_REMOVED);

        assert swap() != null;

        assert cache().putx("key1", "val1");
        assert cache().putx("key2", "val2");

        assert swap().size(spaceName()) == 0;

        assert cache().evict("key1");
        assert cache().evict("key2");

        assert swapLatch.await(1, SECONDS);
        Thread.sleep(100);

        assert swap().count(spaceName()) == 2;
        assert swap().size(spaceName()) > 0 : swap().size(spaceName());

        cache().reloadAll(F.asList("key1", "key2"));

        assert unswapLatch.await(1, SECONDS);

        assert swap().count(spaceName()) == 0;
    }

    /**
     * @return Swap space SPI.
     */
    private SwapSpaceSpi swap() {
        return grid().configuration().getSwapSpaceSpi();
    }

    /**
     * @return Swap space name.
     */
    private String spaceName() {
        return CU.swapSpaceName(((GridKernal)grid()).internalCache().context());
    }

    /**
     * Test store.
     */
    private static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** */
        private Map<Object, Object> map = new ConcurrentHashMap<>();

        /** */
        void reset() {
            map.clear();
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Object, ? extends Object> entry) {
            map.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            map.remove(key);
        }
    }
}
