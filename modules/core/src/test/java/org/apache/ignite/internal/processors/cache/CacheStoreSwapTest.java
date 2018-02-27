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

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;

/**
 * Checks that loaded entries from store are swapped if need.
 */
public class CacheStoreSwapTest extends GridCommonAbstractTest {
    /** Swap directory. */
    private final static String SWAP_DIR;

    /** Cache name. */
    public static final String CACHE_NAME = "test";

    /** Map. */
    private static final ConcurrentHashMap8<Integer, Value> MAP = new ConcurrentHashMap8<>();

    static {
        try {
            SWAP_DIR = U.defaultWorkDirectory() + File.separatorChar + "swap";
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        U.delete(new File(SWAP_DIR));

        MAP.clear();

        for (int i = 0; i < 500; i++)
            MAP.put(i, new Value());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        U.delete(new File(SWAP_DIR));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(gridName.contains("client"));

        FileSwapSpaceSpi swapSpi = new FileSwapSpaceSpi();

        swapSpi.setBaseDirectory(SWAP_DIR);

        cfg.setSwapSpaceSpi(swapSpi);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        ccfg.setSwapEnabled(true);
        ccfg.setCopyOnRead(false);
        ccfg.setBackups(1);
        ccfg.setMemoryMode(OFFHEAP_TIERED);
        //noinspection unchecked
        ccfg.setCacheStoreFactory(singletonFactory(new TestStore()));
        ccfg.setReadThrough(true);
        ccfg.setStatisticsEnabled(true);
        ccfg.setOffHeapMaxMemory(100 * 1024);

        cfg.setCacheConfiguration(ccfg);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(LOCAL_IP_FINDER);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAll() throws Exception {
        startGridsMultiThreaded(2, true);
        Ignite client = startGrid("client");

        IgniteCache<Integer, Value> cache = client.cache(CACHE_NAME);

        Map<Integer, Value> all = cache.getAll(MAP.keySet());

        assertEquals(MAP, all);

        CacheMetrics metrics = cache.metrics();

        assert metrics.getSwapSize() > 0 : "Loaded entries must be swapped, [swapSize=" + metrics.getSwapSize() + ']';
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter<Integer, Value> {
        /** {@inheritDoc} */
        @Override public Value load(Integer key) throws CacheLoaderException {
            return MAP.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(
            Cache.Entry<? extends Integer, ? extends Value> entry) throws CacheWriterException {
            MAP.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            //noinspection SuspiciousMethodCalls
            MAP.remove(key);
        }
    }

    /**
     *
     */
    private static class Value {
        /** Data. */
        private final byte[] data = new byte[1024 * 1024];

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Value val = (Value)o;

            return Arrays.equals(data, val.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Arrays.hashCode(data);
        }
    }
}
