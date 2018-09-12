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

package org.apache.ignite.internal.processors.cache.eviction;

import java.io.Serializable;
import java.util.Collections;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicyFactory;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Checking that DHT and near cache evictions work correctly when both are set.
 *
 * This is a regression test for IGNITE-9315.
 */
public class DhtAndNearEvictionTest extends GridCommonAbstractTest {
    /** */
    public GridStringLogger strLog;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setGridLogger(strLog);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singleton("127.0.0.1:47500..47501"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        return cfg;
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        strLog = new GridStringLogger(false, log);
        strLog.logLength(1024 * 1024);
    }

    /** */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Checking the case that provokes usage of
     * GridCacheUtils.createBackupPostProcessingClosure.BackupPostProcessingClosure
     * which used to be affected by IGNITE-9315:
     * <ul>
     *     <li>2 nodes, one writing, one reading</li>
     *     <li>cache store with read-through</li>
     *     <li>backups=1</li>
     * </ul>
     */
    public void testConcurrentWritesAndReadsWithReadThrough() throws Exception {
        startGrid(0);
        startGrid(1);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<Integer, Integer>("mycache")
            .setOnheapCacheEnabled(true)
            .setEvictionPolicyFactory(new LruEvictionPolicyFactory<>(500))
            .setNearConfiguration(
                new NearCacheConfiguration<Integer, Integer>()
                    .setNearEvictionPolicyFactory(new LruEvictionPolicyFactory<>(100))
            )
            .setReadThrough(true)
            .setCacheStoreFactory(DummyCacheStore.factoryOf())
            .setBackups(1);

        grid(0).createCache(ccfg);

        IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(() -> {
            IgniteCache<Integer, Integer> cache = grid(0).cache("mycache");

            for (int i = 0; i < 1000; i++)
                cache.put(i, i);

            return null;
        });

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(() -> {
            IgniteCache<Integer, Integer> cache = grid(1).cache("mycache");

            for (int i = 0; i < 1000; i++)
                cache.get(i);

            return null;
        });

        // AssertionError may leave the node hanging.
        // Because of that, wait until either the futures are done or the log contains an error.
        while (!fut1.isDone() || !fut2.isDone()) {
            assertFalse(strLog.toString().contains("AssertionError"));

            Thread.sleep(1000);
        }

        fut1.get();
        fut2.get();

        assertFalse(strLog.toString().contains("AssertionError"));
    }

    /**
     * Checking rebalancing which used to be affected by IGNITE-9315.
     */
    public void testRebalancing() throws Exception {
        Ignite grid0 = startGrid(0);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>("mycache")
            .setOnheapCacheEnabled(true)
            .setEvictionPolicyFactory(new LruEvictionPolicyFactory<>(500))
            .setNearConfiguration(
                new NearCacheConfiguration<Integer, Integer>()
                    .setNearEvictionPolicyFactory(new LruEvictionPolicyFactory<>(100))
            );

        IgniteCache<Integer, Integer> cache = grid0.createCache(ccfg);

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        startGrid(1);

        awaitPartitionMapExchange(true, true, null);

        assertFalse(strLog.toString().contains("AssertionError"));
    }

    /** */
    private static class DummyCacheStore extends CacheStoreAdapter<Integer, Integer> implements Serializable {
        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void write(
            Cache.Entry<? extends Integer, ? extends Integer> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }

        /** */
        public static Factory<DummyCacheStore> factoryOf() {
            return new Factory<DummyCacheStore>() {
                @Override public DummyCacheStore create() {
                    return new DummyCacheStore();
                }
            };
        }
    }
}
