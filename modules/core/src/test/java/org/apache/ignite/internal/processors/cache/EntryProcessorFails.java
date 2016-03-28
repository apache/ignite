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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.cache.Cache.Entry;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;

/**
 *
 */
public class EntryProcessorFails {
    /**
     * @param cacheName Cache anme.
     * @return Cache configuration.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private CacheConfiguration<String, List<Double>> createCacheConfiguration(String cacheName) {
        CacheConfiguration<String, List<Double>> cacheConfiguration = new CacheConfiguration<>();

        cacheConfiguration.setName(cacheName);
        cacheConfiguration.setCacheMode(PARTITIONED);
        cacheConfiguration.setAtomicityMode(ATOMIC);
        cacheConfiguration.setAtomicWriteOrderMode(PRIMARY);
        cacheConfiguration.setWriteSynchronizationMode(PRIMARY_SYNC);

        cacheConfiguration.setReadThrough(true);
        cacheConfiguration.setWriteThrough(true);

        Factory cacheStoreFactory = new FactoryBuilder.SingletonFactory(new DummyCacheStore());
        cacheConfiguration.setCacheStoreFactory(cacheStoreFactory);

        return cacheConfiguration;
    }

    /**
     * @param gridName Grid name.
     * @return Ignite config.
     * @throws IgniteException If failed.
     */
    private IgniteConfiguration getGridConfiguration(String gridName) throws IgniteException {
        IgniteConfiguration gridCfg = new IgniteConfiguration();
        gridCfg.setGridName(gridName);

        CacheConfiguration<String, List<Double>> cacheCfg = createCacheConfiguration("testCache");
        gridCfg.setCacheConfiguration(cacheCfg);

        gridCfg.setLocalHost("127.0.0.1");
        gridCfg.setDeploymentMode(SHARED);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47501", "127.0.0.1:47502"));

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(ipFinder);
        spi.setLocalAddress("127.0.0.1");
        spi.setLocalPort(47501);

        gridCfg.setDiscoverySpi(spi);

        return gridCfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void doCacheEntryProcessor() throws Exception {
        IgniteConfiguration gridCfg = getGridConfiguration("testGrid");

        try (Ignite grid = Ignition.start(gridCfg)) {
            final Set<String> testKeys = new HashSet<>();

            testKeys.add("testKey_1");
            testKeys.add("testKey_2");
            testKeys.add("testKey_3");

            final IgniteCache<String, List<Double>> testCache = grid.cache("testCache");

            EntryProcessor<String, List<Double>, List<Double>> entryProcessor = new DummyEntryProcessor();

            testCache.invokeAll(testKeys, entryProcessor);
        }
    }

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Running cache entry test.");

        EntryProcessorFails test = new EntryProcessorFails();

        test.doCacheEntryProcessor();

        System.out.println("Test complete!");
    }

    /**
     *
     */
    private static class DummyEntryProcessor implements EntryProcessor<String, List<Double>, List<Double>> {
        /** {@inheritDoc} */
        @Override public List<Double> process(MutableEntry<String, List<Double>> entry, Object... arguments)
            throws EntryProcessorException {
            List<Double> currVal = entry.getValue();

            if (currVal == null)
                entry.setValue(Arrays.asList(1.0, 1.0, 1.0, 1.0, 1.0));
            else {
                List<Double> newVal = new ArrayList<>();

                for (Double item : currVal)
                    newVal.add(item + 1);

                entry.setValue(newVal);
            }

            // returning null for test because we don't need result
            return null;
        }
    }

    /**
     *
     */
    @SuppressWarnings("serial")
    private static class DummyCacheStore extends CacheStoreAdapter<String, List<Double>> implements Serializable {
        /** {@inheritDoc} */
        @Override public List<Double> load(String key) throws CacheLoaderException {
            return new ArrayList<>(Arrays.asList(1.0, 1.0, 1.0, 1.0, 1.0));
        }

        /** {@inheritDoc} */
        @Override public void write(Entry<? extends String, ? extends List<Double>> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }
}
