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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;

/**
 *
 */
public class EntryProcessorFailsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(createCacheConfiguration());

        cfg.setLocalHost("127.0.0.1");
        cfg.setDeploymentMode(SHARED);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47501", "127.0.0.1:47502"));

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(ipFinder);
        spi.setLocalAddress("127.0.0.1");
        spi.setLocalPort(47501);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private CacheConfiguration<String, List<Double>> createCacheConfiguration() {
        CacheConfiguration<String, List<Double>> cacheConfiguration = new CacheConfiguration<>();

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

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    public void testCacheEntryProcessor() throws Exception {
        final Set<String> testKeys = new HashSet<String>() {{
            add("testKey_1");
            add("testKey_2");
            add("testKey_3");
        }};

        final IgniteCache<String, List<Double>> cache = grid(0).cache(null);

        cache.invokeAll(testKeys, new DummyEntryProcessor());
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
