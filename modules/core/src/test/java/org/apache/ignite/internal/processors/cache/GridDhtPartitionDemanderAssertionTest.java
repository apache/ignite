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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.store.CacheLocalStore;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentHashMap8;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.io.Serializable;
import java.util.Map;

import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * Throws assertion error on partition move.
 */
public class GridDhtPartitionDemanderAssertionTest  extends GridCommonAbstractTest {
    /** IP finder. */
    protected static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache store. */
    protected static final GridCacheLocalTestStore store = new GridCacheLocalTestStore();

    /** Test cache name. */
    protected static final String CACHE_NAME = "cache_name";

    /** Cache mode. */
    protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** Cache write order mode. */
    protected CacheAtomicWriteOrderMode cacheAtomicWriteOrderMode() {
        return CacheAtomicWriteOrderMode.PRIMARY;
    }

    /** */
    private CacheWriteSynchronizationMode cacheWriteSynchronizationMode() {
        return CacheWriteSynchronizationMode.PRIMARY_SYNC;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        if (gridName != null && gridName.toLowerCase().startsWith("binary"))
            c.setMarshaller(new BinaryMarshaller());
        else
            c.setMarshaller(new OptimizedMarshaller());

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setSwapEnabled(false);
        cc.setRebalanceMode(SYNC);

        cc.setCacheStoreFactory(singletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);
        cc.setStoreKeepBinary(true);

        cc.setCacheMode(cacheMode());
        cc.setAtomicWriteOrderMode(cacheAtomicWriteOrderMode());
        cc.setWriteSynchronizationMode(cacheWriteSynchronizationMode());

        cc.setBackups(0);

        cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Throws assertion error on partition move.
     *
     * @throws Exception
     */
    public void testPartitionMove() throws Exception {
        final Ignite grid = startGrid("binaryGrid1");

        final IgniteCache<TestObj, TestObj> cache = grid.createCache(CACHE_NAME).withKeepBinary();

        cache.put(new TestObj(-1), new TestObj(-1));

        final BinaryObjectBuilder builder = grid.binary().builder(TestObj.class.getName());

        final IgniteDataStreamer<BinaryObject, BinaryObject> streamer = grid.dataStreamer(CACHE_NAME);

        streamer.keepBinary(true);

        for (int i = 0; i < 10_000; i++) {
            final BinaryObject key = builder.setField("id", i).build();

            streamer.addData(key, key);
        }

        streamer.close();

        startGrid("binaryGrid2");
        startGrid("binaryGrid3");
        startGrid("binaryGrid4");

        Thread.sleep(20_000);
    }

    /** */
    private static class TestObj implements Serializable {
        /** */
        private int id;

        public TestObj(final int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final TestObj testObj = (TestObj) o;

            return id == testObj.id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }


    /**
     * Local store mock.
     *
     * @param <K>
     * @param <V>
     */
    @CacheLocalStore
    protected static class GridCacheLocalTestStore<K, V> extends CacheStoreAdapter<K, V> {
        /** */
        public final Map<K, V> map = new ConcurrentHashMap8<>();

        /** {@inheritDoc} */
        @Override public V load(final K key) throws CacheLoaderException {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(final Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
            map.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(final Object key) throws CacheWriterException {
            map.remove(key);
        }
    }


}
