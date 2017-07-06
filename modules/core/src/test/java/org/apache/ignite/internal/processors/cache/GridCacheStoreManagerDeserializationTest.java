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
import java.util.Map;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras;
import org.apache.ignite.internal.processors.cache.store.CacheLocalStore;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Checks whether storing to local store doesn't cause binary objects unmarshalling,
 * and as a consequence {@link ClassNotFoundException} to be thrown.
 *
 * @see <a href="https://issues.apache.org/jira/browse/IGNITE-2753">
 *     https://issues.apache.org/jira/browse/IGNITE-2753
 *     </a>
 */
public class GridCacheStoreManagerDeserializationTest extends GridCommonAbstractTest {
    /** IP finder. */
    protected static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache store. */
    protected static final GridCacheLocalTestStore store = new GridCacheLocalTestStore();

    /** Test cache name. */
    protected static final String CACHE_NAME = "cache_name";

    /**
     * @return Cache mode.
     */
    protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return Cache synchronization mode.
     */
    private CacheWriteSynchronizationMode cacheWriteSynchronizationMode() {
        return FULL_SYNC;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(final String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName != null && igniteInstanceName.toLowerCase().startsWith("binary"))
            c.setMarshaller(new BinaryMarshaller());
        else
            c.setMarshaller(new JdkMarshaller());

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        c.setCacheConfiguration(cacheConfiguration());

        return c;
    }

    /**
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = defaultCacheConfiguration();

        // Template
        cc.setName("*");

        cc.setRebalanceMode(SYNC);

        cc.setCacheStoreFactory(singletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);
        cc.setStoreKeepBinary(true);

        cc.setCacheMode(cacheMode());
        cc.setWriteSynchronizationMode(cacheWriteSynchronizationMode());

        cc.setBackups(0);

        cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        return cc;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        store.map.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Check whether test objects are stored correctly via stream API.
     *
     * @throws Exception If failed.
     */
    public void testStream() throws Exception {
        final Ignite grid = startGrid();

        final IgniteCache<TestObj, TestObj> cache = grid.createCache(CACHE_NAME);

        final TestObj testObj = streamData(grid);

        cache.destroy();
        cache.close();

        assert store.map.containsKey(testObj);

        final IgniteCache<TestObj, TestObj> cache2 = grid.createCache(CACHE_NAME);

        assert testObj.equals(cache2.get(testObj));
        assert store.map.containsKey(testObj);
    }

    /**
     * Simulate case where is called
     * {@link org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry#clearInternal(
     * GridCacheVersion, GridCacheObsoleteEntryExtras)}
     *
     * @throws Exception If failed.
     */
    public void testPartitionMove() throws Exception {
        final Ignite grid = startGrid("binaryGrid1");

        grid.createCache(CACHE_NAME);

        final BinaryObjectBuilder builder = grid.binary().builder("custom_type");

        final IgniteDataStreamer<BinaryObject, BinaryObject> streamer = grid.dataStreamer(CACHE_NAME);

        streamer.keepBinary(true);

        final int itemsNum = 10_000;

        for (int i = 0; i < itemsNum; i++) {
            final BinaryObject key = builder.setField("id", i).build();

            streamer.addData(key, key);
        }

        streamer.flush();
        streamer.close();

        streamer.future().get();

        assert store.map.size() == itemsNum;

        startGrid("binaryGrid2");
        startGrid("binaryGrid3");
        startGrid("binaryGrid4");

        Thread.sleep(10_000);
    }

    /**
     * TODO GG-11148.
     *
     * Check whether binary objects are stored without unmarshalling via stream API.
     *
     * @throws Exception If failed.
     */
    public void _testBinaryStream() throws Exception {
        final Ignite grid = startGrid("binaryGrid");

        final IgniteCache<BinaryObject, BinaryObject> cache = grid.createCache(CACHE_NAME).withKeepBinary();

        final BinaryObject key = streamBinaryData(grid);

        assert cache.containsKey(key);
        assert store.map.containsKey(key);

        cache.destroy();
        cache.close();

        assert store.map.containsKey(key);

        final IgniteCache<BinaryObject, BinaryObject> cache2 = grid.createCache(CACHE_NAME).withKeepBinary();

        final BinaryObject loaded = cache2.get(key);

        assertSame(loaded, key);
        assertTrue(store.map.containsKey(key));
    }

    /**
     * Create and add test data via Streamer API.
     *
     * @param grid to get streamer.
     * @return test object (it is key and val).
     */
    private TestObj streamData(final Ignite grid) {
        final IgniteDataStreamer<TestObj, TestObj> streamer = grid.dataStreamer(CACHE_NAME);

        TestObj entity = null;

        for (int i = 0; i < 1; i++) {
            entity = new TestObj(i);

            streamer.addData(entity, entity);
        }

        streamer.flush();
        streamer.close();
        streamer.future().get();

        return entity;
    }

    /**
     * Create and add binary data via Streamer API.
     *
     * @param grid to get streamer.
     * @return test object (it is key and val).
     */
    private BinaryObject streamBinaryData(final Ignite grid) {
        final IgniteDataStreamer<BinaryObject, BinaryObject> streamer = grid.dataStreamer(CACHE_NAME);

        streamer.keepBinary(true);

        final BinaryObjectBuilder builder = grid.binary().builder("custom_type");

        BinaryObject entity = null;

        for (int i = 0; i < 1; i++) {
            builder.setField("id", i);

            entity = builder.build();

            streamer.addData(entity, entity);
        }

        streamer.flush();
        streamer.close();
        streamer.future().get();

        return entity;
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

    /**
     * Test object.
     */
    static class TestObj implements Serializable {
        /** */
        Integer val;

        /** */
        public TestObj() {
        }

        /** */
        public TestObj(final Integer val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final TestObj testObj = (TestObj) o;

            return val != null ? val.equals(testObj.val) : testObj.val == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val != null ? val.hashCode() : 0;
        }
    }
}
