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
import java.lang.reflect.Method;
import java.util.Arrays;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class GridCacheEntryMemorySizeSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Null reference size (optimized marshaller writes one byte for null reference). */
    private static final int NULL_REF_SIZE = 1;

    /** Entry overhead. */
    private static int ENTRY_OVERHEAD;

    /** Replicated entry overhead. */
    private static int REPLICATED_ENTRY_OVERHEAD;

    /** DHT entry overhead. */
    private static int DHT_ENTRY_OVERHEAD;

    /** Near entry overhead. */
    private static int NEAR_ENTRY_OVERHEAD;

    /** Reader size. */
    private static int READER_SIZE = 24;

    /** Key size in bytes. */
    private static int KEY_SIZE;

    /** 1KB value size in bytes. */
    private static int ONE_KB_VAL_SIZE;

    /** 2KB value size in bytes. */
    private static int TWO_KB_VAL_SIZE;

    /** Cache mode. */
    private CacheMode mode;

    /** Near cache enabled flag. */
    private boolean nearEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(mode);
        cacheCfg.setNearConfiguration(nearEnabled ? new NearCacheConfiguration() : null);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        if (mode == PARTITIONED)
            cacheCfg.setBackups(0);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        try {
            ENTRY_OVERHEAD = U.<Integer>staticField(GridCacheMapEntry.class, "SIZE_OVERHEAD");
            DHT_ENTRY_OVERHEAD = U.<Integer>staticField(GridDhtCacheEntry.class, "DHT_SIZE_OVERHEAD");
            NEAR_ENTRY_OVERHEAD = U.<Integer>staticField(GridNearCacheEntry.class, "NEAR_SIZE_OVERHEAD");
            REPLICATED_ENTRY_OVERHEAD = DHT_ENTRY_OVERHEAD;

            Marshaller marsh = createMarshaller();

            KEY_SIZE = marsh.marshal(1).length;
            ONE_KB_VAL_SIZE = marsh.marshal(new Value(new byte[1024])).length;
            TWO_KB_VAL_SIZE = marsh.marshal(new Value(new byte[2048])).length;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Creates an instance of Marshaller that is used by caches during the test run.
     *
     * @return
     */
    protected Marshaller createMarshaller() throws IgniteCheckedException {
        Marshaller marsh = new OptimizedMarshaller();

        marsh.setContext(new MarshallerContext() {
            @Override public boolean registerClass(int id, Class cls) {
                return true;
            }

            @Override public Class getClass(int id, ClassLoader ldr) {
                throw new UnsupportedOperationException();
            }

            @Override public boolean isSystemType(String typeName) {
                return false;
            }
        });

        return marsh;
    }

    /** @throws Exception If failed. */
    public void testLocal() throws Exception {
        mode = LOCAL;

        try {
            IgniteCache<Integer, Value> cache = startGrid().cache(null);

            cache.put(1, new Value(new byte[1024]));
            cache.put(2, new Value(new byte[2048]));

            GridCacheAdapter<Integer, Value> internalCache = internalCache(cache);

            assertEquals(KEY_SIZE + NULL_REF_SIZE + ENTRY_OVERHEAD + extrasSize(internalCache.entryEx(0)),
                internalCache.entryEx(0).memorySize());
            assertEquals(KEY_SIZE + ONE_KB_VAL_SIZE + ENTRY_OVERHEAD + extrasSize(internalCache.entryEx(1)),
                internalCache.entryEx(1).memorySize());
            assertEquals(KEY_SIZE + TWO_KB_VAL_SIZE + ENTRY_OVERHEAD + extrasSize(internalCache.entryEx(2)),
                internalCache.entryEx(2).memorySize());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testReplicated() throws Exception {
        mode = REPLICATED;

        try {
            IgniteCache<Integer, Value> cache = startGrid().cache(null);

            cache.put(1, new Value(new byte[1024]));
            cache.put(2, new Value(new byte[2048]));

            GridCacheAdapter<Integer, Value> internalCache = dht(cache);

            assertEquals(KEY_SIZE + NULL_REF_SIZE + ENTRY_OVERHEAD + REPLICATED_ENTRY_OVERHEAD +
                extrasSize(internalCache.entryEx(0)), internalCache.entryEx(0).memorySize());
            assertEquals(KEY_SIZE + ONE_KB_VAL_SIZE + ENTRY_OVERHEAD + REPLICATED_ENTRY_OVERHEAD +
                extrasSize(internalCache.entryEx(1)), internalCache.entryEx(1).memorySize());
            assertEquals(KEY_SIZE + TWO_KB_VAL_SIZE + ENTRY_OVERHEAD + REPLICATED_ENTRY_OVERHEAD +
                extrasSize(internalCache.entryEx(2)), internalCache.entryEx(2).memorySize());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testPartitionedNearEnabled() throws Exception {
        mode = PARTITIONED;
        nearEnabled = true;

        try {
            startGridsMultiThreaded(2);

            int[] keys = new int[3];

            int key = 0;

            for (int i = 0; i < keys.length; i++) {
                while (true) {
                    key++;

                    if (grid(0).cluster().mapKeyToNode(null, key).equals(grid(0).localNode())) {
                        if (i > 0)
                            jcache(0).put(key, new Value(new byte[i * 1024]));

                        keys[i] = key;

                        break;
                    }
                }
            }

            // Create near entries.
            assertNotNull(jcache(1).get(keys[1]));
            assertNotNull(jcache(1).get(keys[2]));

            GridCacheAdapter<Object, Object> cache0 = dht(jcache(0));

            assertEquals(KEY_SIZE + NULL_REF_SIZE + ENTRY_OVERHEAD + DHT_ENTRY_OVERHEAD +
                extrasSize(cache0.entryEx(keys[0])), cache0.entryEx(keys[0]).memorySize());
            assertEquals(KEY_SIZE + ONE_KB_VAL_SIZE + ENTRY_OVERHEAD + DHT_ENTRY_OVERHEAD + READER_SIZE +
                extrasSize(cache0.entryEx(keys[1])), cache0.entryEx(keys[1]).memorySize());
            assertEquals(KEY_SIZE + TWO_KB_VAL_SIZE + ENTRY_OVERHEAD + DHT_ENTRY_OVERHEAD + READER_SIZE +
                extrasSize(cache0.entryEx(keys[2])), cache0.entryEx(keys[2]).memorySize());

            GridCacheAdapter<Object, Object> cache1 = near(jcache(1));

            assertEquals(KEY_SIZE + NULL_REF_SIZE + ENTRY_OVERHEAD + NEAR_ENTRY_OVERHEAD +
                extrasSize(cache1.entryEx(keys[0])), cache1.entryEx(keys[0]).memorySize());
            assertEquals(KEY_SIZE + ONE_KB_VAL_SIZE + ENTRY_OVERHEAD + NEAR_ENTRY_OVERHEAD +
                extrasSize(cache1.entryEx(keys[1])), cache1.entryEx(keys[1]).memorySize());
            assertEquals(KEY_SIZE + TWO_KB_VAL_SIZE + ENTRY_OVERHEAD + NEAR_ENTRY_OVERHEAD +
                extrasSize(cache1.entryEx(keys[2])), cache1.entryEx(keys[2]).memorySize());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testPartitionedNearDisabled() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;

        try {
            startGridsMultiThreaded(2);

            int[] keys = new int[3];

            int key = 0;

            for (int i = 0; i < keys.length; i++) {
                while (true) {
                    key++;

                    if (grid(0).cluster().mapKeyToNode(null, key).equals(grid(0).localNode())) {
                        if (i > 0)
                            jcache(0).put(key, new Value(new byte[i * 1024]));

                        keys[i] = key;

                        break;
                    }
                }
            }

            // Create near entries.
            assertNotNull(jcache(1).get(keys[1]));
            assertNotNull(jcache(1).get(keys[2]));

            GridCacheAdapter<Object, Object> cache = dht(jcache(0));

            assertEquals(KEY_SIZE + NULL_REF_SIZE + ENTRY_OVERHEAD + DHT_ENTRY_OVERHEAD +
                extrasSize(cache.entryEx(keys[0])), cache.entryEx(keys[0]).memorySize());
            assertEquals(KEY_SIZE + ONE_KB_VAL_SIZE + ENTRY_OVERHEAD + DHT_ENTRY_OVERHEAD +
                extrasSize(cache.entryEx(keys[1])), cache.entryEx(keys[1]).memorySize());
            assertEquals(KEY_SIZE + TWO_KB_VAL_SIZE + ENTRY_OVERHEAD + DHT_ENTRY_OVERHEAD +
                extrasSize(cache.entryEx(keys[2])), cache.entryEx(keys[2]).memorySize());

            // Do not test other node since there are no backups.
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Get entry extras size.
     *
     * @param entry Entry.
     * @return Extras size.
     * @throws Exception If failed.
     */
    private int extrasSize(GridCacheEntryEx entry) throws Exception {
        Method mthd = GridCacheMapEntry.class.getDeclaredMethod("extrasSize");

        mthd.setAccessible(true);

        return (Integer)mthd.invoke(entry);
    }

    /** Value. */
    @SuppressWarnings("UnusedDeclaration")
    private static class Value implements Serializable {
        /** Byte array. */
        private byte[] arr;

        /** @param arr Byte array. */
        private Value(byte[] arr) {
            this.arr = arr;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Value val = (Value)o;

            return Arrays.equals(arr, val.arr);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return arr != null ? Arrays.hashCode(arr) : 0;
        }
    }
}