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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.cache.GridCacheMode.*;

/**
 * Tests from {@link GridCacheEntry#memorySize()} method.
 */
public class GridCacheEntryMemorySizeSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Null reference size (optimized marshaller writes one byte for null reference). */
    private static final int NULL_REF_SIZE = 1;

    /** Entry overhead. */
    private static final int ENTRY_OVERHEAD;

    /** Replicated entry overhead. */
    private static final int REPLICATED_ENTRY_OVERHEAD;

    /** DHT entry overhead. */
    private static final int DHT_ENTRY_OVERHEAD;

    /** Near entry overhead. */
    private static final int NEAR_ENTRY_OVERHEAD;

    /** Reader size. */
    private static final int READER_SIZE = 24;

    /** Key size in bytes. */
    private static final int KEY_SIZE;

    /** 1KB value size in bytes. */
    private static final int ONE_KB_VAL_SIZE;

    /** 2KB value size in bytes. */
    private static final int TWO_KB_VAL_SIZE;

    /**
     *
     */
    static {
        try {
            ENTRY_OVERHEAD = U.<Integer>staticField(GridCacheMapEntry.class, "SIZE_OVERHEAD");
            DHT_ENTRY_OVERHEAD = U.<Integer>staticField(GridDhtCacheEntry.class, "DHT_SIZE_OVERHEAD");
            NEAR_ENTRY_OVERHEAD = U.<Integer>staticField(GridNearCacheEntry.class, "NEAR_SIZE_OVERHEAD");
            REPLICATED_ENTRY_OVERHEAD = DHT_ENTRY_OVERHEAD;

            IgniteMarshaller marsh = new IgniteOptimizedMarshaller();

            KEY_SIZE = marsh.marshal(1).length;
            ONE_KB_VAL_SIZE = marsh.marshal(new Value(new byte[1024])).length;
            TWO_KB_VAL_SIZE = marsh.marshal(new Value(new byte[2048])).length;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** Cache mode. */
    private GridCacheMode mode;

    /** Near cache enabled flag. */
    private boolean nearEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(mode);
        cacheCfg.setDistributionMode(nearEnabled ? NEAR_PARTITIONED : PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        if (mode == PARTITIONED)
            cacheCfg.setBackups(0);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testLocal() throws Exception {
        mode = LOCAL;

        try {
            GridCache<Integer, Value> cache = startGrid().cache(null);

            assertTrue(cache.putx(1, new Value(new byte[1024])));
            assertTrue(cache.putx(2, new Value(new byte[2048])));

            assertEquals(KEY_SIZE + NULL_REF_SIZE + ENTRY_OVERHEAD + extrasSize(cache.entry(0)),
                cache.entry(0).memorySize());
            assertEquals(KEY_SIZE + ONE_KB_VAL_SIZE + ENTRY_OVERHEAD + extrasSize(cache.entry(1)),
                cache.entry(1).memorySize());
            assertEquals(KEY_SIZE + TWO_KB_VAL_SIZE + ENTRY_OVERHEAD + extrasSize(cache.entry(2)),
                cache.entry(2).memorySize());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testReplicated() throws Exception {
        mode = REPLICATED;

        try {
            GridCache<Integer, Value> cache = startGrid().cache(null);

            assertTrue(cache.putx(1, new Value(new byte[1024])));
            assertTrue(cache.putx(2, new Value(new byte[2048])));

            assertEquals(KEY_SIZE + NULL_REF_SIZE + ENTRY_OVERHEAD + REPLICATED_ENTRY_OVERHEAD +
                extrasSize(cache.entry(0)), cache.entry(0).memorySize());
            assertEquals(KEY_SIZE + ONE_KB_VAL_SIZE + ENTRY_OVERHEAD + REPLICATED_ENTRY_OVERHEAD +
                extrasSize(cache.entry(1)), cache.entry(1).memorySize());
            assertEquals(KEY_SIZE + TWO_KB_VAL_SIZE + ENTRY_OVERHEAD + REPLICATED_ENTRY_OVERHEAD +
                extrasSize(cache.entry(2)), cache.entry(2).memorySize());
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

                    if (grid(0).mapKeyToNode(null, key).equals(grid(0).localNode())) {
                        if (i > 0)
                            assertTrue(cache(0).putx(key, new Value(new byte[i * 1024])));

                        keys[i] = key;

                        break;
                    }
                }
            }

            // Create near entries.
            assertNotNull(cache(1).get(keys[1]));
            assertNotNull(cache(1).get(keys[2]));

            assertEquals(KEY_SIZE + NULL_REF_SIZE + ENTRY_OVERHEAD + DHT_ENTRY_OVERHEAD +
                extrasSize(cache(0).entry(keys[0])), cache(0).entry(keys[0]).memorySize());
            assertEquals(KEY_SIZE + ONE_KB_VAL_SIZE + ENTRY_OVERHEAD + DHT_ENTRY_OVERHEAD + READER_SIZE +
                extrasSize(cache(0).entry(keys[1])), cache(0).entry(keys[1]).memorySize());
            assertEquals(KEY_SIZE + TWO_KB_VAL_SIZE + ENTRY_OVERHEAD + DHT_ENTRY_OVERHEAD + READER_SIZE +
                extrasSize(cache(0).entry(keys[2])), cache(0).entry(keys[2]).memorySize());

            assertEquals(KEY_SIZE + NULL_REF_SIZE + ENTRY_OVERHEAD + NEAR_ENTRY_OVERHEAD +
                extrasSize(cache(1).entry(keys[0])), cache(1).entry(keys[0]).memorySize());
            assertEquals(KEY_SIZE + ONE_KB_VAL_SIZE + ENTRY_OVERHEAD + NEAR_ENTRY_OVERHEAD +
                extrasSize(cache(1).entry(keys[1])), cache(1).entry(keys[1]).memorySize());
            assertEquals(KEY_SIZE + TWO_KB_VAL_SIZE + ENTRY_OVERHEAD + NEAR_ENTRY_OVERHEAD +
                extrasSize(cache(1).entry(keys[2])), cache(1).entry(keys[2]).memorySize());
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

                    if (grid(0).mapKeyToNode(null, key).equals(grid(0).localNode())) {
                        if (i > 0)
                            assertTrue(cache(0).putx(key, new Value(new byte[i * 1024])));

                        keys[i] = key;

                        break;
                    }
                }
            }

            // Create near entries.
            assertNotNull(cache(1).get(keys[1]));
            assertNotNull(cache(1).get(keys[2]));

            assertEquals(KEY_SIZE + NULL_REF_SIZE + ENTRY_OVERHEAD + DHT_ENTRY_OVERHEAD +
                extrasSize(cache(0).entry(keys[0])), cache(0).entry(keys[0]).memorySize());
            assertEquals(KEY_SIZE + ONE_KB_VAL_SIZE + ENTRY_OVERHEAD + DHT_ENTRY_OVERHEAD +
                extrasSize(cache(0).entry(keys[1])), cache(0).entry(keys[1]).memorySize());
            assertEquals(KEY_SIZE + TWO_KB_VAL_SIZE + ENTRY_OVERHEAD + DHT_ENTRY_OVERHEAD +
                extrasSize(cache(0).entry(keys[2])), cache(0).entry(keys[2]).memorySize());

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
    private int extrasSize(GridCacheEntry entry) throws Exception {
        Method mthd = GridCacheMapEntry.class.getDeclaredMethod("extrasSize");

        mthd.setAccessible(true);

        GridCacheContext ctx = U.field(entry, "ctx");

        GridCacheEntryEx entry0 = ((GridCacheEntryImpl) entry).entryEx(false, ctx.discovery().topologyVersion());

        return (Integer)mthd.invoke(entry0);
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
