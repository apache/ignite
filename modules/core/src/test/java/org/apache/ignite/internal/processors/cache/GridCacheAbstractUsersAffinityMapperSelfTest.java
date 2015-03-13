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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;

import static org.apache.ignite.cache.CacheRebalanceMode.*;

/**
 * Test affinity mapper.
 */
public abstract class GridCacheAbstractUsersAffinityMapperSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int KEY_CNT = 1000;

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    public static final CacheAffinityKeyMapper AFFINITY_MAPPER = new UsersAffinityKeyMapper();

    /** */
    public GridCacheAbstractUsersAffinityMapperSelfTest() {
        super(false /* doesn't start grid */);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName(null);
        cacheCfg.setCacheMode(getCacheMode());
        cacheCfg.setAtomicityMode(getAtomicMode());
        cacheCfg.setDistributionMode(getDistributionMode());
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setAffinityMapper(AFFINITY_MAPPER);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /**
     * @return Distribution mode.
     */
    protected abstract CacheDistributionMode getDistributionMode();

    /**
     * @return Cache atomicity mode.
     */
    protected abstract CacheAtomicityMode getAtomicMode();

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode getCacheMode();

    /**
     * @throws Exception If failed.
     */
    public void testAffinityMapper() throws Exception {
        IgniteCache<Object, Object> cache = startGrid(0).jcache(null);

        for (int i = 0; i < KEY_CNT; i++) {
            cache.put(String.valueOf(i), String.valueOf(i));

            cache.put(new TestAffinityKey(i, String.valueOf(i)), i);
        }

        assertEquals(1, cache.get(new TestAffinityKey(1, "1")));

        startGrid(1);

        for (int i = 0; i < KEY_CNT; i++)
            grid(i % 2).compute().affinityRun(null, new TestAffinityKey(1, "1"), new NoopClosure());
    }

    /**
     * Test key for field annotation.
     */
    private static class TestAffinityKey implements Externalizable {
        /** Key. */
        private int key;

        /** Affinity key. */
        @CacheAffinityKeyMapped
        private String affKey;

        /**
         * Constructor.
         */
        public TestAffinityKey() {
        }

        /**
         * Constructor.
         *
         * @param key Key.
         * @param affKey Affinity key.
         */
        TestAffinityKey(int key, String affKey) {
            this.key = key;
            this.affKey = affKey;
        }

        /**
         * @return Key.
         */
        public int key() {
            return key;
        }

        /**
         * @return Affinity key.
         */
        public String affinityKey() {
            return affKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return o instanceof TestAffinityKey && key == ((TestAffinityKey)o).key();
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key + affKey.hashCode();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(key);
            out.writeUTF(affKey);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            key = in.readInt();
            affKey = in.readUTF();
        }
    }

    /**
     * Users affinity mapper.
     */
    private static class UsersAffinityKeyMapper extends GridCacheDefaultAffinityKeyMapper{
        /** {@inheritDoc} */
        @Override public Object affinityKey(Object key) {
            GridArgumentCheck.notNull(key, "key");

            assertFalse("GridCacheInternal entry mustn't be passed in user's key mapper.",
                key instanceof GridCacheInternal);

            return super.affinityKey(key);
        }
    }

    /**
     * Noop closure.
     */
    private static class NoopClosure implements IgniteRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            // No-op.
        }
    }
}
