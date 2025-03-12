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

package org.apache.ignite.compatibility.persistence;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 *
 */
public class SnapshotCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** */
    private static final String OLD_IGNITE_VERSION = "2.16.0";

    /** */
    private static final String SNAPSHOT_NAME = "test_snapshot";

    /** */
    public static final String CACHE_NAME = "organizations";

    /** */
    public static final int BASE_CACHE_SIZE = 10_000;

    /** */
    public static final int ENTRIES_CNT_FOR_INCREMENT = 10_000;

    /**
     *
     */
    @Test
    public void testSnapshotRestore() throws Exception {
        try {
            startGrid(1, OLD_IGNITE_VERSION, new ConfigurationClosure(null), new PostStartupClosure());

            stopAllGrids();

            cleanPersistenceDir(true);

            IgniteEx curIgn = startGrid(getCurrentIgniteConfiguration(null));

            curIgn.cluster().state(ClusterState.ACTIVE);

            curIgn.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(CACHE_NAME)).get();

            checkCache(curIgn.cache(CACHE_NAME), BASE_CACHE_SIZE);

            curIgn.cache(CACHE_NAME).destroy();

            curIgn.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(CACHE_NAME), 1).get();

            checkCache(curIgn.cache(CACHE_NAME), BASE_CACHE_SIZE + ENTRIES_CNT_FOR_INCREMENT);
        }
        finally {
            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /**
     *
     */
    private @NotNull IgniteConfiguration getCurrentIgniteConfiguration(String consistentId) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        new ConfigurationClosure(consistentId).apply(cfg);

        return cfg;
    }

    /**
     *
     */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final String consistentId;

        /**
         * @param consistentId ConsistentID.
         */
        public ConfigurationClosure(String consistentId) {
            this.consistentId = consistentId;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration igniteConfiguration) {
            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

            storageCfg.setWalCompactionEnabled(true);

            igniteConfiguration.setDataStorageConfiguration(storageCfg);

            igniteConfiguration.setConsistentId(consistentId);
        }
    }

    /**
     *
     */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, String> cache = ignite.createCache(CACHE_NAME);

            addItemsToCache(cache, 0, BASE_CACHE_SIZE);

            ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

            addItemsToCache(cache, BASE_CACHE_SIZE, ENTRIES_CNT_FOR_INCREMENT);

            ignite.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get();
        }
    }

    /** */
    private static void addItemsToCache(IgniteCache<Integer, String> cache, int startIdx, int cnt) {
        for (int i = startIdx; i < startIdx + cnt; ++i)
            cache.put(i, calcValue(i));
    }

    /** */
    private static void checkCache(IgniteCache<Integer, String> cache, int expectedSize) {
        assertEquals(expectedSize, cache.size());

        for (int i = 0; i < expectedSize; ++i)
            assertEquals(calcValue(i), cache.get(i));
    }

    /** */
    private static String calcValue(int idx) {
        return "organization-" + idx;
    }
}