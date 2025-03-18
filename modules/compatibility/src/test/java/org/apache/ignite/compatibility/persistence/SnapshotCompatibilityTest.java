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

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpConsumer;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.dump.DumpReader;
import org.apache.ignite.dump.DumpReaderConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 *
 */
@RunWith(Parameterized.class)
public class SnapshotCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** */
    private static final String OLD_IGNITE_VERSION = "2.16.0";

    /** */
    private static final String SNAPSHOT_NAME = "test_snapshot";

    /** */
    private static final String CACHE_DUMP_NAME = "test_cache_dump";

    /** */
    public static final String CACHE_NAME = "organizations";

    /** */
    public static final int BASE_CACHE_SIZE = 10_000;

    /** */
    public static final int ENTRIES_CNT_FOR_INCREMENT = 10_000;

    /** */
    private static final String CONSISTENT_ID = "db3e5e20-91c1-4b2d-95c9-f7e5f7a0b8d3";

    /**
     * The test is parameterized by whether an incremental snapshot is taken and by consistentId
     * Restore incremental snapshot if consistentId is null is fixed in 2.17.0, see here https://issues.apache.org/jira/browse/IGNITE-23222
     */
    @Parameters(name = "incrementalSnp={0}, consistentID={1}, oldNodesCnt={2}, createDump={3}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { false, CONSISTENT_ID, 1, false },
            { false, CONSISTENT_ID, 1, true },
            { false, CONSISTENT_ID, 3, false },
            { false, CONSISTENT_ID, 3, true },
            { false, null, 1, false },
            { false, null, 1, true },
            { false, null, 3, false },
            { false, null, 3, true },
            { true, CONSISTENT_ID, 1, false },
            { true, CONSISTENT_ID, 3, false },
        });
    }

    /** */
    @Parameterized.Parameter
    public boolean incrementalSnp;

    /** */
    @Parameterized.Parameter(1)
    public String consistentId;

    /** */
    @Parameterized.Parameter(2)
    public int oldNodesCnt;

    /** */
    @Parameterized.Parameter(3)
    public boolean cacheDump;

    /** */
    @Test
    public void testSnapshotRestore() throws Exception {
        try {
            startGrid(
                oldNodesCnt,
                OLD_IGNITE_VERSION,
                new ConfigurationClosure(incrementalSnp, consistentId),
                new PostStartupClosure(incrementalSnp, cacheDump)
            );

            stopAllGrids();

            cleanPersistenceDir(true);

            IgniteEx curIgn = startGrid(getCurrentIgniteConfiguration(incrementalSnp, consistentId));

            curIgn.cluster().state(ClusterState.ACTIVE);

            if (cacheDump)
                checkCacheDump(curIgn);

            if (incrementalSnp)
                checkIncrementalSnapshot(curIgn);
            else
                checkSnapshot(curIgn);

        }
        finally {
            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    private static void checkSnapshot(IgniteEx curIgn) {
        curIgn.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(CACHE_NAME)).get();

        checkCache(curIgn.cache(CACHE_NAME), BASE_CACHE_SIZE);
    }

    /** */
    private static void checkIncrementalSnapshot(IgniteEx curIgn) {
        curIgn.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(CACHE_NAME), 1).get();

        checkCache(curIgn.cache(CACHE_NAME), BASE_CACHE_SIZE + ENTRIES_CNT_FOR_INCREMENT);
    }

    /** */
    private void checkCacheDump(IgniteEx curIgn) {
        Set<Integer> storedKeys = new HashSet<>();

        DumpConsumer consumer = new DumpConsumer() {
            @Override public void start() {

            }

            @Override public void onMappings(Iterator<TypeMapping> mappings) {

            }

            @Override public void onTypes(Iterator<BinaryType> types) {

            }

            @Override public void onCacheConfigs(Iterator<StoredCacheData> caches) {

            }

            @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
                while (data.hasNext()) {
                    DumpEntry de = data.next();

                    assertNotNull(de);

                    Integer key = (Integer)de.key();
                    String val = (String)de.value();

                    assertEquals(calcValue(key), val);

                    storedKeys.add(key);
                }
            }

            @Override public void stop() {
            }
        };

        new DumpReader(
            new DumpReaderConfiguration(
                new File(sharedFileTree(curIgn.configuration()).snapshotsRoot(), CACHE_DUMP_NAME),
                consumer
            ),
            log
        ).run();

        assertEquals(BASE_CACHE_SIZE, storedKeys.size());
        for (int i = 0; i < BASE_CACHE_SIZE; i++)
            assertTrue(storedKeys.contains(i));
    }

    /** */
    private @NotNull IgniteConfiguration getCurrentIgniteConfiguration(boolean incrementalSnp, String consistentId) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        // We configure current Ignite version in the same way as the old one.
        new ConfigurationClosure(incrementalSnp, consistentId).apply(cfg);

        return cfg;
    }

    /**
     * Configuration closure for old Ignite version.
     */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final String consistentId;

        /** */
        private final boolean incrementalSnp;

        /** */
        public ConfigurationClosure(boolean incrementalSnp, String consistentId) {
            this.consistentId = consistentId;
            this.incrementalSnp = incrementalSnp;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration igniteConfiguration) {
            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

            igniteConfiguration.setDataStorageConfiguration(storageCfg);

            igniteConfiguration.setConsistentId(consistentId);

            if (incrementalSnp)
                storageCfg.setWalCompactionEnabled(true);
        }
    }

    /**
     * Post startup closure for old Ignite version.
     */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** */
        private final boolean incrementalSnp;

        /** */
        private final boolean cacheDump;

        /** */
        public PostStartupClosure(boolean incrementalSnp, boolean cacheDump) {
            this.incrementalSnp = incrementalSnp;
            this.cacheDump = cacheDump;
        }

        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, String> cache = ignite.createCache(CACHE_NAME);

            addItemsToCache(cache, 0, BASE_CACHE_SIZE);

            ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

            if (incrementalSnp) {
                addItemsToCache(cache, BASE_CACHE_SIZE, ENTRIES_CNT_FOR_INCREMENT);

                ignite.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get();
            }

            if (cacheDump)
                ignite.snapshot().createDump(CACHE_DUMP_NAME, Collections.singleton(CACHE_NAME)).get();
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
