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

import java.util.Arrays;
import java.util.Collection;
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
    public static final String CACHE_NAME = "organizations";

    /** */
    public static final int BASE_CACHE_SIZE = 10_000;

    /** */
    public static final int ENTRIES_CNT_FOR_INCREMENT = 10_000;

    /** */
    private static final String CONSISTENT_ID = "node00-db3e5e20-91c1-4b2d-95c9-f7e5f7a0b8d3";

    /**
     * The test is parameterized by whether an incremental snapshot is taken and by consistentId
     * Restore incremental snapshot if consistentId is null is fixed in 2.17.0, see here https://issues.apache.org/jira/browse/IGNITE-23222
     */
    @Parameters(name = "incrementalSnp={0}, consistentID={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { false, CONSISTENT_ID },
            { false, null },
            { true, CONSISTENT_ID }
        });
    }

    /** */
    @Parameterized.Parameter
    public boolean incrementalSnp;

    /** */
    @Parameterized.Parameter(1)
    public String consistentId;

    /** */
    @Test
    public void testSnapshotRestore() throws Exception {
        try {
            startGrid(
                1,
                OLD_IGNITE_VERSION,
                new ConfigurationClosure(incrementalSnp, consistentId),
                new PostStartupClosure(incrementalSnp)
            );

            stopAllGrids();

            cleanPersistenceDir(true);

            IgniteEx curIgn = startGrid(getCurrentIgniteConfiguration(incrementalSnp, consistentId));

            curIgn.cluster().state(ClusterState.ACTIVE);

            if (incrementalSnp)
                curIgn.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(CACHE_NAME), 1).get();
            else
                curIgn.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(CACHE_NAME)).get();

            checkCache(curIgn.cache(CACHE_NAME), incrementalSnp ? BASE_CACHE_SIZE + ENTRIES_CNT_FOR_INCREMENT : BASE_CACHE_SIZE);
        }
        finally {
            stopAllGrids();

            cleanPersistenceDir();
        }
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
        public PostStartupClosure(boolean incrementalSnp) {
            this.incrementalSnp = incrementalSnp;
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
