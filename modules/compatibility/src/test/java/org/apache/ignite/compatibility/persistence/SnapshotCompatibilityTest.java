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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpConsumer;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.dump.DumpReader;
import org.apache.ignite.dump.DumpReaderConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
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
    private static final int BASE_CACHE_SIZE = 10_000;

    /** */
    private static final int ENTRIES_CNT_FOR_INCREMENT = 10_000;

    /** */
    private static final String CUSTOM_SNP_RELATIVE_PATH = "ex_snapshots";

    /** */
    private static final String CONSISTENT_ID = UUID.randomUUID().toString();

    /** */
    @Parameterized.Parameter
    public boolean incrementalSnp;

    /** */
    @Parameterized.Parameter(1)
    @Nullable public String consistentId;

    /** */
    @Parameterized.Parameter(2)
    public int oldNodesCnt;

    /** */
    @Parameterized.Parameter(3)
    public boolean cacheDump;

    /** */
    @Parameterized.Parameter(4)
    public boolean customSnpPath;

    /** */
    @Parameterized.Parameter(5)
    public boolean testCacheGrp;

    /** */
    private CacheGroupInfo cacheGrpInfo;

    /**
     * The test is parameterized by whether an incremental snapshot is taken and by consistentId.
     * Restore incremental snapshot if consistentId is null is fixed in 2.17.0, see here https://issues.apache.org/jira/browse/IGNITE-23222.
     * Also restoring cache dump and any kind of snapshot is pointless.
     */
    @Parameters(name = "incrementalSnp={0}, consistentID={1}, oldNodesCnt={2}, cacheDump={3}, customSnpPath={4}, testCacheGrp={5}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<>();

        for (Boolean incrementalSnp : Arrays.asList(true, false))
            for (String consistentId : Arrays.asList(CONSISTENT_ID, null))
                for (Integer oldNodesCnt : Arrays.asList(1, 3))
                    for (Boolean cacheDump : Arrays.asList(true, false))
                        for (Boolean customSnpPath : Arrays.asList(true, false))
                            for (Boolean testCacheGrp : Arrays.asList(true, false))
                                if ((!incrementalSnp || !cacheDump) && (!incrementalSnp || consistentId != null))
                                    data.add(
                                        new Object[]{incrementalSnp, consistentId, oldNodesCnt, cacheDump, customSnpPath, testCacheGrp}
                                    );

        return data;
    }

    /** */
    @Before
    public void setUp() {
        cacheGrpInfo = new CacheGroupInfo("test-cache", testCacheGrp ? 2 : 1);
    }

    /** */
    @Test
    public void testSnapshotRestore() throws Exception {
        try {
            startGrid(
                oldNodesCnt,
                OLD_IGNITE_VERSION,
                new ConfigurationClosure(incrementalSnp, consistentId, customSnpPath, true, cacheGrpInfo),
                new PostStartupClosure(incrementalSnp, cacheDump, cacheGrpInfo)
            );

            stopAllGrids();

            cleanPersistenceDir(true);

            IgniteEx curIgn = startGrid(currentIgniteConfiguration(incrementalSnp, consistentId, customSnpPath));

            curIgn.cluster().state(ClusterState.ACTIVE);

            if (cacheDump)
                checkCacheDump(curIgn);
            else if (incrementalSnp)
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
    private void checkSnapshot(IgniteEx curIgn) {
        curIgn.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(cacheGrpInfo.name())).get();

        cacheGrpInfo.checkCaches(curIgn, BASE_CACHE_SIZE);
    }

    /** */
    private void checkIncrementalSnapshot(IgniteEx curIgn) {
        curIgn.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(cacheGrpInfo.name()), 1).get();

        cacheGrpInfo.checkCaches(curIgn, BASE_CACHE_SIZE + ENTRIES_CNT_FOR_INCREMENT);
    }

    /** */
    private void checkCacheDump(IgniteEx curIgn) throws IgniteCheckedException {
        Map<String, Integer> foundCacheSizes = new ConcurrentHashMap<>();

        Set<String> foundCacheNames = ConcurrentHashMap.newKeySet();

        DumpConsumer consumer = new DumpConsumer() {
            @Override public void start() {
                // No-op.
            }

            @Override public void onMappings(Iterator<TypeMapping> mappings) {
                // No-op.
            }

            @Override public void onTypes(Iterator<BinaryType> types) {
                // No-op.
            }

            @Override public void onCacheConfigs(Iterator<StoredCacheData> caches) {
                assertNotNull(cacheGrpInfo);

                caches.forEachRemaining(cache -> {
                    CacheConfiguration<?, ?> ccfg = cache.config();

                    assertNotNull(ccfg);

                    assertEquals(cacheGrpInfo.name(), ccfg.getGroupName());

                    foundCacheNames.add(ccfg.getName());
                });
            }

            @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
                assertNotNull(cacheGrpInfo);

                data.forEachRemaining(de -> {
                    assertNotNull(de);

                    Integer key = (Integer)de.key();
                    String val = (String)de.value();

                    for (String cacheName : cacheGrpInfo.cacheNamesList()) {
                        if (val.startsWith(cacheName)) {
                            assertEquals(calcValue(cacheName, key), val);

                            foundCacheSizes.put(cacheName, foundCacheSizes.getOrDefault(cacheName, 0) + 1);

                            break;
                        }
                    }
                });
            }

            @Override public void stop() {
                // No-op.
            }
        };

        new DumpReader(new DumpReaderConfiguration(
            CACHE_DUMP_NAME,
            customSnpPath ? customSnapshotPath(CUSTOM_SNP_RELATIVE_PATH, false) : null,
            curIgn.configuration(),
            consumer
        ), log).run();

        cacheGrpInfo.cacheNamesList().forEach(
            cacheName -> assertEquals(BASE_CACHE_SIZE, (int)foundCacheSizes.get(cacheName))
        );

        assertTrue(cacheGrpInfo.cacheNamesList().containsAll(foundCacheNames));
        assertEquals(cacheGrpInfo.cacheNamesList().size(), foundCacheNames.size());
    }

    /** */
    private @NotNull IgniteConfiguration currentIgniteConfiguration(
        boolean incrementalSnp,
        String consistentId,
        boolean customSnpPath
    ) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        // We configure current Ignite version in the same way as the old one.
        new ConfigurationClosure(incrementalSnp, consistentId, customSnpPath, false, cacheGrpInfo).apply(cfg);

        return cfg;
    }

    /** */
    private static String customSnapshotPath(String relativePath, boolean forSnapshotTake) throws IgniteCheckedException {
        return U.resolveWorkDirectory(U.defaultWorkDirectory(), relativePath, forSnapshotTake).getAbsolutePath();
    }

    /** */
    private static String calcValue(String cacheName, int key) {
        return cacheName + "-organization-" + key;
    }

    /**
     * Configuration closure both for old and current Ignite version.
     */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final String consistentId;

        /** */
        private final boolean incrementalSnp;

        /** */
        private final boolean customSnpPath;

        /** */
        private final boolean forSnapshotTake;

        /** */
        private final CacheGroupInfo cacheGrpInfo;

        /** */
        public ConfigurationClosure(
            boolean incrementalSnp,
            String consistentId,
            boolean customSnpPath,
            boolean forSnapshotTake,
            CacheGroupInfo cacheGrpInfo
        ) {
            this.consistentId = consistentId;
            this.incrementalSnp = incrementalSnp;
            this.customSnpPath = customSnpPath;
            this.forSnapshotTake = forSnapshotTake;
            this.cacheGrpInfo = cacheGrpInfo;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration igniteConfiguration) {
            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

            igniteConfiguration.setDataStorageConfiguration(storageCfg);

            igniteConfiguration.setConsistentId(consistentId);

            if (incrementalSnp)
                storageCfg.setWalCompactionEnabled(true);

            if (forSnapshotTake)
                igniteConfiguration.setCacheConfiguration(
                    cacheGrpInfo.cacheNamesList().stream()
                        .map(cacheName -> new CacheConfiguration<Integer, String>(cacheName)
                            .setGroupName(cacheGrpInfo.name())
                            .setAffinity(new RendezvousAffinityFunction(false, 10))
                        )
                        .toArray(CacheConfiguration[]::new)
                );

            if (customSnpPath) {
                try {
                    igniteConfiguration.setSnapshotPath(customSnapshotPath(CUSTOM_SNP_RELATIVE_PATH, forSnapshotTake));
                }
                catch (IgniteCheckedException e) {
                    throw new RuntimeException(e);
                }
            }
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
        private final CacheGroupInfo cacheGrpInfo;

        /** */
        public PostStartupClosure(boolean incrementalSnp, boolean cacheDump, CacheGroupInfo cacheGrpInfo) {
            this.incrementalSnp = incrementalSnp;
            this.cacheDump = cacheDump;
            this.cacheGrpInfo = cacheGrpInfo;
        }

        /** {@inheritDoc} */
        @Override public void apply(Ignite ign) {
            ign.cluster().state(ClusterState.ACTIVE);

            cacheGrpInfo.addItemsToCacheGrp(ign, 0, BASE_CACHE_SIZE);

            if (cacheDump)
                ign.snapshot().createDump(CACHE_DUMP_NAME, Collections.singleton(cacheGrpInfo.name())).get();
            else
                ign.snapshot().createSnapshot(SNAPSHOT_NAME).get();

            if (incrementalSnp) {
                cacheGrpInfo.addItemsToCacheGrp(ign, BASE_CACHE_SIZE, ENTRIES_CNT_FOR_INCREMENT);

                ign.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get();
            }
        }
    }

    /** */
    private static class CacheGroupInfo {
        /** */
        private final String name;

        /** */
        private final List<String> cacheNames;

        /** */
        public CacheGroupInfo(String name, int cachesCnt) {
            this.name = name;

            List<String> cacheNames = new ArrayList<>();

            for (int i = 0; i < cachesCnt; ++i)
                cacheNames.add("test-cache-" + i);

            this.cacheNames = Collections.unmodifiableList(cacheNames);
        }

        /** */
        public String name() {
            return name;
        }

        /** */
        public List<String> cacheNamesList() {
            return cacheNames;
        }

        /** */
        public void addItemsToCacheGrp(Ignite ign, int startIdx, int cnt) {
            for (String cacheName : cacheNames)
                addItemsToCache(ign.cache(cacheName), startIdx, cnt);
        }

        /** */
        private void addItemsToCache(IgniteCache<Integer, String> cache, int startIdx, int cnt) {
            for (int i = startIdx; i < startIdx + cnt; ++i)
                cache.put(i, calcValue(cache.getName(), i));
        }

        /** */
        public void checkCaches(Ignite ign, int expectedCacheSize) {
            for (String cacheName : cacheNames) {
                IgniteCache<Integer, String> cache = ign.cache(cacheName);

                assertNotNull(cache);

                checkCache(cache, expectedCacheSize);
            }
        }

        /** */
        private void checkCache(IgniteCache<Integer, String> cache, int expectedSize) {
            assertEquals(expectedSize, cache.size());

            for (int i = 0; i < expectedSize; ++i)
                assertEquals(calcValue(cache.getName(), i), cache.get(i));
        }
    }
}
