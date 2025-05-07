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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
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

import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 *
 */
@RunWith(Parameterized.class)
public class SnapshotCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** */
    private static final String OLD_IGNITE_VERSION = Arrays.stream(IgniteReleasedVersion.values())
        .max(Comparator.comparing(IgniteReleasedVersion::version))
        .map(IgniteReleasedVersion::toString)
        .orElseThrow(() -> new IllegalStateException("Enum is empty"));

    /** */
    private static final String SNAPSHOT_NAME = "test_snapshot";

    /** */
    private static final String CACHE_DUMP_NAME = "test_cache_dump";

    /** */
    private static final int BASE_CACHE_SIZE = 100;

    /** */
    private static final int ENTRIES_CNT_FOR_INCREMENT = 100;

    /** */
    private static final String CUSTOM_SNP_RELATIVE_PATH = "ex_snapshots";

    /** */
    @Parameterized.Parameter
    public boolean incSnp;

    /** */
    @Parameterized.Parameter(1)
    public boolean customConsId;

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

    /** */
    @Parameterized.Parameters(name = "incSnp={0}, customConsId={1}, oldNodesCnt={2}, cacheDump={3}, customSnpPath={4}, testCacheGrp={5}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<>();

        for (boolean incSnp : Arrays.asList(true, false))
            for (boolean customConsId: Arrays.asList(true, false))
                for (int oldNodesCnt : Arrays.asList(1, 3))
                    for (boolean cacheDump : Arrays.asList(true, false))
                        for (boolean customSnpPath : Arrays.asList(true, false))
                            for (boolean testCacheGrp : Arrays.asList(true, false))
                                data.add(new Object[]{incSnp, customConsId, oldNodesCnt, cacheDump, customSnpPath, testCacheGrp});

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
        if (incSnp) {
            assumeFalse("Incremental snapshots for cache dump not supported", cacheDump);

            assumeTrue("Incremental snapshots require same consistentID", customConsId);

            assumeTrue("https://issues.apache.org/jira/browse/IGNITE-25096", oldNodesCnt == 1);
        }

        try {
            for (int i = 1; i <= oldNodesCnt; ++i) {
                if (i == oldNodesCnt) {
                    startGrid(
                            oldNodesCnt,
                            OLD_IGNITE_VERSION,
                        oldConfigurationClosure(oldNodesCnt),
                        new CreateSnapshotClosure(incSnp, cacheDump, cacheGrpInfo)
                    );
                }
                else {
                    startGrid(
                        i,
                        OLD_IGNITE_VERSION,
                        oldConfigurationClosure(i)
                    );
                }
            }

            stopAllGrids();

            cleanPersistenceDir(true);

            IgniteEx node = startGrid(currentIgniteConfiguration(incSnp, consId(1), customSnpPath));

            node.cluster().state(ClusterState.ACTIVE);

            if (cacheDump)
                checkCacheDump(node);
            else if (incSnp)
                checkIncrementalSnapshot(node);
            else
                checkSnapshot(node);
        }
        finally {
            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    private ConfigurationClosure oldConfigurationClosure(int i) {
        return new ConfigurationClosure(incSnp, consId(i), customSnpPath, true, cacheGrpInfo);
    }

    /** */
    private void checkSnapshot(IgniteEx node) {
        node.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(cacheGrpInfo.name())).get();

        cacheGrpInfo.checkCaches(node, BASE_CACHE_SIZE);
    }

    /** */
    private void checkIncrementalSnapshot(IgniteEx node) {
        node.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(cacheGrpInfo.name()), 1).get();

        cacheGrpInfo.checkCaches(node, BASE_CACHE_SIZE + ENTRIES_CNT_FOR_INCREMENT);
    }

    /** */
    private void checkCacheDump(IgniteEx node) throws IgniteCheckedException {
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
            node.configuration(),
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
        boolean incSnp,
        String consId,
        boolean customSnpPath
    ) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        // We configure current Ignite version in the same way as the old one.
        new ConfigurationClosure(incSnp, consId, customSnpPath, false, cacheGrpInfo).apply(cfg);

        return cfg;
    }

    /** */
    private static String customSnapshotPath(String relativePath, boolean delIfExist) throws IgniteCheckedException {
        return U.resolveWorkDirectory(U.defaultWorkDirectory(), relativePath, delIfExist).getAbsolutePath();
    }

    /** */
    private static String calcValue(String cacheName, int key) {
        return cacheName + "-organization-" + key;
    }

    /** */
    private String consId(int nodeIdx) {
        return customConsId ? "node-" + nodeIdx : null;
    }

    /**
     * Configuration closure both for old and current Ignite version.
     */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final String consId;

        /** */
        private final boolean incSnp;

        /** */
        private final boolean customSnpPath;

        /** */
        private final boolean delIfExist;

        /** */
        private final CacheGroupInfo cacheGrpInfo;

        /** */
        public ConfigurationClosure(
            boolean incSnp,
            String consId,
            boolean customSnpPath,
            boolean delIfExist,
            CacheGroupInfo cacheGrpInfo
        ) {
            this.consId = consId;
            this.incSnp = incSnp;
            this.customSnpPath = customSnpPath;
            this.delIfExist = delIfExist;
            this.cacheGrpInfo = cacheGrpInfo;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

            cfg.setDataStorageConfiguration(storageCfg);

            cfg.setConsistentId(consId);

            storageCfg.setWalCompactionEnabled(incSnp);

            if (delIfExist) {
                cfg.setCacheConfiguration(
                    cacheGrpInfo.cacheNamesList().stream()
                        .map(cacheName -> new CacheConfiguration<Integer, String>(cacheName)
                            .setGroupName(cacheGrpInfo.name())
                            .setAffinity(new RendezvousAffinityFunction(false, 10))
                        )
                        .toArray(CacheConfiguration[]::new)
                );
            }

            if (customSnpPath) {
                try {
                    cfg.setSnapshotPath(customSnapshotPath(CUSTOM_SNP_RELATIVE_PATH, delIfExist));
                }
                catch (IgniteCheckedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Snapshot creating closure for old Ignite version.
     */
    private static class CreateSnapshotClosure implements IgniteInClosure<Ignite> {
        /** */
        private final boolean incSnp;

        /** */
        private final boolean cacheDump;

        /** */
        private final CacheGroupInfo cacheGrpInfo;

        /** */
        public CreateSnapshotClosure(boolean incSnp, boolean cacheDump, CacheGroupInfo cacheGrpInfo) {
            this.incSnp = incSnp;
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

            if (incSnp) {
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

            cacheNames = new ArrayList<>();

            for (int i = 0; i < cachesCnt; ++i)
                cacheNames.add("test-cache-" + i);
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
