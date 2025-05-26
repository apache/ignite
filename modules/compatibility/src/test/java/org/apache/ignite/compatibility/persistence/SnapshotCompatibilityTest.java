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

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
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
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class SnapshotCompatibilityTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    private static final String OLD_IGNITE_VERSION = Collections.max(
        Arrays.asList(IgniteReleasedVersion.values()),
        Comparator.comparing(IgniteReleasedVersion::version)
    ).toString();

    /** */
    private static final String SNAPSHOT_NAME = "test_snapshot";

    /** */
    private static final String CACHE_DUMP_NAME = "test_cache_dump";

    /** */
    private static final int BASE_CACHE_SIZE = 100;

    /** */
    private static final int ENTRIES_CNT_FOR_INCREMENT = 100;

    /** */
    @Parameterized.Parameter
    public boolean customConsId;

    /** */
    @Parameterized.Parameter(1)
    public boolean customSnpDir;

    /** */
    @Parameterized.Parameter(2)
    public int oldNodesCnt;

    /** */
    private final CacheGroupsConfig cacheGrpsCfg = new CacheGroupsConfig(
        Set.of(
            new CacheGroupInfo("singleCache", Collections.singleton("singleCache")),
            new CacheGroupInfo("testCacheGrp", Set.of("testCache1", "testCache2"))
        )
    );

    /** */
    @Parameterized.Parameters(name = "customConsId={0}, customSnpDir={1}, oldNodesCnt={2}")
    public static Collection<Object[]> data() {
        return GridTestUtils.cartesianProduct(
            List.of(true, false),
            List.of(true, false),
            List.of(1, 3)
        );
    }

    /** */
    @Test
    public void testSnapshotRestore() throws Exception {
        boolean incSnp = customConsId && oldNodesCnt == 1;

        for (int i = 1; i <= oldNodesCnt; ++i) {
            startGrid(
                i,
                OLD_IGNITE_VERSION,
                new ConfigurationClosure(consId(i), snpDir(true), true, cacheGrpsCfg),
                i == oldNodesCnt ? new CreateSnapshotClosure(incSnp, cacheGrpsCfg) : null
            );
        }

        stopAllGrids();

        cleanPersistenceDir(true);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        // We configure current Ignite version in the same way as the old one.
        new ConfigurationClosure(consId(1), snpDir(false), false, cacheGrpsCfg).apply(cfg);

        IgniteEx node = startGrid(cfg);

        node.cluster().state(ClusterState.ACTIVE);

        if (incSnp)
            checkIncrementalSnapshot(node);
        else
            checkSnapshot(node);

        checkDump(node);
    }

    /** */
    private void checkSnapshot(IgniteEx node) {
        node.snapshot().restoreSnapshot(SNAPSHOT_NAME, cacheGrpsCfg.cacheGroupNames()).get();

        cacheGrpsCfg.cacheGroupInfos().forEach(cacheGrpInfo -> cacheGrpInfo.checkCaches(node, BASE_CACHE_SIZE));
    }

    /** */
    private void checkIncrementalSnapshot(IgniteEx node) {
        node.snapshot().restoreSnapshot(SNAPSHOT_NAME, cacheGrpsCfg.cacheGroupNames(), 1).get();

        cacheGrpsCfg.cacheGroupInfos().forEach(
            cacheGrpInfo -> cacheGrpInfo.checkCaches(node, BASE_CACHE_SIZE + ENTRIES_CNT_FOR_INCREMENT)
        );
    }

    /** */
    private void checkDump(IgniteEx node) {
        try {
            CacheGroupsConfig foundCacheGrpsInfo = new CacheGroupsConfig();

            Map<String, Integer> foundCacheSizes = new HashMap<>();

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
                    caches.forEachRemaining(cache -> {
                        CacheConfiguration<?, ?> ccfg = cache.config();

                        assertNotNull(ccfg);

                        foundCacheGrpsInfo.addCache(ccfg.getGroupName(), ccfg.getName());
                    });
                }

                @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
                    data.forEachRemaining(dumpEntry -> {
                        assertNotNull(dumpEntry);

                        Integer key = (Integer)dumpEntry.key();
                        String val = (String)dumpEntry.value();

                        Optional<String> cacheName = cacheGrpsCfg.cacheNames().stream().filter(val::startsWith).findFirst();

                        assertTrue(cacheName.isPresent());

                        assertEquals(CacheGroupInfo.calcValue(cacheName.get(), key), val);

                        foundCacheSizes.merge(cacheName.get(), 1, Integer::sum);
                    });
                }

                @Override public void stop() {
                    // No-op.
                }
            };

            new DumpReader(new DumpReaderConfiguration(CACHE_DUMP_NAME, snpDir(false), node.configuration(), consumer), log).run();

            assertEquals(cacheGrpsCfg.cacheGrpInfos, foundCacheGrpsInfo.cacheGrpInfos);

            cacheGrpsCfg.cacheNames().forEach(
                cacheName -> assertEquals(BASE_CACHE_SIZE, foundCacheSizes.get(cacheName).intValue())
            );
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private String consId(int nodeIdx) {
        return customConsId ? "node-" + nodeIdx : null;
    }

    /** */
    private String snpDir(boolean delIfExist) throws IgniteCheckedException {
        return U.resolveWorkDirectory(U.defaultWorkDirectory(), customSnpDir ? "ex_snapshots" : "snapshots", delIfExist).getAbsolutePath();
    }

    /** */
    private String snpPath(String snpName, boolean delIfExist) throws IgniteCheckedException {
        return Paths.get(snpDir(delIfExist), snpName).toString();
    }

    /** Configuration closure both for old and current Ignite version. */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final String consId;

        /** */
        private final String snpDir;

        /** */
        private final boolean delIfExist;

        /** */
        private final CacheGroupsConfig cacheGrpsCfg;

        /** */
        private final String workDir;

        /** */
        public ConfigurationClosure(
            String consId,
            String snpDir,
            boolean delIfExist,
            CacheGroupsConfig cacheGrpsCfg
        ) throws IgniteCheckedException {
            this.consId = consId;
            this.snpDir = snpDir;
            this.delIfExist = delIfExist;
            this.cacheGrpsCfg = cacheGrpsCfg;
            workDir = U.defaultWorkDirectory();
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setWorkDirectory(workDir);

            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

            cfg.setDataStorageConfiguration(storageCfg);

            cfg.setConsistentId(consId);

            storageCfg.setWalCompactionEnabled(true);

            if (delIfExist) {
                cfg.setCacheConfiguration(
                    cacheGrpsCfg.cacheGroupInfos().stream()
                        .flatMap(cacheGrpInfo ->
                            cacheGrpInfo.cacheNames().stream()
                                .map(cacheName -> new CacheConfiguration<Integer, String>(cacheName)
                                    .setGroupName(cacheGrpInfo.name())
                                    .setAffinity(new RendezvousAffinityFunction(false, 10))
                                )
                        )
                        .toArray(CacheConfiguration[]::new)
                );
            }

            cfg.setSnapshotPath(snpDir);
        }
    }

    /** Snapshot creating closure both for old and current Ignite version. */
    private static class CreateSnapshotClosure implements IgniteInClosure<Ignite> {
        /** */
        private final boolean incSnp;

        /** */
        private final CacheGroupsConfig cacheGrpsCfg;

        /** */
        public CreateSnapshotClosure(boolean incSnp, CacheGroupsConfig cacheGrpsCfg) {
            this.incSnp = incSnp;
            this.cacheGrpsCfg = cacheGrpsCfg;
        }

        /** {@inheritDoc} */
        @Override public void apply(Ignite ign) {
            ign.cluster().state(ClusterState.ACTIVE);

            cacheGrpsCfg.cacheGroupInfos().forEach(cacheGrpInfo -> cacheGrpInfo.addItemsToCacheGrp(ign, 0, BASE_CACHE_SIZE));

            ign.snapshot().createSnapshot(SNAPSHOT_NAME).get();

            ign.snapshot().createDump(CACHE_DUMP_NAME, cacheGrpsCfg.cacheGroupNames()).get();

            if (incSnp) {
                cacheGrpsCfg.cacheGroupInfos().forEach(
                    cacheGrpInfo -> cacheGrpInfo.addItemsToCacheGrp(ign, BASE_CACHE_SIZE, ENTRIES_CNT_FOR_INCREMENT)
                );

                ign.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get();
            }
        }
    }

    /** */
    private static class CacheGroupsConfig {
        /** */
        private final Map<String, CacheGroupInfo> cacheGrpInfos = new HashMap<>();

        /** */
        public CacheGroupsConfig() {
            // No-op
        }

        /** */
        public CacheGroupsConfig(Set<CacheGroupInfo> cacheGrpInfos) {
            cacheGrpInfos.forEach(cacheGrpInfo -> this.cacheGrpInfos.put(cacheGrpInfo.name(), cacheGrpInfo));
        }

        /** */
        public Collection<CacheGroupInfo> cacheGroupInfos() {
            return cacheGrpInfos.values();
        }

        /** */
        public Set<String> cacheGroupNames() {
            return new HashSet<>(cacheGrpInfos.keySet());
        }

        /** */
        public Set<String> cacheNames() {
            return cacheGrpInfos.values().stream().flatMap(cacheGrpInfo -> cacheGrpInfo.cacheNames().stream()).collect(Collectors.toSet());
        }

        /** */
        public void addCache(String cacheGrpName, String cacheName) {
            cacheGrpInfos.computeIfAbsent(cacheGrpName, CacheGroupInfo::new).addCacheName(cacheName);
        }
    }

    /** */
    private static class CacheGroupInfo {
        /** */
        private final String name;

        /** */
        private final Set<String> cacheNames;

        /** */
        public CacheGroupInfo(String name) {
            this.name = name;

            cacheNames = new HashSet<>();
        }

        /** */
        public CacheGroupInfo(String name, Set<String> cacheNames) {
            this.name = name;
            this.cacheNames = cacheNames;
        }

        /** */
        public boolean addCacheName(String cacheName) {
            return cacheNames.add(cacheName);
        }

        /** */
        public String name() {
            return name;
        }

        /** */
        public Set<String> cacheNames() {
            return cacheNames;
        }

        /** */
        public static String calcValue(String cacheName, int key) {
            return cacheName + "-organization-" + key;
        }

        /** */
        public void addItemsToCacheGrp(Ignite ign, int startIdx, int cnt) {
            for (String cacheName : cacheNames)
                addItemsToCache(ign.cache(cacheName), startIdx, cnt);
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
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof CacheGroupInfo))
                return false;

            CacheGroupInfo that = (CacheGroupInfo)o;

            return Objects.equals(name, that.name) && Objects.equals(cacheNames, that.cacheNames);
        }

        /** */
        @Override public int hashCode() {
            return Objects.hashCode(name);
        }

        /** */
        private void checkCache(IgniteCache<Integer, String> cache, int expectedSize) {
            assertEquals(expectedSize, cache.size());

            for (int i = 0; i < expectedSize; ++i)
                assertEquals(calcValue(cache.getName(), i), cache.get(i));
        }

        /** */
        private void addItemsToCache(IgniteCache<Integer, String> cache, int startIdx, int cnt) {
            for (int i = startIdx; i < startIdx + cnt; ++i)
                cache.put(i, calcValue(cache.getName(), i));
        }
    }
}
