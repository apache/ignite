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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
import org.apache.ignite.internal.util.typedef.internal.CU;
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
    private static final Map<String, String> cacheToGrp = Map.of(
        "singleCache", "singleCache",
        "testCache1", "testCacheGrp",
        "testCache2", "testCacheGrp"
    );

    /** */
    @Parameterized.Parameter
    public boolean customConsId;

    /** */
    @Parameterized.Parameter(1)
    public int oldNodesCnt;

    /** */
    @Parameterized.Parameters(name = "customConsId={0}, oldNodesCnt={1}")
    public static Collection<Object[]> data() {
        return GridTestUtils.cartesianProduct(
            List.of(true, false),
            List.of(1, 3)
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testSnapshotRestore() throws Exception {
        for (int i = 1; i <= oldNodesCnt; ++i) {
            startGrid(
                i,
                OLD_IGNITE_VERSION,
                new ConfigurationClosure(consId(i), true, cacheToGrp),
                i == oldNodesCnt ? new CreateSnapshotClosure(cacheToGrp) : null
            );
        }

        stopAllGrids();

        cleanPersistenceDir(true);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        // We configure current Ignite version in the same way as the old one.
        new ConfigurationClosure(consId(1), false, cacheToGrp).apply(cfg);

        IgniteEx node = startGrid(cfg);

        node.cluster().state(ClusterState.ACTIVE);

        // Incremental snapshots require same consistentID
        // https://issues.apache.org/jira/browse/IGNITE-25096
        if (customConsId && oldNodesCnt == 1)
            checkIncrementalSnapshot(node);
        else
            checkSnapshot(node);

        checkCacheDump(node);
    }

    /** */
    private static String calcValue(String cacheName, int key) {
        return cacheName + "-organization-" + key;
    }

    /** */
    private void checkCaches(Ignite ign, int expectedCacheSize) {
        cacheToGrp.keySet().forEach(cacheName -> {
            IgniteCache<Integer, String> cache = ign.cache(cacheName);

            assertEquals(expectedCacheSize, cache.size());

            for (int i = 0; i < expectedCacheSize; ++i)
                assertEquals(calcValue(cache.getName(), i), cache.get(i));
        });
    }

    /** */
    private void checkSnapshot(IgniteEx node) {
        node.snapshot().restoreSnapshot(SNAPSHOT_NAME, new HashSet<>(cacheToGrp.values())).get();

        checkCaches(node, BASE_CACHE_SIZE);
    }

    /** */
    private void checkIncrementalSnapshot(IgniteEx node) {
        node.snapshot().restoreSnapshot(SNAPSHOT_NAME, new HashSet<>(cacheToGrp.values()), 1).get();

        checkCaches(node, BASE_CACHE_SIZE + ENTRIES_CNT_FOR_INCREMENT);
    }

    /** */
    private void checkCacheDump(IgniteEx node) throws IgniteCheckedException {
        Map<String, String> foundCacheToGrp = new HashMap<>();

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

                    foundCacheToGrp.put(ccfg.getName(), CU.cacheOrGroupName(ccfg));
                });
            }

            @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
                data.forEachRemaining(dumpEntry -> {
                    assertNotNull(dumpEntry);

                    Integer key = (Integer)dumpEntry.key();
                    String val = (String)dumpEntry.value();

                    Optional<String> cacheName = cacheToGrp.keySet().stream().filter(val::startsWith).findFirst();

                    assertTrue(cacheName.isPresent());

                    assertEquals(calcValue(cacheName.get(), key), val);

                    foundCacheSizes.merge(cacheName.get(), 1, Integer::sum);
                });
            }

            @Override public void stop() {
                // No-op
            }
        };

        new DumpReader(
            new DumpReaderConfiguration(
                CACHE_DUMP_NAME,
                U.resolveWorkDirectory(U.defaultWorkDirectory(), "snapshots", false).getAbsolutePath(),
                node.configuration(),
                consumer
            ),
            log
        ).run();

        assertEquals(cacheToGrp, foundCacheToGrp);

        cacheToGrp.keySet().forEach(name -> assertEquals(BASE_CACHE_SIZE, foundCacheSizes.get(name).intValue()));
    }

    /** */
    private String consId(int nodeIdx) {
        return customConsId ? "node-" + nodeIdx : null;
    }

    /** Configuration closure both for old and current Ignite version. */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final String consId;

        /** */
        private final boolean delIfExist;

        /** */
        private final Map<String, String> cacheToGrp;

        /** */
        private final String workDir;

        /** */
        public ConfigurationClosure(
            String consId,
            boolean delIfExist,
            Map<String, String> cacheToGrp
        ) throws IgniteCheckedException {
            this.consId = consId;
            this.delIfExist = delIfExist;
            this.cacheToGrp = cacheToGrp;
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
                    cacheToGrp.entrySet().stream()
                        .map(e -> new CacheConfiguration<Integer, String>(e.getKey())
                            .setGroupName(Objects.equals(e.getKey(), e.getValue()) ? null : e.getValue())
                            .setAffinity(new RendezvousAffinityFunction(false, 10))
                        )
                        .toArray(CacheConfiguration[]::new)
                );
            }
        }
    }

    /** Snapshot creating closure both for old and current Ignite version. */
    private static class CreateSnapshotClosure implements IgniteInClosure<Ignite> {
        /** */
        private final Map<String, String> cacheToGrp;

        /** */
        public CreateSnapshotClosure(Map<String, String> cacheToGrp) {
            this.cacheToGrp = cacheToGrp;
        }

        /** {@inheritDoc} */
        @Override public void apply(Ignite ign) {
            ign.cluster().state(ClusterState.ACTIVE);

            cacheToGrp.keySet().forEach(cacheName -> addItemsToCache(ign.cache(cacheName), 0, BASE_CACHE_SIZE));

            ign.snapshot().createSnapshot(SNAPSHOT_NAME).get();

            ign.snapshot().createDump(CACHE_DUMP_NAME, cacheToGrp.values()).get();

            // Incremental snapshots require same consistentID
            // https://issues.apache.org/jira/browse/IGNITE-25096
            if (ign.configuration().getConsistentId() != null && ign.cluster().nodes().size() == 1) {
                cacheToGrp.keySet().forEach(
                    cacheName -> addItemsToCache(ign.cache(cacheName), BASE_CACHE_SIZE, ENTRIES_CNT_FOR_INCREMENT)
                );

                ign.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get();
            }
        }

        /** */
        private static void addItemsToCache(IgniteCache<Integer, String> cache, int startIdx, int cnt) {
            for (int i = startIdx; i < startIdx + cnt; ++i)
                cache.put(i, calcValue(cache.getName(), i));
        }
    }
}
