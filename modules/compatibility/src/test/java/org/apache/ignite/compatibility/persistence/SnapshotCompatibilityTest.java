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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
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

/**
 *
 */
public class SnapshotCompatibilityTest extends IgniteNodeFileTreeCompatibilityAbstractTest {
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
}
