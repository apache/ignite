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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpConsumer;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.dump.DumpReader;
import org.apache.ignite.dump.DumpReaderConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assume.assumeFalse;

/**
 *
 */
public class SnapshotCompatibilityTest extends IgniteNodeFileTreeCompatibilityAbstractTest {
    /** */
    @Parameter(5)
    public int oldNodesCnt;

    /** */
    @Parameters(name = "incSnp={0}, customConsId={1}, cacheDump={2}, customSnpPath={3}, testCacheGrp={4}, oldNodesCnt={5}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<>();

        for (boolean incSnp : Arrays.asList(true, false))
            for (boolean customConsId: Arrays.asList(true, false))
                for (boolean cacheDump : Arrays.asList(true, false))
                    for (boolean customSnpPath : Arrays.asList(true, false))
                        for (boolean testCacheGrp : Arrays.asList(true, false))
                            for (int oldNodesCnt : Arrays.asList(1, 3))
                                if (!incSnp || (!cacheDump && customConsId && oldNodesCnt == 1))
                                    data.add(new Object[]{incSnp, customConsId, cacheDump, customSnpPath, testCacheGrp, oldNodesCnt});

        return data;
    }

    /** */
    @Test
    public void testSnapshotRestore() throws Exception {
        assumeFalse("Incremental snapshots for cache dump not supported", incSnp && cacheDump);

        assumeFalse("Incremental snapshots require same consistentID", incSnp && !customConsId);

        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-25096", incSnp && oldNodesCnt == 3);

        try {
            for (int i = 1; i < oldNodesCnt; ++i) {
                startGrid(
                    i,
                    OLD_IGNITE_VERSION,
                    new ConfigurationClosure(incSnp, consId(customConsId, i), customSnpPath, true, cacheGrpInfo)
                );
            }

            startGrid(
                oldNodesCnt,
                OLD_IGNITE_VERSION,
                new ConfigurationClosure(incSnp, consId(customConsId, oldNodesCnt), customSnpPath, true, cacheGrpInfo),
                new CreateSnapshotClosure(incSnp, cacheDump, cacheGrpInfo)
            );

            stopAllGrids();

            cleanPersistenceDir(true);

            IgniteEx node = startGrid(currentIgniteConfiguration(incSnp, consId(customConsId, 1), customSnpPath));

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

        new DumpReader(
            new DumpReaderConfiguration(
                CACHE_DUMP_NAME,
                snpDir(customSnpPath, U.defaultWorkDirectory(), false),
                node.configuration(),
                consumer
            ),
            log
        ).run();

        cacheGrpInfo.cacheNamesList().forEach(
            cacheName -> assertEquals(BASE_CACHE_SIZE, (int)foundCacheSizes.get(cacheName))
        );

        assertTrue(cacheGrpInfo.cacheNamesList().containsAll(foundCacheNames));
        assertEquals(cacheGrpInfo.cacheNamesList().size(), foundCacheNames.size());
    }

    /** */
    private @NotNull IgniteConfiguration currentIgniteConfiguration(boolean incSnp, String consId, boolean customSnpPath) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        // We configure current Ignite version in the same way as the old one.
        new ConfigurationClosure(incSnp, consId, customSnpPath, false, cacheGrpInfo).apply(cfg);

        return cfg;
    }
}
