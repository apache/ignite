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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.compatibility.persistence.CompatibilityTestCore.ConfigurationClosure;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpConsumer;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.dump.DumpReader;
import org.apache.ignite.dump.DumpReaderConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.compatibility.persistence.CompatibilityTestCore.BASE_CACHE_SIZE;
import static org.apache.ignite.compatibility.persistence.CompatibilityTestCore.CACHE_DUMP_NAME;
import static org.apache.ignite.compatibility.persistence.CompatibilityTestCore.CreateSnapshotClosure;
import static org.apache.ignite.compatibility.persistence.CompatibilityTestCore.ENTRIES_CNT_FOR_INCREMENT;
import static org.apache.ignite.compatibility.persistence.CompatibilityTestCore.INCREMENTAL_SNAPSHOTS_FOR_CACHE_DUMP_NOT_SUPPORTED;
import static org.apache.ignite.compatibility.persistence.CompatibilityTestCore.SNAPSHOT_NAME;
import static org.apache.ignite.compatibility.persistence.CompatibilityTestCore.calcValue;
import static org.apache.ignite.compatibility.persistence.CompatibilityTestCore.snpDir;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/** */
@RunWith(Parameterized.class)
public class SnapshotCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** */
    private static final String OLD_IGNITE_VERSION = Arrays.stream(IgniteReleasedVersion.values())
        .max(Comparator.comparing(IgniteReleasedVersion::version))
        .map(IgniteReleasedVersion::toString)
        .orElseThrow(() -> new IllegalStateException("Enum is empty"));

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
    public boolean cacheDump;

    /** */
    @Parameterized.Parameter(3)
    public boolean customSnpPath;

    /** */
    @Parameterized.Parameter(4)
    public boolean testCacheGrp;

    /** */
    @Parameterized.Parameter(5)
    public int oldNodesCnt;

    /** */
    private CompatibilityTestCore core;

    /** */
    @Parameterized.Parameters(name = "incSnp={0}, customConsId={1}, cacheDump={2}, customSnpPath={3}, testCacheGrp={4}, oldNodesCnt={5}")
    public static Collection<Object[]> data() {
        return GridTestUtils.cartesianProduct(
            Arrays.asList(true, false),
            Arrays.asList(true, false),
            Arrays.asList(true, false),
            Arrays.asList(true, false),
            Arrays.asList(true, false),
            Arrays.asList(1, 3)
        );
    }

    /** */
    @Before
    public void setUp() {
        core = new CompatibilityTestCore(customConsId, customSnpPath, testCacheGrp);
    }

    /** */
    @Test
    public void testSnapshotRestore() throws Exception {
        if (incSnp) {
            assumeFalse(INCREMENTAL_SNAPSHOTS_FOR_CACHE_DUMP_NOT_SUPPORTED, cacheDump);

            assumeTrue("Incremental snapshots require same consistentID", customConsId);

            assumeTrue("https://issues.apache.org/jira/browse/IGNITE-25096", oldNodesCnt == 1);
        }

        try {
            for (int i = 1; i <= oldNodesCnt; ++i) {
                startGrid(
                    i,
                    OLD_IGNITE_VERSION,
                    new ConfigurationClosure(incSnp, core.consId(i), customSnpPath, true, core.cacheGrpInfo()),
                    i == oldNodesCnt ? new CreateSnapshotClosure(incSnp, cacheDump, core.cacheGrpInfo()) : null
                );
            }

            stopAllGrids();

            cleanPersistenceDir(true);

            IgniteEx node = startGrid(currentIgniteConfiguration(incSnp, core.consId(1), customSnpPath));

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
        node.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(core.cacheGrpInfo().name())).get();

        core.cacheGrpInfo().checkCaches(node, BASE_CACHE_SIZE);
    }

    /** */
    private void checkIncrementalSnapshot(IgniteEx node) {
        node.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(core.cacheGrpInfo().name()), 1).get();

        core.cacheGrpInfo().checkCaches(node, BASE_CACHE_SIZE + ENTRIES_CNT_FOR_INCREMENT);
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
                assertNotNull(core.cacheGrpInfo());

                caches.forEachRemaining(cache -> {
                    CacheConfiguration<?, ?> ccfg = cache.config();

                    assertNotNull(ccfg);

                    assertEquals(core.cacheGrpInfo().name(), ccfg.getGroupName());

                    foundCacheNames.add(ccfg.getName());
                });
            }

            @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
                assertNotNull(core.cacheGrpInfo());

                data.forEachRemaining(de -> {
                    assertNotNull(de);

                    Integer key = (Integer)de.key();
                    String val = (String)de.value();

                    for (String cacheName : core.cacheGrpInfo().cacheNamesList()) {
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
            snpDir(customSnpPath, U.defaultWorkDirectory(), false),
            node.configuration(),
            consumer
        ), log).run();

        core.cacheGrpInfo().cacheNamesList().forEach(
            cacheName -> assertEquals(BASE_CACHE_SIZE, (int)foundCacheSizes.get(cacheName))
        );

        assertTrue(core.cacheGrpInfo().cacheNamesList().containsAll(foundCacheNames));
        assertEquals(core.cacheGrpInfo().cacheNamesList().size(), foundCacheNames.size());
    }

    /** */
    private @NotNull IgniteConfiguration currentIgniteConfiguration(
        boolean incSnp,
        String consId,
        boolean customSnpPath
    ) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        // We configure current Ignite version in the same way as the old one.
        new ConfigurationClosure(incSnp, consId, customSnpPath, false, core.cacheGrpInfo()).apply(cfg);

        return cfg;
    }
}
