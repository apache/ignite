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
import java.util.function.Consumer;
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

import static org.junit.Assume.assumeTrue;

/** */
@RunWith(Parameterized.class)
public class SnapshotCompatibilityTest extends SnapshotCompatibilityAbstractTest {
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
    @Parameterized.Parameters(name = "customConsId={0}, customSnpDir={1}, oldNodesCnt={2}")
    public static Collection<Object[]> data() {
        return GridTestUtils.cartesianProduct(
            List.of(true, false),
            List.of(true, false),
            List.of(1, 3)
        );
    }

    /** */
    private final SnapshotPathResolver snpPathResolver = new SnapshotPathResolver(customSnpDir);

    /** */
    @Test
    public void testSnapshotRestore() throws Exception {
        for (int i = 1; i <= oldNodesCnt; ++i) {
            startGrid(
                i,
                OLD_IGNITE_VERSION,
                new ConfigurationClosure(consId(i), snpDir(true), true, cacheGrpsCfg),
                i == oldNodesCnt ? new CreateSnapshotClosure(cacheGrpsCfg) : null
            );
        }

        stopAllGrids();

        cleanPersistenceDir(true);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        // We configure current Ignite version in the same way as the old one.
        new ConfigurationClosure(consId(1), snpDir(false), false, cacheGrpsCfg).apply(cfg);

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
    private void checkCacheDump(IgniteEx node) throws IgniteCheckedException {
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

    /** */
    private void doRestoreTest(boolean incSnp, boolean cacheDump, Consumer<IgniteEx> curNodeChecker) throws Exception {
        for (int i = 1; i <= oldNodesCnt; ++i) {
            startGrid(
                i,
                OLD_IGNITE_VERSION,
                new ConfigurationClosure(incSnp, consId(i), snpDir(true), true, cacheGrpsCfg),
                i == oldNodesCnt ? new CreateSnapshotClosure(incSnp, cacheDump, cacheGrpsCfg) : null
            );
        }

        stopAllGrids();

        cleanPersistenceDir(true);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        // We configure current Ignite version in the same way as the old one.
        new ConfigurationClosure(incSnp, consId(1), snpDir(false), false, cacheGrpsCfg).apply(cfg);

        IgniteEx node = startGrid(cfg);

        node.cluster().state(ClusterState.ACTIVE);

        curNodeChecker.accept(node);
    }

    /** */
    private String consId(int nodeIdx) {
        return customConsId ? "node-" + nodeIdx : null;
    }
}
