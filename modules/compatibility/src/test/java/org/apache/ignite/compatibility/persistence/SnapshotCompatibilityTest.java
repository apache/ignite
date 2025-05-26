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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

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
        doRestoreTest(false, false, node -> {
            node.snapshot().restoreSnapshot(SNAPSHOT_NAME, cacheGrpsCfg.cacheGroupNames()).get();

            cacheGrpsCfg.cacheGroupInfos().forEach(cacheGrpInfo -> cacheGrpInfo.checkCaches(node, BASE_CACHE_SIZE));
        });
    }

    /** */
    @Test
    public void testIncrementalSnapshotRestore() throws Exception {
        assumeTrue("Incremental snapshots require same consistentID", customConsId);

        assumeTrue("https://issues.apache.org/jira/browse/IGNITE-25096", oldNodesCnt == 1);

        doRestoreTest(true, false, node -> {
            node.snapshot().restoreSnapshot(SNAPSHOT_NAME, cacheGrpsCfg.cacheGroupNames(), 1).get();

            cacheGrpsCfg.cacheGroupInfos().forEach(
                cacheGrpInfo -> cacheGrpInfo.checkCaches(node, BASE_CACHE_SIZE + ENTRIES_CNT_FOR_INCREMENT)
            );
        });
    }

    /** */
    @Test
    public void testDumpRestore() throws Exception {
        doRestoreTest(false, true, node -> {
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

                new DumpReader(
                    new DumpReaderConfiguration(CACHE_DUMP_NAME, snpPathResolver.snpDir(false), node.configuration(), consumer), log
                ).run();

                assertEquals(cacheGrpsCfg, foundCacheGrpsInfo);

                cacheGrpsCfg.cacheNames().forEach(
                    cacheName -> assertEquals(BASE_CACHE_SIZE, foundCacheSizes.get(cacheName).intValue())
                );
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /** */
    private void doRestoreTest(boolean incSnp, boolean cacheDump, Consumer<IgniteEx> curNodeChecker) throws Exception {
        for (int i = 1; i <= oldNodesCnt; ++i) {
            startGrid(
                i,
                OLD_IGNITE_VERSION,
                new ConfigurationClosure(incSnp, consId(i), snpPathResolver.snpDir(true), true, cacheGrpsCfg),
                i == oldNodesCnt ? new CreateSnapshotClosure(incSnp, cacheDump, cacheGrpsCfg) : null
            );
        }

        stopAllGrids();

        cleanPersistenceDir(true);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        // We configure current Ignite version in the same way as the old one.
        new ConfigurationClosure(incSnp, consId(1), snpPathResolver.snpDir(false), false, cacheGrpsCfg).apply(cfg);

        IgniteEx node = startGrid(cfg);

        node.cluster().state(ClusterState.ACTIVE);

        curNodeChecker.accept(node);
    }

    /** */
    private String consId(int nodeIdx) {
        return customConsId ? "node-" + nodeIdx : null;
    }
}
