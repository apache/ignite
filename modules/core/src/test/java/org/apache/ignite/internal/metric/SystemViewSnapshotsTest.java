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

package org.apache.ignite.internal.metric;

import java.util.List;
import com.google.common.collect.Lists;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.spi.systemview.view.SnapshotView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_NAME;
import static org.apache.ignite.spi.systemview.view.SnapshotView.SNAPSHOT_SYS_VIEW;

/** Tests for {@link SystemView} for snapshots. */
public class SystemViewSnapshotsTest extends SystemViewAbstractTest {
    /** */
    @Test
    public void testSnapshot() throws Exception {
        cleanPersistenceDir();

        String dfltCacheGrp = "testGroup";

        String testSnap0 = "testSnap0";
        String testSnap1 = "testSnap1";

        try (IgniteEx ignite = startGrid(getConfiguration()
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setGroupName(dfltCacheGrp))
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setName("pds").setPersistenceEnabled(true)
                ).setWalCompactionEnabled(true)))
        ) {
            ignite.cluster().state(ClusterState.ACTIVE);

            ignite.cache(DEFAULT_CACHE_NAME).put(1, 1);

            ignite.snapshot().createSnapshot(testSnap0).get();
            ignite.snapshot().createSnapshot(testSnap1).get(getTestTimeout());
            ignite.snapshot().createIncrementalSnapshot(testSnap1).get(getTestTimeout());
            ignite.snapshot().createIncrementalSnapshot(testSnap1).get(getTestTimeout());

            SystemView<SnapshotView> views = ignite.context().systemView().view(SNAPSHOT_SYS_VIEW);

            List<T2<String, Integer>> exp = Lists.newArrayList(
                new T2<>(testSnap0, null),
                new T2<>(testSnap1, null),
                new T2<>(testSnap1, 1),
                new T2<>(testSnap1, 2));

            assertEquals(4, views.size());

            for (SnapshotView v: views) {
                assertTrue(exp.remove(new T2<>(v.name(), v.incrementIndex())));

                assertEquals(ignite.localNode().consistentId().toString(), v.consistentId());
                assertNotNull(v.snapshotRecordSegment());
                assertTrue("snapshotTime should be non-zero value",
                        v.snapshotTime() > 0);

                Integer incIdx = v.incrementIndex();

                if (incIdx == null) {
                    assertEquals(ignite.localNode().consistentId().toString(), v.baselineNodes());
                    assertEquals(String.join(",", dfltCacheGrp, METASTORAGE_CACHE_NAME), v.cacheGroups());
                    assertEquals("FULL", v.type());
                }
                else
                    assertEquals("INCREMENTAL", v.type());
            }

            assertTrue(exp.isEmpty());
        }
    }
}
