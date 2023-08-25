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

package org.apache.ignite.cdc;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAware;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * This tests check that the following scenario will works correctly.
 */
@RunWith(Parameterized.class)
public class WalRolloverOnStopTest extends GridCommonAbstractTest {
    /** WAL mode. */
    @Parameterized.Parameter
    public WALMode walMode;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "walMode={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{WALMode.BACKGROUND}, {WALMode.LOG_ONLY}, {WALMode.FSYNC}});
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setWalAutoArchiveAfterInactivity(1500L)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test scenario:
     *
     * 0. {@link DataStorageConfiguration#getWalAutoArchiveAfterInactivity()} > 0.
     * 1. Node is gracefully stopping using {@link G#stop(String, boolean)}.
     * 2. T0: {@code Checkpointer#doCheckpoint()} execute last checkpoint on stop and freeze.
     * 3. T1: Rollover segment after inactivity timeout.
     * 4. T2: Archive segment.
     *
     * After restart WAL should log in the next segment.
     * */
    @Test
    public void testWallRollover() throws Exception {
        AtomicLong curIdx = new AtomicLong();

        for (int i = 0; i < 2; i++) {
            IgniteEx ign = startGrid(0);

            GridCacheDatabaseSharedManager db =
                (GridCacheDatabaseSharedManager)ign.context().cache().context().database();

            SegmentAware aware = GridTestUtils.getFieldValue(ign.context().cache().context().wal(), "segmentAware");

            ign.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, Integer> cache = ign.getOrCreateCache("my-cache");

            CountDownLatch waitAfterCp = new CountDownLatch(1);
            AtomicLong cntr = new AtomicLong(0);

            db.addCheckpointListener(new CheckpointListener() {
                @Override public void afterCheckpointEnd(Context ctx) {
                    if (!ign.context().isStopping())
                        return;

                    try {
                        waitAfterCp.await(getTestTimeout(), TimeUnit.MILLISECONDS);

                        cntr.incrementAndGet();
                    }
                    catch (InterruptedException e) {
                        throw new IgniteException(e);
                    }
                }

                @Override public void onMarkCheckpointBegin(Context ctx) {
                    // No-op.
                }

                @Override public void onCheckpointBegin(Context ctx) {
                    // No-op.
                }

                @Override public void beforeCheckpointBegin(Context ctx) {
                    // No-op.
                }
            });

            int maxKey = (i + 1) * 3;

            for (int j = i * 3; j < maxKey; j++)
                cache.put(j, j);

            curIdx.set(aware.curAbsWalIdx());

            IgniteInternalFuture<?> fut = runAsync(() -> {
                try {
                    aware.awaitSegmentArchived(curIdx.get());

                    cntr.incrementAndGet();
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteException(e);
                }
                finally {
                    waitAfterCp.countDown();
                }
            });

            G.stop(ign.name(), false);

            fut.get(getTestTimeout());

            // Checkpoint will happens two time because of segment archivation.
            assertEquals("Should successfully wait for current segment archivation", 3, cntr.get());

            IgniteWalIteratorFactory.IteratorParametersBuilder builder =
                new IgniteWalIteratorFactory.IteratorParametersBuilder()
                    .log(ign.log())
                    .filesOrDirs(
                        U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_WAL_ARCHIVE_PATH, false))
                    .filter((type, ptr) -> type == DATA_RECORD_V2);

            Set<Integer> keys = new HashSet<>();

            try (WALIterator it = new IgniteWalIteratorFactory().iterator(builder)) {
                while (it.hasNext()) {
                    IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

                    DataRecord rec = (DataRecord)tup.get2();

                    for (DataEntry entry : rec.writeEntries())
                        keys.add(entry.key().value(null, false));
                }
            }

            for (int j = 0; j < maxKey; j++)
                assertTrue(keys.contains(j));
        }
    }
}
