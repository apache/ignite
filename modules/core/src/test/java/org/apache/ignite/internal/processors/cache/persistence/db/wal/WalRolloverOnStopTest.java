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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAware;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * This tests check that the following scenario will works correctly.
 */
public class WalRolloverOnStopTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalAutoArchiveAfterInactivity(1500L)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
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
        for (int i = 0; i < 2; i++) {
            IgniteEx ign = startGrid(0);

            GridCacheDatabaseSharedManager db =
                (GridCacheDatabaseSharedManager)ign.context().cache().context().database();

            SegmentAware aware = GridTestUtils.getFieldValue(ign.context().cache().context().wal(), "segmentAware");

            ign.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, Integer> cache = ign.getOrCreateCache("my-cache");

            CountDownLatch waitAfterCp = new CountDownLatch(1);
            AtomicLong cntr = new AtomicLong(0);

            db.addCheckpointListener(new WaitOnLastCheckpoint(ign, waitAfterCp, cntr, getTestTimeout()));

            for (int j = i * 3; j < (i + 1) * 3; j++)
                cache.put(j, j);

            long cutIdx = aware.curAbsWalIdx();

            runAsync(() -> {
                try {
                    aware.awaitSegmentArchived(cutIdx);

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

            // Checkpoint will happens two time because of segment archivation.
            assertEquals("Should successfully wait for current segment archivation", 3, cntr.get());
        }
    }

    /** */
    private static class WaitOnLastCheckpoint implements CheckpointListener {
        /** */
        private final IgniteEx ign;

        /** */
        private final CountDownLatch waitAfterCp;

        /** */
        private final long timeout;

        /** */
        private final AtomicLong cntr;

        /** */
        public WaitOnLastCheckpoint(IgniteEx ign, CountDownLatch waitAfterCp, AtomicLong cntr, long timeout) {
            this.ign = ign;
            this.waitAfterCp = waitAfterCp;
            this.cntr = cntr;
            this.timeout = timeout;

        }

        /** {@inheritDoc} */
        @Override public void afterCheckpointEnd(Context ctx) {
            if (!ign.context().isStopping())
                return;

            try {
                waitAfterCp.await(timeout, TimeUnit.MILLISECONDS);

                cntr.incrementAndGet();
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void onMarkCheckpointBegin(Context ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onCheckpointBegin(Context ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void beforeCheckpointBegin(Context ctx) {
            // No-op.
        }
    }
}
