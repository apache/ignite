/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 *
 */
public abstract class WalRolloverRecordLoggingTest extends GridCommonAbstractTest {
    /** */
    private static class RolloverRecord extends CheckpointRecord {
        /** */
        private RolloverRecord() {
            super(null);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(40 * 1024 * 1024))
            .setWalMode(walMode())
            .setWalSegmentSize(4 * 1024 * 1024)
        );

        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler(false, 0));

        return cfg;
    }

    /**
     * @return Wal mode.
     */
    @NotNull public abstract WALMode walMode();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testAvoidInfinityWaitingOnRolloverOfSegment() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        long startTime = U.currentTimeMillis();
        long duration = 5_000;

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(
            () -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();

                while (U.currentTimeMillis() - startTime < duration)
                    cache.put(random.nextInt(100_000), random.nextInt(100_000));
            },
            8, "cache-put-thread");

        Thread t = new Thread(() -> {
            do {
                try {
                    U.sleep(100);
                }
                catch (IgniteInterruptedCheckedException e) {
                    // No-op.
                }

                ig.context().cache().context().database().wakeupForCheckpoint("test");
            } while (U.currentTimeMillis() - startTime < duration);
        });

        t.start();

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        IgniteCacheDatabaseSharedManager dbMgr = ig.context().cache().context().database();

        RolloverRecord rec = new RolloverRecord();

        do {
            try {
                dbMgr.checkpointReadLock();

                try {
                    walMgr.log(rec, RolloverType.NEXT_SEGMENT);
                }
                finally {
                    dbMgr.checkpointReadUnlock();
                }
            }
            catch (IgniteCheckedException e) {
                log.error(e.getMessage(), e);
            }
        } while (U.currentTimeMillis() - startTime < duration);

        fut.get();

        t.join();
    }
}
