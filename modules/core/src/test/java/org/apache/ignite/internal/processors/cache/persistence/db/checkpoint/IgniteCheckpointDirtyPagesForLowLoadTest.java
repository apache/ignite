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

package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test generates low load to grid in having some shared groups. Test checks if pages marked dirty after some time will
 * became reasonable low for 1 put.
 */
public class IgniteCheckpointDirtyPagesForLowLoadTest extends GridCommonAbstractTest {
    /** Caches in group. */
    private static final int CACHES_IN_GRP = 1;

    /** Groups. */
    private static final int GROUPS = 1;

    /** Parts. */
    private static final int PARTS = 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        for (int g = 0; g < GROUPS; g++) {
            for (int i = 0; i < CACHES_IN_GRP; i++) {
                CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>().setName("dummyCache" + i + "." + g)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setGroupName("dummyGroup" + g)
                    .setAffinity(new RendezvousAffinityFunction(false, PARTS));

                ccfgs.add(ccfg);
            }
        }
        cfg.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
        );
        dsCfg.setCheckpointFrequency(500);
        dsCfg.setWalMode(WALMode.LOG_ONLY);
        dsCfg.setWalHistorySize(1);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "temp", false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testManyCachesAndNotManyPuts() throws Exception {
        try {
            IgniteEx ignite = startGrid(0);
            ignite.active(true);

            log.info("Saving initial data to caches");

            for (int g = 0; g < GROUPS; g++) {
                for (int c = 0; c < CACHES_IN_GRP; c++) {
                    ignite.cache("dummyCache" + c + "." + g)
                        .putAll(new TreeMap<Long, Long>() {{
                            for (int j = 0; j < PARTS; j++) {
                                // to fill each partition cache with at least 1 element
                                put((long)j, (long)j);
                            }
                        }});
                }
            }

            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)(ignite.context().cache().context().database());

            Collection<Integer> pageCntObserved = new ArrayList<>();

            boolean checkpointWithLowNumOfPagesFound = false;

            for (int i = 0; i < 20; i++) {
                Random random = new Random();
                //touch some entry
                int d = random.nextInt(PARTS) + PARTS;
                int cIdx = random.nextInt(CACHES_IN_GRP);
                int gIdx = random.nextInt(GROUPS);

                String fullname = "dummyCache" + cIdx + "." + gIdx;

                ignite.cache(fullname).put(d, d);

                if (log.isInfoEnabled())
                    log.info("Put to cache [" + fullname + "] value " + d);

                long start = System.nanoTime();
                try {
                    final int cpTimeout = 25000;

                    db.wakeupForCheckpoint("").get(cpTimeout, TimeUnit.MILLISECONDS);
                }
                catch (IgniteFutureTimeoutCheckedException ignored) {
                    long msPassed = U.millisSinceNanos(start);

                    log.error("Timeout during waiting for checkpoint to start:" +
                        " [" + msPassed + "] but checkpoint is not running");

                    continue;
                }

                final int timeout = 5000;
                int currCpPages = waitForCurrentCheckpointPagesCounterUpdated(db, timeout);

                if (currCpPages < 0) {
                    log.error("Timeout during waiting for checkpoint counter to be updated");

                    continue;
                }

                pageCntObserved.add(currCpPages);

                log.info("Current CP pages: " + currCpPages);

                if (currCpPages < PARTS * GROUPS) {
                    checkpointWithLowNumOfPagesFound = true;  //reasonable number of pages in CP
                    break;
                }
            }

            stopGrid(0);

            assertTrue("All checkpoints mark too much pages: " + pageCntObserved,
                checkpointWithLowNumOfPagesFound);

        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Waits counter of pages will be set up. If it is not changed for timeout milliseconds, method returns negative
     * value.
     *
     * @param db DB shared manager.
     * @param timeout milliseconds to wait.
     * @return counter when it becomes non-zero, negative value indicates timeout during wait for update.
     */
    private int waitForCurrentCheckpointPagesCounterUpdated(GridCacheDatabaseSharedManager db, int timeout) {
        int currCpPages = 0;
        long start = System.currentTimeMillis();

        while (currCpPages == 0) {
            LockSupport.parkNanos(U.millisToNanos(1));
            currCpPages = db.getCheckpointer().currentProgress().currentCheckpointPagesCount();

            if (currCpPages == 0 && ((System.currentTimeMillis() - start) > timeout))
                return -1;
        }

        return currCpPages;
    }

}
