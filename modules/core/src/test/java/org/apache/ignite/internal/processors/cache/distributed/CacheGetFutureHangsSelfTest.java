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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Test for reproducing problems during simultaneously Ignite instances stopping and cache requests executing.
 */
public class CacheGetFutureHangsSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 8;

    /** Grids. */
    private static Ignite[] grids;

    /** Ids. */
    private static String[] ids;

    /** Flags. */
    private static AtomicBoolean[] flags;

    /** Futs. */
    private static Collection<IgniteInternalFuture> futs;

    /** Alive grids. */
    private static Set<Integer> aliveGrids;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        OptimizedMarshaller marsh = new OptimizedMarshaller();
        marsh.setRequireSerializable(false);

        cfg.setMarshaller(marsh);

        CacheConfiguration ccfg = defaultCacheConfiguration();
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setWriteSynchronizationMode(PRIMARY_SYNC);
        ccfg.setNearConfiguration(null);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailover() throws Exception {
        int cnt = 10;

        for (int i = 0; i < cnt; i++) {
            try {
                U.debug("*** Iteration " + (i + 1) + '/' + cnt);

                init();

                doTestFailover();
            }
            finally {
                stopAllGrids();
            }
        }
    }

    /**
     * Initializes test.
     */
    private void init() {
        grids = new Ignite[GRID_CNT + 1];

        ids = new String[GRID_CNT + 1];

        aliveGrids = new HashSet<>();

        flags = new AtomicBoolean[GRID_CNT + 1];

        futs = new ArrayList<>();
    }

    /**
     * Executes one test iteration.
     */
    private void doTestFailover() throws Exception {
        try {
            for (int i = 0; i < GRID_CNT + 1; i++) {
                final IgniteEx grid = startGrid(i);

                grids[i] = grid;

                ids[i] = grid.localNode().id().toString();

                aliveGrids.add(i);

                flags[i] = new AtomicBoolean();
            }

            for (int i = 0; i < GRID_CNT + 1; i++) {
                final int gridIdx = i;

                futs.add(multithreadedAsync(new Runnable() {
                    @Override public void run() {
                        IgniteCache cache = grids[gridIdx].cache(null);

                        while (!flags[gridIdx].get()) {
                            int idx = ThreadLocalRandom.current().nextInt(GRID_CNT + 1);

                            String id = ids[idx];

                            if (id != null /*&& grids[gridIdx] != null*/) {
                                //U.debug("!!! Grid containsKey start " + gridIdx);
                                cache.containsKey(id);
                                //U.debug("!!! Grid containsKey finished " + gridIdx);
                            }

                            try {
                                Thread.sleep(ThreadLocalRandom.current().nextLong(50));
                            }
                            catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }, 1, "containsKey-thread-" + i));

                futs.add(multithreadedAsync(new Runnable() {
                    @Override public void run() {
                        IgniteCache cache = grids[gridIdx].cache(null);

                        while (!flags[gridIdx].get()) {
                            int idx = ThreadLocalRandom.current().nextInt(GRID_CNT + 1);

                            String id = ids[idx];

                            if (id != null /*&& grids[gridIdx] != null*/) {
                                //U.debug("!!! Grid put start " + gridIdx);
                                cache.put(id, UUID.randomUUID());
                                //U.debug("!!! Grid put finished " + gridIdx);
                            }

                            try {
                                Thread.sleep(ThreadLocalRandom.current().nextLong(50));
                            }
                            catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }, 1, "put-thread-" + i));
            }

            while (aliveGrids.size() > 1) {
                final int gridToKill = ThreadLocalRandom.current().nextInt(GRID_CNT) + 1;

                if (gridToKill > 0 && grids[gridToKill] != null) {
                    U.debug("!!! Trying to kill grid " + gridToKill);

                    //synchronized (mons[gridToKill]) {
                        U.debug("!!! Grid stop start " + gridToKill);

                        grids[gridToKill].close();

                        aliveGrids.remove(gridToKill);

                        grids[gridToKill] = null;

                        flags[gridToKill].set(true);

                        U.debug("!!! Grid stop finished " + gridToKill);
                    //}
                }
            }

            Thread.sleep(ThreadLocalRandom.current().nextLong(100));
        }
        finally {
            flags[0].set(true);

            for (IgniteInternalFuture fut : futs)
                fut.get();
        }
    }
}