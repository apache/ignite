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

package org.apache.ignite.internal.processors.cache.persistence.standbycluster.join;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Printout tests
 */
public class PrintoutTest extends GridCommonAbstractTest {
    /** PDS. */
    private boolean pds = false;

    /** Active on start. */
    private boolean activeOnStart = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (pds) {
            cfg.setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setMaxSize(20 * 1024 * 1024).setPersistenceEnabled(true)
                )
            );
        }

        cfg.setGridLogger(new NullLogger() {
            @Override public boolean isQuiet() {
                return true;
            }
        });

        cfg.setActiveOnStart(activeOnStart);

        return cfg;
    }

    /**
     * @param baos Baos.
     */
    private boolean checkNotActiveMessage(ByteArrayOutputStream baos) {
        boolean res = baos.toString().indexOf(">>> Ignite cluster is not active") >= 0;

        baos.reset();

        return res;
    }


    /**
     * Printout active on start cluster.
     */
    public void testPrintoutActivated() throws Exception {
        pds = false;
        activeOnStart = false;

        cleanPersistenceDir();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        PrintStream ps = new PrintStream(baos);

        System.setOut(ps);

        IgniteEx ignite1 = startGrid(1);

        assertTrue(checkNotActiveMessage(baos));

        stopAllGrids();
    }

    /**
     * Printout active on start cluster.
     */
    public void testPrintoutNotActivated() throws Exception {
        pds = false;
        activeOnStart = true;

        cleanPersistenceDir();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        PrintStream ps = new PrintStream(baos);

        System.setOut(ps);

        IgniteEx ignite1 = startGrid(1);

        assertFalse(checkNotActiveMessage(baos));

        stopAllGrids();
    }

    /**
     * Printout not active cluster with PDS.
     */
    public void testPrintoutNotActivatedPds() throws Exception {
        pds = true;
        activeOnStart = false;

        cleanPersistenceDir();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        PrintStream ps = new PrintStream(baos);

        System.setOut(ps);

        IgniteEx ignite1 = startGrid(1);

        // Cluster started in inactive state.
        assertTrue(checkNotActiveMessage(baos));

        IgniteEx ignite2 = startGrid(2);

        // Node joined in inactive state.
        assertTrue(checkNotActiveMessage(baos));

        final CountDownLatch latch = new CountDownLatch(1);

        ((GridCacheDatabaseSharedManager)ignite1.context().cache().context().database()).addCheckpointListener(
            new DbCheckpointListener() {
                @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
                    doSleep(5_000L);

                    latch.countDown();
                }
            }
        );

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                ignite2.cluster().active(true);
            }
        });

        startGrid(3);

        // Node joined, cluster in transition state
        assertFalse(checkNotActiveMessage(baos));

        latch.await();

        startGrid(4);

        // Node joined, cluster in active state
        assertFalse(checkNotActiveMessage(baos));

        stopAllGrids();
    }
}
