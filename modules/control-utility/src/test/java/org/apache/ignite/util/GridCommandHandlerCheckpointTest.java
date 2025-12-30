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

package org.apache.ignite.util;

import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/** Test for checkpoint in control.sh command. */
public class GridCommandHandlerCheckpointTest extends GridCommandHandlerAbstractTest {
    /** */
    private static final String PERSISTENT_REGION_NAME = "pds-reg";

    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** */
    private final LogListener checkpointFinishedLsnr = LogListener.matches("Checkpoint finished").build();

    /** */
    private boolean mixedConfig;

    /** Latch for blocking checkpoint in timeout test. */
    private CountDownLatch blockCheckpointLatch;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        listeningLog.registerListener(checkpointFinishedLsnr);

        cfg.setGridLogger(listeningLog);

        if (mixedConfig) {
            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            storageCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration().setName("default_in_memory_region")
                    .setPersistenceEnabled(false));

            if (igniteInstanceName.contains("persistent_instance")) {
                DataRegionConfiguration persistentRegionCfg = new DataRegionConfiguration();

                storageCfg.setDataRegionConfigurations(persistentRegionCfg.setName(PERSISTENT_REGION_NAME)
                        .setPersistenceEnabled(true));
            }

            cfg.setDataStorageConfiguration(storageCfg);
        }
        else if (!persistenceEnable()) {
            cfg.setDataStorageConfiguration(null);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();
        injectTestSystemOut();

        checkpointFinishedLsnr.reset();
        blockCheckpointLatch = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // Release latch if test was interrupted
        if (blockCheckpointLatch != null) {
            blockCheckpointLatch.countDown();
        }

        stopAllGrids();
        cleanPersistenceDir();

        super.afterTest();
    }

    /** Test checkpoint command with persistence enabled. */
    @Test
    public void testCheckpointPersistenceCluster() throws Exception {
        persistenceEnable(true);

        IgniteEx srv = startGrids(2);
        IgniteEx cli = startClientGrid("client");

        srv.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheCli.put(1, 1);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint"));
        assertTrue(GridTestUtils.waitForCondition(checkpointFinishedLsnr::check, 10_000));
        assertFalse(testOut.toString().contains("persistence disabled"));

        outputContains(": Checkpoint started");

        testOut.reset();

        checkpointFinishedLsnr.reset();

        cacheCli.put(2, 2);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--reason", "test_reason"));

        LogListener checkpointReasonLsnr = LogListener.matches("reason='test_reason'").build();

        listeningLog.registerListener(checkpointReasonLsnr);

        assertTrue(GridTestUtils.waitForCondition(checkpointFinishedLsnr::check, 10_000));
        assertTrue(GridTestUtils.waitForCondition(checkpointReasonLsnr::check, 10_000));
        assertFalse(testOut.toString().contains("persistence disabled"));

        outputContains(": Checkpoint started");

        testOut.reset();

        checkpointFinishedLsnr.reset();

        cacheCli.put(3, 3);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish"));
        assertTrue(checkpointFinishedLsnr.check());
        assertFalse(testOut.toString().contains("persistence disabled"));
    }

    /** Test checkpoint command with in-memory cluster. */
    @Test
    public void testCheckpointInMemoryCluster() throws Exception {
        persistenceEnable(false);

        IgniteEx srv = startGrids(2);

        startClientGrid("client");

        srv.cluster().state(ClusterState.ACTIVE);

        srv.createCache("testCache");

        assertEquals(EXIT_CODE_OK, execute("--checkpoint"));
        assertFalse(checkpointFinishedLsnr.check());

        outputContains("persistence disabled");
    }

    /** Test checkpoint with timeout when checkpoint completes within timeout. */
    @Test
    public void testCheckpointTimeout() throws Exception {
        persistenceEnable(true);

        IgniteEx srv = startGrids(1);

        srv.cluster().state(ClusterState.ACTIVE);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish", "--timeout", "1000"));

        assertTrue(checkpointFinishedLsnr.check());

        assertFalse(testOut.toString().contains("persistence disabled"));
    }

    /** Test checkpoint timeout when checkpoint doesn't complete within timeout. */
    @Test
    public void testCheckpointTimeoutExceeded() throws Exception {
        persistenceEnable(true);

        IgniteEx srv = startGrids(1);

        srv.cluster().state(ClusterState.ACTIVE);

        blockCheckpointLatch = new CountDownLatch(1);

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)srv.context().cache().context().database();

        dbMgr.addCheckpointListener(new CheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) {
                try {
                    blockCheckpointLatch.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override public void onCheckpointBegin(Context ctx) {
                // No-op
            }

            @Override public void beforeCheckpointBegin(Context ctx) {
                // No-op
            }
        });

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish", "--timeout", "500"));

        outputContains("Checkpoint started but not finished within timeout 500 ms");

        blockCheckpointLatch.countDown();

        assertTrue(GridTestUtils.waitForCondition(checkpointFinishedLsnr::check, 10_000));
    }

    /** Mixed cluster test. */
    @Test
    public void testMixedCluster() throws Exception {
        mixedConfig = true;

        IgniteEx node0 = startGrid("in-memory_instance");

        node0.cluster().baselineAutoAdjustEnabled(false);

        IgniteEx node1 = startGrid("persistent_instance");

        node0.cluster().state(ClusterState.ACTIVE);

        assertEquals(2, node0.cluster().nodes().size());

        DataStorageConfiguration node0Storage = node0.configuration().getDataStorageConfiguration();
        DataStorageConfiguration node1Storage = node1.configuration().getDataStorageConfiguration();

        DataRegionConfiguration node0Dflt = node0Storage.getDefaultDataRegionConfiguration();
        DataRegionConfiguration node1Dflt = node1Storage.getDefaultDataRegionConfiguration();

        assertEquals(node0Dflt.getName(), node1Dflt.getName());
        assertEquals(node0Dflt.isPersistenceEnabled(), node1Dflt.isPersistenceEnabled());
        assertEquals(node0Dflt.getMaxSize(), node1Dflt.getMaxSize());

        DataRegionConfiguration[] node1Regions = node1Storage.getDataRegionConfigurations();
        assertEquals(1, node1Regions.length);

        DataRegionConfiguration persistentRegion = node1Regions[0];

        assertEquals(PERSISTENT_REGION_NAME, persistentRegion.getName());
        assertEquals(true, persistentRegion.isPersistenceEnabled());

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish"));

        assertTrue(checkpointFinishedLsnr.check());

        outputContains("persistence disabled");
        outputContains("Checkpoint finished");
    }

    /** */
    private void outputContains(String regexp) {
        assertTrue(Pattern.compile(regexp).matcher(testOut.toString()).find());
    }
}
