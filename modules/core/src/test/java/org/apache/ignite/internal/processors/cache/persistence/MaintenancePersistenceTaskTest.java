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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.persistence.PersistenceCleanCachesTaskArg;
import org.apache.ignite.internal.management.persistence.PersistenceCommand;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.persistence.PersistenceTask;
import org.apache.ignite.internal.visor.persistence.PersistenceTaskResult;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import static org.apache.ignite.testframework.GridTestUtils.deleteLastCheckpointEndMarker;

/**
 * Tests for maintenance persistence task.
 */
public class MaintenancePersistenceTaskTest extends GridCommonAbstractTest {
    /** Test cache name. */
    private static final String CACHE_NAME = "test-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(10 * 1024 * 1024)
                        .setPersistenceEnabled(true)
                )

        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests that executing "persistence --info" after "persistence --clean" works.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInfoAfterClean() throws Exception {
        IgniteEx server1 = startGrid("test1");

        server1.cluster().state(ClusterState.ACTIVE);

        server1.getOrCreateCache(CACHE_NAME);

        // Disable WAL for the cache
        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
            assertTrue(client.cluster().isWalEnabled(CACHE_NAME));

            client.cluster().disableWal(CACHE_NAME);

            assertFalse(client.cluster().isWalEnabled(CACHE_NAME));
        }
        catch (Exception e) {
            fail(e.getMessage());
        }

        stopGrid("test1", false);

        deleteLastCheckpointEndMarker(server1);

        try {
            server1 = startGrid("test1");
            fail();
        }
        catch (Exception ignored) {
            // It's ok, we should not be able to start with WAL disabled for a system cache.
            // We will start in the maintenance mode.
        }

        server1.close();

        // Starts in maintenance
        server1 = startGrid("test1");

        assertTrue(server1.context().maintenanceRegistry().isMaintenanceMode());

        PersistenceTaskResult infoResultBeforeClean = executeInfo(server1);
        assertNotNull(infoResultBeforeClean);

        PersistenceTaskResult cleanResult = executeClean(server1);
        assertNotNull(cleanResult);

        PersistenceTaskResult infoResultAfterClean = executeInfo(server1);
        assertNotNull(infoResultAfterClean);

        server1.close();

        // Restart node again, it should start normally
        server1 = startGrid("test1");

        assertFalse(server1.context().maintenanceRegistry().isMaintenanceMode());
    }

    /**
     * Executes "--persistence info".
     *
     * @param node Ignite node.
     * @return Execution's result.
     */
    private PersistenceTaskResult executeInfo(IgniteEx node) {
        VisorTaskArgument<PersistenceCommand.PersistenceTaskArg> infoArgument = new VisorTaskArgument<>(
            node.localNode().id(),
            new PersistenceCommand.PersistenceInfoTaskArg(),
            false
        );

        return node.compute().execute(new PersistenceTask(), infoArgument);
    }

    /**
     * Executes "--persistence clean".
     *
     * @param node Ignite node.
     * @return Execution's result.
     */
    private PersistenceTaskResult executeClean(IgniteEx node) {
        PersistenceCleanCachesTaskArg arg = new PersistenceCleanCachesTaskArg();

        arg.caches(new String[]{CACHE_NAME});

        VisorTaskArgument<PersistenceCommand.PersistenceTaskArg> cleanArgument = new VisorTaskArgument<>(
            node.localNode().id(),
            arg,
            false
        );

        return node.compute().execute(new PersistenceTask(), cleanArgument);
    }
}
