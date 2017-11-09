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

import java.io.File;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Command line handler test.
 */
public class GridCommandHandlerTest extends GridCommonAbstractTest {
    /**
     * @return Folder in work directory.
     * @throws IgniteCheckedException If failed to resolve folder name.
     */
    protected File folder(String folder) throws IgniteCheckedException {
        return U.resolveWorkDirectory(U.defaultWorkDirectory(), folder, false);
    }

    /**
     * Delete all files created by database engine during test.
     */
    private void cleanupDbFiles() throws IgniteCheckedException {
        deleteRecursively(folder("db"));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanupDbFiles();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanupDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(100 * 1024 * 1024));

        cfg.setDataStorageConfiguration(memCfg);

        DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();
        dsCfg.setWalMode(WALMode.LOG_ONLY);
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        return cfg;
    }

    /**
     * Test activation works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testActivate() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.active());

        CommandHandler cmd = new CommandHandler();

        assertEquals(0, cmd.execute("--activate"));

        assertTrue(ignite.active());
    }

    /**
     * Test deactivation works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testDeactivate() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.active());

        ignite.active(true);

        assertTrue(ignite.active());

        CommandHandler cmd = new CommandHandler();

        assertEquals(0, cmd.execute("--deactivate"));

        assertFalse(ignite.active());
    }

    /**
     * Test cluster active state works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testState() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.active());

        CommandHandler cmd = new CommandHandler();

        assertEquals(0, cmd.execute("--state"));

        ignite.active(true);

        assertEquals(0, cmd.execute("--state"));
    }

    /**
     * Test baseline manipulations works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaseline() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.active());

        ignite.active(true); // Baseline can be changed only on active cluster.

        CommandHandler cmd = new CommandHandler();

        cmd.execute("--baseline");
    }
}
