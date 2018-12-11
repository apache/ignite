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

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_CONNECTION_FAILED;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/**
 * Command line handler test.
 */
@RunWith(JUnit4.class)
public class GridCommandHandlerSslTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        stopAllGrids();

    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration());
        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setMaxSize(100 * 1024 * 1024);
        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());
        cfg.getConnectorConfiguration().setSslEnabled(true);
        cfg.setSslContextFactory(GridTestUtils.sslFactory());

        return cfg;
    }

    /**
     * Test activation works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testActivate() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        final CommandHandler cmd = new CommandHandler();
        assertEquals(EXIT_CODE_OK, cmd.execute(Arrays.asList(
            "--activate",
            "--keystore", GridTestUtils.keyStorePath("node01"),
            "--keystore-password", GridTestUtils.keyStorePassword())));

        assertTrue(ignite.cluster().active());

        assertEquals(EXIT_CODE_CONNECTION_FAILED, cmd.execute(Arrays.asList("--deactivate", "--yes")));
    }

}
