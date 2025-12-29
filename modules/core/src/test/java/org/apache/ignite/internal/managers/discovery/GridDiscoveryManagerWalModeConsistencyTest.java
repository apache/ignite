/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for WAL mode consistency validation when nodes join cluster.
 */
public class GridDiscoveryManagerWalModeConsistencyTest extends GridCommonAbstractTest {
    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** */
    private WALMode walMode;

    /** */
    private boolean persistenceEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
                .setGridLogger(testLog);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setWalMode(walMode);

        if (persistenceEnabled) {
            dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        }

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        walMode = null;
        persistenceEnabled = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Tests that nodes with same WAL mode can join cluster successfully.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSameWalModeJoinsSuccessfully() throws Exception {
        walMode = WALMode.LOG_ONLY;
        persistenceEnabled = true;

        IgniteEx ignite0 = startGrid(0);

        IgniteEx ignite1 = startGrid(1);

        assertEquals(2, ignite0.cluster().nodes().size());
        assertEquals(2, ignite1.cluster().nodes().size());
    }

    /**
     * Tests that nodes with different WAL modes cannot join cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDifferentWalModesCannotJoin() throws Exception {
        walMode = WALMode.LOG_ONLY;
        persistenceEnabled = true;

        IgniteEx ignite0 = startGrid(0);

        walMode = WALMode.FSYNC;

        LogListener walModeErrorLsnr = LogListener.matches("WAL mode validation failed")
                .andMatches("LOG_ONLY")
                .andMatches("FSYNC")
                .andMatches("All nodes in the cluster must have the same WALMode configuration")
                .build();
        testLog.registerListener(walModeErrorLsnr);

        GridTestUtils.assertThrowsWithCause(() -> startGrid(1), IgniteCheckedException.class);

        assertTrue(walModeErrorLsnr.check());
        assertEquals(1, ignite0.cluster().nodes().size());
    }


}
