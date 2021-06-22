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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Test suite for checking WAL archive size configuration validation.
 */
public class WalArchiveSizeConfigurationTest extends GridCommonAbstractTest {
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
     * Checks that no warning messages regarding WAL configuration are logged when a node is started in
     * non-persistent mode, even if they are not correct.
     */
    @Test
    public void testNonPersistentConfiguration() throws Exception {
        String logMsg = getPersistentWalLogWarning(false);
        assertTrue(
            "Invalid configuration warning was printed during non-persistent startup: " + logMsg,
            logMsg.isEmpty()
        );
    }

    /**
     * Checks that a warning is logged if legacy WAL configuration parameters are used.
     */
    @Test
    public void testPersistentConfiguration() throws Exception {
        String logMsg = getPersistentWalLogWarning(true);
        assertFalse(
            "Configuration warning was not printed during persistent startup",
            logMsg.isEmpty()
        );
    }

    /**
     * Checks that an exception is thrown if WAL segment size is larger than max WAL archive size.
     */
    @Test
    public void testIncorrectMaxArchiveSizeConfiguration() throws Exception {
        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration()
            .setWalSegmentSize((int)U.MB)
            .setMaxWalArchiveSize(10)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            );

        assertThrowsAnyCause(
            log,
            () -> startGrid(0, (IgniteConfiguration cfg) -> cfg.setDataStorageConfiguration(dataStorageConfiguration)),
            IgniteCheckedException.class,
            "maxWalArchiveSize must be no less than"
        );
    }

    /**
     * Checks that no exceptions are thrown for a special case of unlimited WAL archive size value.
     */
    @Test
    public void testUnlimitedMaxArchiveSizeConfiguration() throws Exception {
        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration()
            .setWalSegmentSize((int) U.MB)
            .setMaxWalArchiveSize(DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            );

        startGrid(0, (IgniteConfiguration cfg) -> cfg.setDataStorageConfiguration(dataStorageConfiguration));
    }

    /**
     * Checks that an exception is thrown if min WAL archive size is larger than max WAL archive size.
     */
    @Test
    public void testIncorrectMinArchiveSizeConfiguration() {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            .setMinWalArchiveSize(Long.MAX_VALUE);

        assertThrowsAnyCause(
            log,
            () -> startGrid(0, (IgniteConfiguration cfg) -> cfg.setDataStorageConfiguration(dsCfg)),
            IgniteCheckedException.class,
            "DataRegionConfiguration.minWalArchiveSize must be less than or equal to"
        );
    }

    /**
     * Checks that no exceptions are thrown when min WAL archive size is correct.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorrectMinArchiveSizeConfiguration() throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        IgniteEx n = startGrid(0, (IgniteConfiguration cfg) -> cfg.setDataStorageConfiguration(dsCfg));

        assertEquals(dsCfg.getMaxWalArchiveSize(), (long)getFieldValue(walMgr(n), "maxWalArchiveSize"));
        assertEquals(dsCfg.getMaxWalArchiveSize() / 2, (long)getFieldValue(walMgr(n), "minWalArchiveSize"));
    }

    /**
     * Starts up a node in persistent or non-persistent mode and retrieves log messages.
     */
    private String getPersistentWalLogWarning(boolean isPersistenceEnabled) throws Exception {
        List<String> msgReceived = Collections.synchronizedList(new ArrayList<>());
        ListeningTestLogger listeningLog = new ListeningTestLogger();
        listeningLog.registerListener(logMsg -> {
            if (logMsg.contains("walHistorySize property is deprecated"))
                msgReceived.add(logMsg);
        });

        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration()
            .setWalHistorySize(123)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(isPersistenceEnabled)
            );

        startGrid(0, (IgniteConfiguration cfg) ->
            cfg
                .setDataStorageConfiguration(dataStorageConfiguration)
                .setGridLogger(listeningLog)
        );

        assertTrue("Received more messages than expected: " + msgReceived, msgReceived.size() <= 1);
        return msgReceived.isEmpty() ? "" : msgReceived.get(0);
    }
}
