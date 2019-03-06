/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that warning is logged when persistence store directory equals {@code System.getProperty("java.io.tmpdir")}.
 */
public class PersistenceDirectoryWarningLoggingTest extends GridCommonAbstractTest {
    /** Warning message to test. */
    private static final String WARN_MSG_PREFIX = "Persistence store directory is in the temp " +
        "directory and may be cleaned.";

    /** String logger to check. */
    private GridStringLogger log0 = new GridStringLogger();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(log0);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPdsDirWarningSuppressed() throws Exception {
        startGrid();

        assertFalse(log0.toString().contains(WARN_MSG_PREFIX));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPdsDirWarningIsLogged() throws Exception {
        IgniteConfiguration cfg = getConfiguration("0");

        String tempDir = System.getProperty("java.io.tmpdir");

        assertNotNull(tempDir);

        // Emulates that Ignite work directory has not been calculated,
        // and IgniteUtils#workDirectory resolved directory into "java.io.tmpdir"
        cfg.setWorkDirectory(tempDir);

        startGrid(cfg);

        assertTrue(log0.toString().contains(WARN_MSG_PREFIX));
    }
}
