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

package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cluster.ClusterProcessor;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 */
public class IgniteUpdateNotifierPerClusterSettingSelfTest extends GridCommonAbstractTest {
    /** */
    private String backup;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backup = System.getProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, backup);

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotifierEnabledForCluster() throws Exception {
        checkNotifierStatusForCluster(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotifierDisabledForCluster() throws Exception {
        checkNotifierStatusForCluster(false);
    }

    /**
     * @param enabled Notifier status.
     * @throws Exception If failed.
     */
    private void checkNotifierStatusForCluster(boolean enabled) throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, String.valueOf(enabled));

        IgniteEx grid1 = startGrid(0);

        checkNotifier(grid1, enabled);

        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, String.valueOf(!enabled));

        IgniteEx grid2 = startGrid(1);

        checkNotifier(grid2, enabled);

        client = true;

        IgniteEx grid3 = startGrid(2);

        checkNotifier(grid3, enabled);

        // Failover.
        stopGrid(0); // Kill oldest.

        client = false;

        IgniteEx grid4 = startGrid(3);

        checkNotifier(grid4, enabled);

        client = true;

        IgniteEx grid5 = startGrid(4);

        checkNotifier(grid5, enabled);
    }

    /**
     * @param ignite Node.
     * @param expEnabled Expected notifier status.
     */
    private void checkNotifier(Ignite ignite, boolean expEnabled) {
        ClusterProcessor proc = ((IgniteKernal)ignite).context().cluster();

        if (expEnabled)
            assertNotNull(GridTestUtils.getFieldValue(proc, "updateNtfTimer"));
        else
            assertNull(GridTestUtils.getFieldValue(proc, "updateNtfTimer"));
    }
}
