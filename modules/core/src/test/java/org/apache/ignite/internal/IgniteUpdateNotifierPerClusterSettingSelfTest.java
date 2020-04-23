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

package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.cluster.ClusterProcessor;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;

/**
 */
public class IgniteUpdateNotifierPerClusterSettingSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_UPDATE_NOTIFIER, value = "true")
    public void testNotifierEnabledForCluster() throws Exception {
        checkNotifierStatusForCluster(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_UPDATE_NOTIFIER, value = "false")
    public void testNotifierDisabledForCluster() throws Exception {
        checkNotifierStatusForCluster(false);
    }

    /**
     * @param enabled Notifier status.
     * @throws Exception If failed.
     */
    private void checkNotifierStatusForCluster(boolean enabled) throws Exception {
        IgniteEx grid1 = startGrid(0);

        checkNotifier(grid1, enabled);

        System.setProperty(IGNITE_UPDATE_NOTIFIER, String.valueOf(!enabled));

        IgniteEx grid2 = startGrid(1);

        checkNotifier(grid2, enabled);

        IgniteEx grid3 = startClientGrid(2);

        checkNotifier(grid3, enabled);

        // Failover.
        stopGrid(0); // Kill oldest.

        IgniteEx grid4 = startGrid(3);

        checkNotifier(grid4, enabled);

        IgniteEx grid5 = startClientGrid(4);

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
