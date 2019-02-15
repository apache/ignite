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

package org.apache.ignite.internal.processors.rest;

import java.util.Map;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Rest processor test.
 */
public class RestProcessorMultiStartSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 3;

    /** */
    private static boolean client = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());
        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        client = false;
    }

    /**
     * Test that multiple nodes can start with JETTY enabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultiStart() throws Exception {
        try {
            for (int i = 0; i < GRID_CNT; i++)
                startGrid(i);

            stopGrid(0);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test that multiple nodes can start with JETTY enabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultiStartWithClient() throws Exception {
        try {
            int clnIdx = GRID_CNT - 1;

            for (int i = 0; i < clnIdx; i++) {
                startGrid(i);

                GridRestProcessor rest = grid(i).context().rest();

                assertNotNull(rest);
                assertFalse(((Map)GridTestUtils.getFieldValue(rest, "handlers")).isEmpty());
            }

            client = true;

            startGrid(clnIdx);

            GridRestProcessor rest = grid(GRID_CNT - 1).context().rest();

            // Check that rest processor doesn't start.
            assertNotNull(rest);
            assertTrue(((Map)GridTestUtils.getFieldValue(rest, "handlers")).isEmpty());
        }
        finally {
            stopAllGrids();
        }
    }
}
