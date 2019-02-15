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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test grids starting with non compatible release types.
 */
@RunWith(JUnit4.class)
public class GridReleaseTypeSelfTest extends GridCommonAbstractTest {
    /** */
    private String nodeVer;

    /** */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (clientMode)
            cfg.setClientMode(true);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
            @Override public void setNodeAttributes(Map<String, Object> attrs,
                IgniteProductVersion ver) {
                super.setNodeAttributes(attrs, ver);

                attrs.put(IgniteNodeAttributes.ATTR_BUILD_VER, nodeVer);
            }
        };

        discoSpi.setIpFinder(sharedStaticIpFinder).setForceServerMode(true);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        clientMode = false;

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOsEditionDoesNotSupportRollingUpdates() throws Exception {
        nodeVer = "1.0.0";

        startGrid(0);

        try {
            nodeVer = "1.0.1";

            startGrid(1);

            fail("Exception has not been thrown.");
        }
        catch (IgniteCheckedException e) {
            StringWriter errors = new StringWriter();

            e.printStackTrace(new PrintWriter(errors));

            String stackTrace = errors.toString();

            if (!stackTrace.contains("Local node and remote node have different version numbers"))
                throw e;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOsEditionDoesNotSupportRollingUpdatesClientMode() throws Exception {
        nodeVer = "1.0.0";

        startGrid(0);

        try {
            nodeVer = "1.0.1";
            clientMode = true;

            startGrid(1);

            fail("Exception has not been thrown.");
        }
        catch (IgniteCheckedException e) {
            StringWriter errors = new StringWriter();

            e.printStackTrace(new PrintWriter(errors));

            String stackTrace = errors.toString();

            if (!stackTrace.contains("Local node and remote node have different version numbers"))
                throw e;
        }
    }
}
