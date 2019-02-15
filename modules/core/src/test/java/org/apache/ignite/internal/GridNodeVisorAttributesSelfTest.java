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

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Ensures that system properties required by Visor are always passed to node attributes.
 */
@RunWith(JUnit4.class)
public class GridNodeVisorAttributesSelfTest extends GridCommonAbstractTest {
    /** System properties required by Visor. */
    private static final String[] SYSTEM_PROPS = new String[] {
        "java.version",
        "java.vm.name",
        "os.arch",
        "os.name",
        "os.version"
    };

    /** Ignite-specific properties required by Visor. */
    private static final String[] IGNITE_PROPS = new String[] {
        "org.apache.ignite.jvm.pid"
    };

    /** System and environment properties to include. */
    private String[] inclProps;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeProperties(inclProps);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Start grid node and ensure that Visor-related node attributes are set properly.
     *
     * @throws Exception If grid start failed.
     */
    private void startGridAndCheck() throws Exception {
        Ignite g = startGrid();

        Map<String, Object> attrs = g.cluster().localNode().attributes();

        for (String prop : SYSTEM_PROPS) {
            assert attrs.containsKey(prop);

            assertEquals(System.getProperty(prop), attrs.get(prop));
        }

        for (String prop : IGNITE_PROPS)
            assert attrs.containsKey(prop);
    }

    /**
     * Test with 'includeProperties' configuration parameter set to {@code null}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncludeNull() throws Exception {
        inclProps = null;

        startGridAndCheck();
    }

    /**
     * Test with 'includeProperties' configuration parameter set to empty array.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    @Test
    public void testIncludeEmpty() throws Exception {
        inclProps = new String[] {};

        startGridAndCheck();
    }

    /**
     * Test with 'includeProperties' configuration parameter set to array with some values.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncludeNonEmpty() throws Exception {
        inclProps = new String[] {"prop1", "prop2"};

        startGridAndCheck();
    }
}
