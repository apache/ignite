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

package org.apache.ignite.internal.jdbc2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.resource.GridResourceIoc;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;

/**
 * Test of cluster and JDBC driver with config that contains cache with POJO store and datasource bean.
 */
public class JdbcSpringSelfTest extends JdbcConnectionSelfTest {
    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected String configURL() {
        return "modules/clients/src/test/config/jdbc-config-cache-store.xml";
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(null); // In this test we are using default Marshaller.

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsWithSpringCtx(GRID_CNT, false, configURL());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testClientNodeId() throws Exception {
        IgniteEx client = (IgniteEx) startGridWithSpringCtx(getTestIgniteInstanceName(), true, configURL());

        UUID clientId = client.localNode().id();

        final String url = CFG_URL_PREFIX + "nodeId=" + clientId + '@' + configURL();

        GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Connection conn = DriverManager.getConnection(url)) {
                            return conn;
                        }
                    }
                },
                SQLException.class,
                "Failed to establish connection with node (is it a server node?): " + clientId
        );
    }

    /**
     * Special class to test Spring context injection.
     */
    private static class TestInjectTarget {
        /** */
        @SpringApplicationContextResource
        private Object appCtx;
    }

    /**
     * Test that we have valid Spring context and also could create beans from it.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testSpringBean() throws Exception {
        String url = CFG_URL_PREFIX + configURL();

        // Create connection.
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull(conn);

            TestInjectTarget target = new TestInjectTarget();

            IgniteKernal kernal = (IgniteKernal)((JdbcConnection)conn).ignite();

            // Inject Spring context to test object.
            kernal.context().resource().inject(target, GridResourceIoc.AnnotationSet.GENERIC);

            assertNotNull(target.appCtx);

            IgniteSpringHelper spring = IgniteComponentType.SPRING.create(false);

            // Load bean by name.
            DataSource ds = spring.loadBeanFromAppContext(target.appCtx, "dsTest");

            assertNotNull(ds);
        }
    }
}
