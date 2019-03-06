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

package org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.junit.Test;

/**
 * JDBC IP finder self test.
 */
public class TcpDiscoveryJdbcIpFinderSelfTest extends
        TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryJdbcIpFinder> {
    /** */
    private ComboPooledDataSource dataSrc;

    /** */
    private boolean initSchema = true;

    /**
     * Constructor.
     *
     * @throws Exception If failed.
     */
    public TcpDiscoveryJdbcIpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryJdbcIpFinder ipFinder() throws Exception {
        TcpDiscoveryJdbcIpFinder finder = new TcpDiscoveryJdbcIpFinder();

        assert finder.isShared() : "IP finder should be shared by default.";

        dataSrc = new ComboPooledDataSource();
        dataSrc.setDriverClass("org.h2.Driver");

        if (initSchema)
            dataSrc.setJdbcUrl("jdbc:h2:mem:./test");
        else {
            dataSrc.setJdbcUrl("jdbc:h2:mem:jdbc_ipfinder_not_initialized_schema");

            finder.setInitSchema(false);
        }

        finder.setDataSource(dataSrc);

        return finder;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInitSchemaFlag() throws Exception {
        initSchema = false;

        try {
            ipFinder().getRegisteredAddresses();

            fail("IP finder didn't throw expected exception.");
        }
        catch (IgniteSpiException e) {
            assertTrue(e.getMessage().contains("IP finder has not been properly initialized"));
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        initSchema = true;

        dataSrc.close();
    }
}
