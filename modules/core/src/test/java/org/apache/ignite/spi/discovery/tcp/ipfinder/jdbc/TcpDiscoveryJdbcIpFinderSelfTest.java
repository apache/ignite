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

package org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;

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