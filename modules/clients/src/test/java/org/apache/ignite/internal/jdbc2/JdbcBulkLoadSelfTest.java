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

package org.apache.ignite.internal.jdbc2;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/** COPY command test for the regular JDBC driver. */
public class JdbcBulkLoadSelfTest extends GridCommonAbstractTest {
    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX +
        "cache=default@modules/clients/src/test/config/jdbc-config.xml";

    /** Connection. */
    protected Connection conn;

    /** The logger. */
    protected transient IgniteLogger log;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return getConfiguration0(gridName);
    }

    /**
     * @param gridName Grid name.
     * @return Grid configuration used for starting the grid.
     * @throws Exception If failed.
     */
    private IgniteConfiguration getConfiguration0(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(
            Integer.class, Person.class
        );

        cfg.setCacheConfiguration(cache);
        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);
        ipFinder.setAddresses(Collections.singleton("127.0.0.1:47500..47501"));

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /**
     * Establishes the JDBC connection.
     *
     * @return Connection to use for the test.
     * @throws Exception if failed.
     */
    private Connection createConnection() throws Exception {
        Properties props = new Properties();

        return DriverManager.getConnection(BASE_URL, props);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.closeQuiet(conn);

        ignite(0).cache(DEFAULT_CACHE_NAME).clear();

        super.afterTest();
    }

    /**
     * This is more a placeholder for implementation of IGNITE-7553.
     *
     * @throws Exception if failed.
     */
    public void testBulkLoadThrows() throws Exception {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                conn = createConnection();

                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate("copy from 'dummy.csv' into Person" +
                        " (_key, id, firstName, lastName) format csv");

                    return null;
                }
            }
        }, SQLException.class, "COPY command is currently supported only in thin JDBC driver.");
    }

    /**
     * A test class for creating a query entity.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Person implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** First name. */
        @QuerySqlField(index = false)
        private final String firstName;

        /** Last name. */
        @QuerySqlField(index = false)
        private final String lastName;

        /** Age. */
        @QuerySqlField
        private final int age;

        /**
         * @param id ID.
         * @param firstName First name
         * @param lastName Last name
         * @param age Age.
         */
        private Person(int id, String firstName, String lastName, int age) {
            assert !F.isEmpty(firstName);
            assert !F.isEmpty(lastName);
            assert age > 0;

            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }
    }
}
