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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.jdbc.thin.JdbcThinAbstractSelfTest;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Data streaming test.
 */
public class JdbcStreamingSelfTest extends JdbcThinAbstractSelfTest {
    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX +
        "cache=default@modules/clients/src/test/config/jdbc-config.xml";

    /** Streaming URL. */
    private static final String STREAMING_URL = CFG_URL_PREFIX +
        "cache=person@modules/clients/src/test/config/jdbc-config.xml";

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
            Integer.class, Integer.class
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
        super.beforeTestsStarted();

        startGrids(2);

        try (Connection c = createOrdinaryConnection()) {
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE PUBLIC.Person(\"id\" int primary key, \"name\" varchar) WITH " +
                    "\"cache_name=person,value_type=Person\"");
            }
        }

        U.sleep(1000);
    }

    /**
     * @return Connection without streaming initially turned on.
     * @throws SQLException if failed.
     */
    protected Connection createOrdinaryConnection() throws SQLException {
        Connection res = DriverManager.getConnection(BASE_URL, new Properties());

        res.setSchema(QueryUtils.DFLT_SCHEMA);

        return res;
    }

    /**
     * @param allowOverwrite Allow overwriting of existing keys.
     * @return Connection to use for the test.
     * @throws Exception if failed.
     */
    protected Connection createStreamedConnection(boolean allowOverwrite) throws Exception {
        return createStreamedConnection(allowOverwrite, 500);
    }

    /**
     * @param allowOverwrite Allow overwriting of existing keys.
     * @param flushTimeout Stream flush timeout.
     * @return Connection to use for the test.
     * @throws Exception if failed.
     */
    protected Connection createStreamedConnection(boolean allowOverwrite, long flushTimeout) throws Exception {
        Properties props = new Properties();

        props.setProperty(IgniteJdbcDriver.PROP_STREAMING, "true");
        props.setProperty(IgniteJdbcDriver.PROP_STREAMING_FLUSH_FREQ, String.valueOf(flushTimeout));

        if (allowOverwrite)
            props.setProperty(IgniteJdbcDriver.PROP_STREAMING_ALLOW_OVERWRITE, "true");

        Connection res = DriverManager.getConnection(STREAMING_URL, props);

        res.setSchema(QueryUtils.DFLT_SCHEMA);

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cache().clear();

        super.afterTest();
    }

    /**
     * @throws Exception if failed.
     */
    public void testStreamedInsert() throws Exception {
        for (int i = 10; i <= 100; i += 10)
            put(i, nameForId(i * 100));

        try (Connection conn = createStreamedConnection(false)) {
            try (PreparedStatement stmt = conn.prepareStatement("insert into PUBLIC.Person(\"id\", \"name\") " +
                "values (?, ?)")) {
                for (int i = 1; i <= 100; i++) {
                    stmt.setInt(1, i);
                    stmt.setString(2, nameForId(i));

                    stmt.executeUpdate();
                }
            }
        }

        U.sleep(500);

        // Now let's check it's all there.
        for (int i = 1; i <= 100; i++) {
            if (i % 10 != 0)
                assertEquals(nameForId(i), nameForIdInCache(i));
            else // All that divides by 10 evenly should point to numbers 100 times greater - see above
                assertEquals(nameForId(i * 100), nameForIdInCache(i));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testStreamedInsertWithoutColumnsList() throws Exception {
        for (int i = 10; i <= 100; i += 10)
            put(i, nameForId(i * 100));

        try (Connection conn = createStreamedConnection(false)) {
            try (PreparedStatement stmt = conn.prepareStatement("insert into PUBLIC.Person(\"id\", \"name\") " +
                "values (?, ?)")) {
                for (int i = 1; i <= 100; i++) {
                    stmt.setInt(1, i);
                    stmt.setString(2, nameForId(i));

                    stmt.executeUpdate();
                }
            }
        }

        U.sleep(500);

        // Now let's check it's all there.
        for (int i = 1; i <= 100; i++) {
            if (i % 10 != 0)
                assertEquals(nameForId(i), nameForIdInCache(i));
            else // All that divides by 10 evenly should point to numbers 100 times greater - see above
                assertEquals(nameForId(i * 100), nameForIdInCache(i));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testStreamedInsertWithOverwritesAllowed() throws Exception {
        for (int i = 10; i <= 100; i += 10)
            put(i, nameForId(i * 100));

        try (Connection conn = createStreamedConnection(true)) {
            try (PreparedStatement stmt = conn.prepareStatement("insert into PUBLIC.Person(\"id\", \"name\") " +
                "values (?, ?)")) {
                for (int i = 1; i <= 100; i++) {
                    stmt.setInt(1, i);
                    stmt.setString(2, nameForId(i));

                    stmt.executeUpdate();
                }
            }
        }

        U.sleep(500);

        // Now let's check it's all there.
        // i should point to i at all times as we've turned overwrites on above.
        for (int i = 1; i <= 100; i++)
            assertEquals(nameForId(i), nameForIdInCache(i));
    }

    /** */
    public void testOnlyInsertsAllowed() {
        assertStatementForbidden("CREATE TABLE PUBLIC.X (x int primary key, y int)");

        assertStatementForbidden("CREATE INDEX idx_1 ON Person(name)");

        assertStatementForbidden("SELECT * from Person");

        assertStatementForbidden("insert into PUBLIC.Person(\"id\", \"name\") " +
            "(select \"id\" + 1, CONCAT(\"name\", '1') from Person)");

        assertStatementForbidden("DELETE from Person");

        assertStatementForbidden("UPDATE Person SET \"name\" = 'name0'");

        assertStatementForbidden("alter table Person add column y int");
    }

    /**
     * @param sql Statement to check.
     */
    @SuppressWarnings("ThrowableNotThrown")
    protected void assertStatementForbidden(String sql) {
        GridTestUtils.assertThrows(null, new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                try (Connection c = createStreamedConnection(false)) {
                    try (PreparedStatement s = c.prepareStatement(sql)) {
                        s.execute();
                    }
                }

                return null;
            }
        }, SQLException.class,"Streaming mode supports only INSERT commands without subqueries.");
    }

    /**
     * @return Person cache.
     */
    protected IgniteCache<Integer, Object> cache() {
        return grid(0).cache("person");
    }

    /**
     * @param id id of person to put.
     * @param name name of person to put.
     */
    protected void put(int id, String name) {
        BinaryObjectBuilder bldr = grid(0).binary().builder("Person");

        bldr.setField("name", name);

        cache().put(id, bldr.build());
    }

    /**
     * @param id Person id.
     * @return Default name for person w/given id.
     */
    protected String nameForId(int id) {
        return "Person" + id;
    }

    /**
     * @param id person id.
     * @return Name for person with given id currently stored in cache.
     */
    protected String nameForIdInCache(int id) {
        Object o = cache().withKeepBinary().get(id);

        assertTrue(String.valueOf(o), o instanceof BinaryObject);

        return ((BinaryObject)o).field("name");
    }
}
