/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * TODO: Add class description.
 */
public class SqlQueryRegressionsTest extends IgniteCompatibilityAbstractTest {
    /** Ignite version. */
    private static final String IGNITE_VERSION = "2.5.0";

    /** */
    private static final int OLD_JDBC_PORT = 10800;

    /** */
    private static final int NEW_JDBC_PORT = 10801;

    /** */
    public static final TcpDiscoveryIpFinder OLD_VER_FINDER = new TcpDiscoveryVmIpFinder(true) {{
        setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
    }};

    /** */
    public static final TcpDiscoveryVmIpFinder NEW_VER_FINDER = new TcpDiscoveryVmIpFinder(true) {{
        setAddresses(Collections.singleton("127.0.0.1:47510..47519"));
    }};

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlPerformanceRegressions() throws Exception {
        IgniteEx oldNode = null;
        IgniteEx newNode = null;
        try {
            oldNode = startGrid(1, IGNITE_VERSION, new ConfigurationClosure(), new PostStartupClosure(true));

            newNode = (IgniteEx)IgnitionEx.start(prepareConfig(getConfiguration(), NEW_VER_FINDER, NEW_JDBC_PORT));

            try (Connection oldConn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:" + OLD_JDBC_PORT);
                Connection newConn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:" + NEW_JDBC_PORT)) {
                System.out.println("populateData(oldConn);");
                populateData(oldConn);
                System.out.println("populateData(newConn);");
                populateData(newConn);

            }


            IgniteCache newCache = newNode.getOrCreateCache("Test");

            System.out.println("newCache.get(1)=" + newCache.get(1));
            newCache.put(2, 2);
            System.out.println("newCache.get(1)=" + newCache.get(1));
            System.out.println("newCache.get(2)=" + newCache.get(2));
            //executeSql(oldNode, "SELECT *");
            doSleep(2000);
        }
        finally {
            IgniteProcessProxy.killAll();
            U.close(newNode, log);
        }
    }

    public void populateData(Connection oldConn) throws SQLException {
        try (Statement stmt = oldConn.createStatement()) {
            stmt.execute("CREATE TABLE person (id BIGINT PRIMARY KEY, name VARCHAR) " +
                "WITH \"cache_name=PERSON_CACHE\"");

            stmt.execute("INSERT INTO person VALUES (1, 'KOKO')");
            stmt.execute("INSERT INTO person VALUES (2, 'BEBE')");
        }
        try (PreparedStatement stmt = oldConn.prepareStatement("SELECT * FROM person")) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    System.out.println("RS=" + rs.getInt(1) + ", " + rs.getString(2));
                }
            }
        }

    }

    /**
     * Run SQL statement on specified node.
     *
     * @param node node to execute query.
     * @param stmt Statement to run.
     * @param args arguments of statements
     * @return Run result.
     */
    private static List<List<?>> executeSql(IgniteEx node, String stmt, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(stmt).setArgs(args), true).getAll();
    }

    /** {@inheritDoc} */
    @Override @NotNull protected Collection<Dependency> getDependencies(String igniteVer) {
        Collection<Dependency> dependencies = super.getDependencies(igniteVer);

        dependencies.add(new Dependency("indexing", "org.apache.ignite", "ignite-indexing", IGNITE_VERSION, false));

        // TODO add and exclude proper versions of h2
        dependencies.add(new Dependency("h2", "com.h2database", "h2", "1.4.195", false));

        return dependencies;
    }

    /** {@inheritDoc} */
    @Override protected Set<String> getExcluded(String ver, Collection<Dependency> dependencies) {
        Set<String> excluded = super.getExcluded(ver, dependencies);

        excluded.add("h2");

        return excluded;
    }

    /** */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {

        /** */
        boolean createTable;

        /**
         * @param createTable {@code true} In case table should be created
         */
        public PostStartupClosure(boolean createTable) {
            this.createTable = createTable;
        }

        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            IgniteEx igniteEx = (IgniteEx)ignite;

            if (createTable)
                initializeTable(igniteEx, "TABLE_NAME");

            IgniteCache oldCache = ignite.getOrCreateCache("Test");

            oldCache.put(1, 1);

            System.out.println("oldCache.get(1)=" + oldCache.get(1));


        }
    }

    /**
     * @param igniteEx Ignite instance.
     * @param tblName Table name.
     */
    @NotNull private static void initializeTable(IgniteEx igniteEx, String tblName) {
        executeSql(igniteEx, "CREATE TABLE " + tblName + " (id int, name varchar, age int, company varchar, city varchar, " +
            "primary key (id, name, city)) WITH \"affinity_key=name\"");

        executeSql(igniteEx, "CREATE INDEX ON " + tblName + "(city, age)");

        for (int i = 0; i < 1000; i++)
            executeSql(igniteEx, "INSERT INTO " + tblName + " (id, name, age, company, city) VALUES(?,'name',2,'company', 'city')", i);
    }

    /** */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            prepareConfig(cfg, OLD_VER_FINDER, OLD_JDBC_PORT);
        }
    }

    private static IgniteConfiguration prepareConfig(IgniteConfiguration cfg, TcpDiscoveryIpFinder ipFinder,
        int jdbcPort) {
        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(disco);

        ClientConnectorConfiguration clientCfg = new ClientConnectorConfiguration();
        clientCfg.setPort(jdbcPort);

        return cfg;
    }
}
