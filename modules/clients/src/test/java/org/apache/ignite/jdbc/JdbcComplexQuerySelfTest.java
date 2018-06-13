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

package org.apache.ignite.jdbc;

import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for complex queries (joins, etc.).
 */
public class JdbcComplexQuerySelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL. */
    private static final String URL = "jdbc:ignite://127.0.0.1/pers";

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration());

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setAtomicityMode(TRANSACTIONAL);

        return cache;
    }



    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty("h2.maxMemoryRows", Integer.toString(10));
        System.setProperty("h2.mvStore", "false");

        startGrids(1);

        IgniteCache<String, Organization> orgCache = jcache(grid(0), cacheConfiguration(), "org",
            String.class, Organization.class);

        assert orgCache != null;

        orgCache.put("o1", new Organization(1, "A"));
        orgCache.put("o2", new Organization(2, "B"));

        IgniteCache<AffinityKey, Person> personCache = jcache(grid(0), cacheConfiguration(), "pers",
            AffinityKey.class, Person.class);

        assert personCache != null;

        personCache.put(new AffinityKey<>("p1", "o1"), new Person(1, "John White", 25, 1));
        personCache.put(new AffinityKey<>("p2", "o1"), new Person(2, "Joe Black", 35, 1));
        personCache.put(new AffinityKey<>("p3", "o2"), new Person(3, "Mike Green", 40, 2));

        personCache.put(new AffinityKey<>("p4", "o2"), new Person(4, "Mike Green", 40, 2));
        personCache.put(new AffinityKey<>("p5", "o2"), new Person(5, "Mike Green", 40, 2));
        personCache.put(new AffinityKey<>("p6", "o2"), new Person(6, "Mike Green", 40, 2));

        personCache.put(new AffinityKey<>("p7", "o2"), new Person(7, "Mike Green", 40, 2));
        personCache.put(new AffinityKey<>("p8", "o2"), new Person(8, "Mike Green", 40, 2));
        personCache.put(new AffinityKey<>("p9", "o2"), new Person(9, "Mike Green", 40, 2));

        personCache.put(new AffinityKey<>("p10", "o2"), new Person(10, "Mike Green", 40, 2));
        personCache.put(new AffinityKey<>("p11", "o2"), new Person(11, "Mike Green", 40, 2));
        personCache.put(new AffinityKey<>("p12", "o2"), new Person(12, "Mike Green", 40, 2));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stmt = DriverManager.getConnection(URL).createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null) {
            stmt.getConnection().close();
            stmt.close();

            assert stmt.isClosed();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoin() throws Exception {
        ResultSet rs = stmt.executeQuery(
            "select p.id, p.name, o.name as orgName from \"pers\".Person p, \"org\".Organization o where p.orgId = o.id");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 1) {
                assert "John White".equals(rs.getString("name"));
                assert "A".equals(rs.getString("orgName"));
            }
            else if (id == 2) {
                assert "Joe Black".equals(rs.getString("name"));
                assert "A".equals(rs.getString("orgName"));
            }
            else if (id == 3) {
                assert "Mike Green".equals(rs.getString("name"));
                assert "B".equals(rs.getString("orgName"));
            }
            else
                assert false : "Wrong ID: " + id;

            cnt++;
        }

        assert cnt == 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinWithoutAlias() throws Exception {
        ResultSet rs0 = stmt.executeQuery("SELECT COUNT(*) FROM \"pers\".Person");

        rs0.next();

        System.out.println(">>> " + rs0.getLong(1));

        ResultSet rs = stmt.executeQuery(
            "select * from \"pers\".Person p");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {

            cnt++;
        }

        assert cnt == 12;
    }

    /**
     * @throws Exception If failed.
     */
    public void testIn() throws Exception {
        ResultSet rs = stmt.executeQuery("select name from \"pers\".Person where age in (25, 35)");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            assert "John White".equals(rs.getString("name")) ||
                "Joe Black".equals(rs.getString("name"));

            cnt++;
        }

        assert cnt == 2;
    }

    /**
     * @throws Exception If failed.
     */
    public void testBetween() throws Exception {
        ResultSet rs = stmt.executeQuery("select name from \"pers\".Person where age between 24 and 36");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            assert "John White".equals(rs.getString("name")) ||
                "Joe Black".equals(rs.getString("name"));

            cnt++;
        }

        assert cnt == 2;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCalculatedValue() throws Exception {
        ResultSet rs = stmt.executeQuery("select age * 2 from \"pers\".Person");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            assert rs.getInt(1) == 50 ||
                rs.getInt(1) == 70 ||
                rs.getInt(1) == 80;

            cnt++;
        }

        assert cnt == 3;
    }

    /**
     * Person.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Person implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** Name. */
        @QuerySqlField(index = false)
        private final String name;

        /** Age. */
        @QuerySqlField
        private final int age;

        /** Organization ID. */
        @QuerySqlField
        private final int orgId;

        /**
         * @param id ID.
         * @param name Name.
         * @param age Age.
         * @param orgId Organization ID.
         */
        private Person(int id, String name, int age, int orgId) {
            assert !F.isEmpty(name);
            assert age > 0;
            assert orgId > 0;

            this.id = id;
            this.name = name;
            this.age = age;
            this.orgId = orgId;
        }
    }

    /**
     * Organization.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Organization implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** Name. */
        @QuerySqlField(index = false)
        private final String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        private Organization(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}