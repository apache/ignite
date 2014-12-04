/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.jdbc;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.sql.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests for complex queries (joins, etc.).
 */
public class GridJdbcComplexQuerySelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** URL. */
    private static final String URL = "jdbc:gridgain://127.0.0.1/";

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setDistributionMode(NEAR_PARTITIONED);
        cache.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cache);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setRestEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(3);

        GridCache<String, Organization> orgCache = grid(0).cache(null);

        assert orgCache != null;

        orgCache.put("o1", new Organization(1, "A"));
        orgCache.put("o2", new Organization(2, "B"));

        GridCache<GridCacheAffinityKey<String>, Person> personCache = grid(0).cache(null);

        assert personCache != null;

        personCache.put(new GridCacheAffinityKey<>("p1", "o1"), new Person(1, "John White", 25, 1));
        personCache.put(new GridCacheAffinityKey<>("p2", "o1"), new Person(2, "Joe Black", 35, 1));
        personCache.put(new GridCacheAffinityKey<>("p3", "o2"), new Person(3, "Mike Green", 40, 2));

        Class.forName("org.gridgain.jdbc.GridJdbcDriver");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
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
            "select p.id, p.name, o.name as orgName from Person p, Organization o where p.orgId = o.id");

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
        ResultSet rs = stmt.executeQuery(
            "select p.id, p.name, o.name from Person p, Organization o where p.orgId = o.id");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt(1);

            if (id == 1) {
                assert "John White".equals(rs.getString("name"));
                assert "John White".equals(rs.getString(2));
                assert "A".equals(rs.getString(3));
            }
            else if (id == 2) {
                assert "Joe Black".equals(rs.getString("name"));
                assert "Joe Black".equals(rs.getString(2));
                assert "A".equals(rs.getString(3));
            }
            else if (id == 3) {
                assert "Mike Green".equals(rs.getString("name"));
                assert "Mike Green".equals(rs.getString(2));
                assert "B".equals(rs.getString(3));
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
    public void testIn() throws Exception {
        ResultSet rs = stmt.executeQuery("select name from Person where age in (25, 35)");

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
        ResultSet rs = stmt.executeQuery("select name from Person where age between 24 and 36");

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
        ResultSet rs = stmt.executeQuery("select age * 2 from Person");

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
        @GridCacheQuerySqlField
        private final int id;

        /** Name. */
        @GridCacheQuerySqlField(index = false)
        private final String name;

        /** Age. */
        @GridCacheQuerySqlField
        private final int age;

        /** Organization ID. */
        @GridCacheQuerySqlField
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
        @GridCacheQuerySqlField
        private final int id;

        /** Name. */
        @GridCacheQuerySqlField(index = false)
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
