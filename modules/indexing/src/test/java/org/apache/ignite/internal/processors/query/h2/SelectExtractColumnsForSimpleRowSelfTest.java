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

package org.apache.ignite.internal.processors.query.h2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.GridSqlUsedColumnInfo;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for join partition pruning.
 */
@SuppressWarnings("deprecation")
@RunWith(JUnit4.class)
public class SelectExtractColumnsForSimpleRowSelfTest extends AbstractIndexingCommonTest {
    /** Tracked map queries. */
    private static final AtomicReference<List<GridCacheSqlQuery>> qrys = new AtomicReference<>();

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder().setShared(true);

    /** Client node name. */
    private static final String CLI_NAME = "cli";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(getConfiguration("srv1"));
        startGrid(getConfiguration("srv2"));

        startGrid(getConfiguration(CLI_NAME).setClientMode(true));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Ignite cli = client();

        cli.destroyCaches(cli.cacheNames());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER))
            .setCommunicationSpi(new TrackingTcpCommunicationSpi())
            .setLocalHost("127.0.0.1");
    }

    /**
     * Test extract only key.
     */
    @Test
    public void testSelectKeyOnly() {
        List<List<?>> res;

        sql("CREATE TABLE test (id LONG PRIMARY KEY, valStr VARCHAR, valLong LONG)");

        for (int i = 0; i < 100; ++i)
            sql("INSERT INTO test VALUES (?, ?, ?)", i, "val_" + i, i);

        // scan
        res = sql("SELECT ID from test WHERE ID < 5");

        assertEquals(5, res.size());
    }

    /**
     * Test extract only value.
     */
    @Test
    public void testSelectValueOnly() {
        List<List<?>> res;

        sql("CREATE TABLE test (id LONG PRIMARY KEY, valStr VARCHAR, valLong LONG)");

        for (int i = 0; i < 100; ++i)
            sql("INSERT INTO test VALUES (?, ?, ?)", i, "val_" + i, i);

        // scan
        res = sql("SELECT valLong from test WHERE valLong < 5");

        assertEquals(5, res.size());
    }

    /**
     * Test single table.
     */
    @Test
    public void testSingleTable() {
        List<List<?>> res;

        sql("CREATE TABLE test (id LONG PRIMARY KEY, valStr VARCHAR, valLong LONG)");

        for (int i = 0; i < 100; ++i)
            sql("INSERT INTO test VALUES (?, ?, ?)", i, "val_" + i, i);

        // scan and wildcard
        res = sql("SELECT * FROM test AS d0");

        assertEquals(100L, res.size());
        assertUsedColumns(qrys.get().get(0).usedColumns(),
            new UsedTableColumns("d0", true, true, 2, 3, 4)
        );

        // hidden fields, only key
        res = sql("SELECT id FROM test AS d0 WHERE _key < 5");

        assertEquals(5, res.size());
        assertUsedColumns(qrys.get().get(0).usedColumns(),
            new UsedTableColumns("d0", true, false, 0, 2)
        );

        // Doesn't work for single-partition query
        res = sql("SELECT valStr FROM test AS d0 WHERE _key = 5");

        assertEquals(1, res.size());
        assertUsedColumns(qrys.get().get(0).usedColumns());

        // simple
        res = sql("SELECT valStr FROM test AS d0 WHERE id > 4 AND valLong < 6");

        assertEquals("val_5", res.get(0).get(0));
        assertUsedColumns(qrys.get().get(0).usedColumns(),
            new UsedTableColumns("d0", true, true, 2, 3, 4)
            );

        // only value
        res = sql("SELECT valStr FROM test AS d0 WHERE valLong < 5");

        assertEquals(5, res.size());
        assertUsedColumns(qrys.get().get(0).usedColumns(),
            new UsedTableColumns("d0", false, true, 3, 4)
        );

        // order by
        res = sql("SELECT valStr FROM test AS d0 WHERE valLong < 5 ORDER BY id");

        assertEquals(5, res.size());
        assertUsedColumns(qrys.get().get(0).usedColumns(),
            new UsedTableColumns("d0", true, true, 2, 3, 4)
        );

        res = sql("SELECT valStr FROM test AS d0 WHERE valLong < 5 ORDER BY valStr");

        assertEquals(5, res.size());
        assertUsedColumns(qrys.get().get(0).usedColumns(),
            new UsedTableColumns("d0", false, true, 3, 4)
        );

        // GROUP BY / aggregates
        res = sql("SELECT SUM(valLong) FROM test AS d0 WHERE valLong < 5 GROUP BY id");
        assertEquals(5, res.size());
        assertUsedColumns(qrys.get().get(0).usedColumns(),
            new UsedTableColumns("d0", true, true, 2, 4)
        );

        // GROUP BY / having
        res = sql("SELECT id FROM test AS d0 WHERE id < 5 GROUP BY id HAVING SUM(valLong) < 5");
        assertEquals(5, res.size());
        assertUsedColumns(qrys.get().get(0).usedColumns(),
            new UsedTableColumns("d0", true, true, 2, 4)
        );
    }

    /**
     * Test single table.
     */
    @Test
    public void testSimpleJoin() {
        List<List<?>> res;

        sql("CREATE TABLE person (id LONG, compId LONG, name VARCHAR, " +
                "PRIMARY KEY (id, compId)) " +
                "WITH \"AFFINITY_KEY=compId\"");

        sql("CREATE TABLE company (id LONG PRIMARY KEY, name VARCHAR)");
        sql("CREATE INDEX idx_company_name ON company (name)");

        long persId = 0;

        for (long compId = 0; compId < 10; ++compId) {
            sql("INSERT INTO company VALUES (?, ?)", compId, "company_" + compId);

            for (long persCnt = 0; persCnt < 10; ++persCnt, ++persId)
                sql("INSERT INTO person VALUES (?, ?, ?)", persId, compId, "person_" + compId + "_" + persId);
        }

        res = sql(
            "SELECT comp.name AS compName, pers.name AS persName FROM company AS comp " +
                "LEFT JOIN person AS pers ON comp.id = pers.compId " +
                "WHERE comp.name='company_1'");

        assertEquals(res.size(), 10);
        assertEquals(1, qrys.get().size());
        assertUsedColumns(qrys.get().get(0).usedColumns(),
            new UsedTableColumns("pers", true, true, 3, 4),
            new UsedTableColumns("comp", true, true, 2, 3)
        );
    }

    /**
     * Test subquery.
     */
    @Test
    public void testSubquery() {
        List<List<?>> res;

        sql(
            "CREATE TABLE person (id LONG, compId LONG, name VARCHAR, " +
                "PRIMARY KEY (id, compId)) " +
                "WITH \"AFFINITY_KEY=compId\"");
        sql("CREATE TABLE company (id LONG PRIMARY KEY, name VARCHAR)");

        long persId = 0;
        for (long compId = 0; compId < 10; ++compId) {
            sql("INSERT INTO company VALUES (?, ?)", compId, "company_" + compId);

            for (long persCnt = 0; persCnt < 10; ++persCnt, ++persId)
                sql("INSERT INTO person VALUES (?, ?, ?)", persId, compId, "person_" + compId + "_" + persId);
        }

        res = sql(
            "SELECT comp.id FROM company AS comp " +
                "WHERE comp.id IN (SELECT MAX(COUNT(pers.id)) FROM person AS pers WHERE pers.compId = comp.id)");

        assertEquals(res.size(), 1);
        assertEquals(1, qrys.get().size());
        assertUsedColumns(qrys.get().get(0).usedColumns(),
            new UsedTableColumns("pers", true, false, 2, 3),
            new UsedTableColumns("comp", true, false, 2)
        );
    }

    /**
     * Test subquery.
     */
    @Test
    public void testSeveralSubqueries() {
        List<List<?>> res;

        sql(
            "CREATE TABLE test (id LONG PRIMARY KEY, " +
                "val0 VARCHAR, val1 VARCHAR, val2 VARCHAR, val3 VARCHAR, val4 VARCHAR, val5 VARCHAR, val6 VARCHAR)");

        for (long i = 0; i < 10; ++i) {
            sql("INSERT INTO test VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                i, "0_" + i, "1_" + i, "2_" + i, "3_" + i, "4_" + i, "5_" + i, "6_" + i);
        }

        res = sql(
            "SELECT t0.val0, t1.val2, t2.val4 FROM " +
                "(SELECT id AS id, val0, val1 FROM test AS d0 where id > -1) AS t0 " +
                "LEFT JOIN " +
                "(SELECT id AS id, val2, val3 FROM test AS d1) AS t1 ON t0.id = t1.id " +
                "LEFT JOIN " +
                "(SELECT id AS id, val4, val5 FROM test AS d2) AS t2 ON t1.id = t2.id");

        assertEquals(1, qrys.get().size());
        assertUsedColumns(qrys.get().get(0).usedColumns(),
            new UsedTableColumns("d0", true, true, 2, 3, 4),
            new UsedTableColumns("d1", true, true, 2, 5, 6),
            new UsedTableColumns("d2", true, true, 2, 7, 8)
            );

        assertEquals(res.size(), 10);
    }

    /**
     * @param actualMap Actual map with used columns info.
     * @param expected expected used columns.
     */
    protected void assertUsedColumns(Map<String, GridSqlUsedColumnInfo> actualMap, UsedTableColumns... expected) {
        if (actualMap == null) {
            if (!F.isEmpty(expected))
                fail("Invalid extracted columns: [actual=null, expected=" + Arrays.toString(expected) + ']');
        }
        else {
            Set<UsedTableColumns> actualSet = actualMap.entrySet().stream()
                .map(UsedTableColumns::new)
                .collect(Collectors.toSet());

            Set<UsedTableColumns> expectedSet = Arrays.stream(expected).collect(Collectors.toSet());

            assertEquals(expectedSet, actualSet);
        }
    }

    /**
     * @return Client node.
     */
    private IgniteEx client() {
        return grid(CLI_NAME);
    }

    /**
     * Execute prepared SQL fields query.
     *
     * @param qry Query.
     * @param args Query parameters.
     * @return Result.
     */
    private List<List<?>> sql(String qry, Object... args) {
        return executeSqlFieldsQuery(new SqlFieldsQuery(qry).setArgs(args));
    }

    /**
     * Execute prepared SQL fields query.
     *
     * @param qry Query.
     * @return Result.
     */
    private List<List<?>> executeSqlFieldsQuery(SqlFieldsQuery qry) {
        qrys.set(null);

        return client().context().query().querySqlFields(qry, false).getAll();
    }

    /**
     * TCP communication SPI which will track outgoing query requests.
     */
    private static class TrackingTcpCommunicationSpi extends TcpCommunicationSpi {
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
            if (msg instanceof GridIoMessage) {
                GridIoMessage msg0 = (GridIoMessage)msg;

                if (msg0.message() instanceof GridH2QueryRequest) {
                    GridH2QueryRequest req = (GridH2QueryRequest)msg0.message();

                    qrys.set(req.queries());
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }

    /** */
    private static class UsedTableColumns {
        /** */
        @GridToStringInclude
        String alias;

        /** */
        @GridToStringInclude
        int [] usedCols;

        /** */
        @GridToStringInclude
        boolean isKeyUsed;

        /** */
        @GridToStringInclude
        boolean isValUsed;

        /** */
        UsedTableColumns(Map.Entry<String, GridSqlUsedColumnInfo> entry) {
            alias = entry.getKey().toUpperCase();
            usedCols = entry.getValue().columns();
            isKeyUsed = entry.getValue().isKeyUsed();
            isValUsed = entry.getValue().isValueUsed();
        }

        /** */
        UsedTableColumns(String alias, boolean isKeyUsed, boolean isValUsed, int...usedCols) {
            this.alias = alias.toUpperCase();
            this.isKeyUsed = isKeyUsed;
            this.isValUsed = isValUsed;
            this.usedCols = usedCols;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            UsedTableColumns columns = (UsedTableColumns)o;

            return isKeyUsed == columns.isKeyUsed &&
                isValUsed == columns.isValUsed &&
                alias.length() < columns.alias.length()
                    ? columns.alias.startsWith(alias)
                    : alias.startsWith(columns.alias) &&
                Arrays.equals(usedCols, columns.usedCols);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = Objects.hash(alias.substring(0, 2), isKeyUsed, isValUsed);

            result = 31 * result + Arrays.hashCode(usedCols);

            return result;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(UsedTableColumns.class, this);
        }
    }
}
