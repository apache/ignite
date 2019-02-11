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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.GridSqlUsedColumnsInfo;
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
    private static final AtomicReference<GridSqlUsedColumnsInfo> usedCols = new AtomicReference<>();

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder().setShared(true);

    /** Client node name. */
    private static final String CLI_NAME = "cli";

    /** Rows. */
    private static int ROWS = 10;

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
     * Test single table.
     */
    @Test
    public void testSingleTable() {
        List<List<?>> res;

        sql("CREATE TABLE test (id LONG PRIMARY KEY, valStr VARCHAR, valLong LONG)");

        for (int i = 0; i < ROWS; ++i)
            sql("INSERT INTO test VALUES (?, ?, ?)", i, "val_" + i, i);

        // hidden fields, only key
        res = sql("SELECT id FROM test AS d0 WHERE _key < 5");

        assertEquals(5, res.size());
        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0", false));

        // only key
        res = sql("SELECT id FROM test AS d0 WHERE id < 5");

        assertEquals(5, res.size());
        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0",  false));

        // scan and wildcard
        res = sql("SELECT * FROM test AS d0");

        assertEquals((long)ROWS, res.size());
        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0", true));

        // Single-partition query (use original query)
        res = sql("SELECT valStr FROM test AS d0 WHERE _key = 5");

        assertEquals(1, res.size());
        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0", true));

        // simple
        res = sql("SELECT valStr FROM test AS d0 WHERE id > 4 AND valLong < 6");

        assertEquals("val_5", res.get(0).get(0));
        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0", true));

        // only value
        res = sql("SELECT valStr FROM test AS d0 WHERE valLong < 5");

        assertEquals(5, res.size());
        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0", true));

        // order by
        res = sql("SELECT valStr FROM test AS d0 WHERE valLong < 5 ORDER BY id");

        assertEquals(5, res.size());
        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0", true));

        res = sql("SELECT valStr FROM test AS d0 WHERE valLong < 5 ORDER BY valStr");

        assertEquals(5, res.size());
        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0", true));

        // GROUP BY / aggregates
        res = sql("SELECT SUM(valLong) FROM test AS d0 WHERE valLong < 5 GROUP BY id");
        assertEquals(5, res.size());
        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0", true));

        // GROUP BY / having
        res = sql("SELECT id FROM test AS d0 WHERE id < 5 GROUP BY id HAVING SUM(valLong) < 5");
        assertEquals(5, res.size());
        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0", true));
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

        for (long compId = 0; compId < ROWS; ++compId) {
            sql("INSERT INTO company VALUES (?, ?)", compId, "company_" + compId);

            for (long persCnt = 0; persCnt < 10; ++persCnt, ++persId)
                sql("INSERT INTO person VALUES (?, ?, ?)", persId, compId, "person_" + compId + "_" + persId);
        }

        res = sql(
            "SELECT comp.name AS compName, pers.name AS persName FROM company AS comp " +
                "LEFT JOIN person AS pers ON comp.id = pers.compId " +
                "WHERE comp.name='company_1'");

        assertEquals(res.size(), 10);
        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("pers", true),
            new UsedTableColumns("comp", true));
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
        for (long compId = 0; compId < ROWS; ++compId) {
            sql("INSERT INTO company VALUES (?, ?)", compId, "company_" + compId);

            for (long persCnt = 0; persCnt < 10; ++persCnt, ++persId)
                sql("INSERT INTO person VALUES (?, ?, ?)", persId, compId, "person_" + compId + "_" + persId);
        }

        res = sql(
            "SELECT comp.id FROM company AS comp " +
                "WHERE comp.id IN (SELECT MAX(COUNT(pers.id)) FROM person AS pers WHERE pers.compId = comp.id)");

        assertEquals(res.size(), 1);
        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("pers", false),
            new UsedTableColumns("comp", false));
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

        for (long i = 0; i < ROWS; ++i) {
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

        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0", true),
            new UsedTableColumns("d1", true),
            new UsedTableColumns("d2", true));

        assertEquals(res.size(), 10);
    }


    /**
     * Test compare last row on the page.
     */
    @Test
    public void testManyPagesInlineKey() {
        List<List<?>> res;

        sql("CREATE TABLE test (id LONG PRIMARY KEY, " +
                "val0 VARCHAR, val1 VARCHAR)");

        sql("CREATE INDEX test_val0 ON test (val0)");
        sql("CREATE INDEX test_val1 ON test (val1) INLINE_SIZE 0");

        for (long i = 0; i < 10000; ++i) {
            sql("INSERT INTO test  VALUES (?, ?, ?)",
                i, "val0", "val1");
        }

        res = sql(
            "SELECT val0 FROM test AS d0 WHERE val0 = 'val0'");

        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0", true));

        assertEquals(10000, res.size());

        res = sql(
            "SELECT val1 FROM test AS d0 WHERE val1 = 'val1'");

        assertUsedColumns(usedCols.get(),
            new UsedTableColumns("d0", true));

        assertEquals(10000, res.size());
    }

    /**
     * @param actualColInfo Actual map with used columns info.
     * @param expected expected used columns.
     */
    protected void assertUsedColumns(GridSqlUsedColumnsInfo actualColInfo, UsedTableColumns... expected) {
        if (actualColInfo == null) {
            if (!F.isEmpty(expected))
                fail("Invalid extracted columns: [actual=null, expected=" + Arrays.toString(expected) + ']');
            else {
                Map<String, Boolean> actualMap  = actualColInfo.createValueUsedMap();

                for (UsedTableColumns exp : expected)
                    assertEquals(actualMap.get(exp.alias), (Boolean)exp.isValUsed);
            }
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
        usedCols.set(null);

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

                    usedCols.set(req.usedColumns());
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }

    /**
     *
     */
    private static class UsedTableColumns {
        /** */
        @GridToStringInclude
        String alias;

        /** */
        @GridToStringInclude
        boolean isValUsed;

        /** */
        UsedTableColumns(String alias, boolean isValUsed) {
            this.alias = alias.toUpperCase();
            this.isValUsed = isValUsed;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            UsedTableColumns columns = (UsedTableColumns)o;

            return isValUsed == columns.isValUsed &&
                alias.length() < columns.alias.length()
                ? columns.alias.startsWith(alias)
                : alias.startsWith(columns.alias);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = Objects.hash(alias.substring(0, 2), isValUsed);

            return result;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(UsedTableColumns.class, this);
        }
    }
}