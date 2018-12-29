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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Tests for join partition pruning.
 */
@SuppressWarnings("deprecation")
@RunWith(JUnit4.class)
public class JoinPartitionPruningSelfTest extends GridCommonAbstractTest {
    /** Number of intercepted requests. */
    private static final AtomicInteger INTERCEPTED_REQS = new AtomicInteger();

    /** Parititions tracked during query execution. */
    private static final ConcurrentSkipListSet<Integer> INTERCEPTED_PARTS = new ConcurrentSkipListSet<>();

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder().setShared(true);

    /** Client node name. */
    private static final String CLI_NAME = "cli";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(getConfiguration("srv1"));
        startGrid(getConfiguration("srv2"));
        startGrid(getConfiguration("srv3"));

        startGrid(getConfiguration(CLI_NAME).setClientMode(true));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        clearIoState();

        Ignite cli = client();

        cli.destroyCaches(cli.cacheNames());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration res = super.getConfiguration(name);

        res.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));
        res.setCommunicationSpi(new TrackingTcpCommunicationSpi());

        res.setLocalHost("127.0.0.1");

        return res;
    }

    /**
     * Test simple join.
     */
    @Test
    public void testSimpleJoin() {
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2");

        createPartitionedTable("t2",
            pkColumn("k1"),
            affinityColumn("ak2"),
            "v3");

        execute("INSERT INTO t1 VALUES ('1', '1')");
        execute("INSERT INTO t2 VALUES ('1', '1', '1')");

        execute("INSERT INTO t1 VALUES ('2', '2')");
        execute("INSERT INTO t2 VALUES ('2', '2', '2')");

        execute("INSERT INTO t1 VALUES ('3', '3')");
        execute("INSERT INTO t2 VALUES ('3', '3', '3')");

        execute("INSERT INTO t1 VALUES ('4', '4')");
        execute("INSERT INTO t2 VALUES ('4', '4', '4')");

        execute("INSERT INTO t1 VALUES ('5', '5')");
        execute("INSERT INTO t2 VALUES ('5', '5', '5')");

        // Key (not alias).
        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ?",
            (res) -> {
                assertPartitions(
                    parititon("t1", "1")
                );
                assertEquals(1, res.size());
                assertEquals("1", res.get(0).get(0));
            },
            "1"
        );

        // Key (alias).
        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1._KEY = ?",
            (res) -> {
                assertPartitions(
                    parititon("t1", "2")
                );
                assertEquals(1, res.size());
                assertEquals("2", res.get(0).get(0));
            },
            "2"
        );

        // Non-affinity key.
        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t2.k1 = ?",
            (res) -> {
                assertNoPartitions();
                assertEquals(1, res.size());
                assertEquals("3", res.get(0).get(0));
            },
            "3"
        );

        // Affinity key.
        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t2.ak2 = ?",
            (res) -> {
                assertPartitions(
                    parititon("t2", "4")
                );
                assertEquals(1, res.size());
                assertEquals("4", res.get(0).get(0));
            },
            "4"
        );

        // Complex key.
        BinaryObject key = client().binary().builder("t2_key").setField("k1", "5").setField("ak2", "5").build();

        List<List<?>> res = execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t2._KEY = ?", key);
        assertPartitions(
            parititon("t2", "5")
        );
        assertEquals(1, res.size());
        assertEquals("5", res.get(0).get(0));
    }

    /**
     * Test how partition ownership is transferred in various cases.
     */
    @Test
    public void testPartitionTransfer() {
        // First co-located table.
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2"
        );

        // Second co-located table.
        createPartitionedTable("t2",
            pkColumn("k1"),
            affinityColumn("ak2"),
            "v3"
        );

        // Third co-located table.
        createPartitionedTable("t3",
            pkColumn("k1"),
            affinityColumn("ak2"),
            "v3"
        );

        // Replicated table.
        createReplicatedTable("t4",
            pkColumn("k1"),
            "v2",
            "v3"
        );

        // Transfer through "AND".
        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? AND t2.ak2 = ?",
            (res) -> assertPartitions(
                parititon("t1", "1")
            ),
            "1", "1"
        );

        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? AND t2.ak2 = ?",
            (res) -> assertNoRequests(),
            "1", "2"
        );

        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? AND t2.ak2 IN (?, ?)",
            (res) -> assertPartitions(
                parititon("t1", "1")
            ),
            "1", "1", "2"
        );

        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? AND t2.ak2 IN (?, ?)",
            (res) -> assertNoRequests(),
            "1", "2", "3"
        );

        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 IN (?, ?) AND t2.ak2 IN (?, ?)",
            (res) -> assertPartitions(
                parititon("t1", "2")
            ),
            "1", "2", "2", "3"
        );

        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 IN (?, ?) AND t2.ak2 IN (?, ?)",
            (res) -> assertNoRequests(),
            "1", "2", "3", "4"
        );

        // Transfer through "OR".
        // TODO

        // No transfer through intermediate table.
        // TODO

        // No transfer through disjunction.
        // TODO
    }

    /**
     * Test various expressions.
     */
    @Test
    public void testExpressions() {
        // TODO
    }

    /**
     * Test cross-joins. They cannot "transfer" partitions between joined tables.
     */
    @Test
    public void testCrossJoin() {
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2");

        createPartitionedTable("t2",
            pkColumn("k1"),
            affinityColumn("ak2"),
            "v3");

        execute("INSERT INTO t1 VALUES ('1', '1')");
        execute("INSERT INTO t2 VALUES ('1', '1', '1')");

        execute("INSERT INTO t1 VALUES ('2', '2')");
        execute("INSERT INTO t2 VALUES ('2', '2', '2')");

        execute("INSERT INTO t1 VALUES ('3', '3')");
        execute("INSERT INTO t2 VALUES ('3', '3', '3')");

        // Left table, should work.
        executeCombinations("SELECT * FROM t1, t2 WHERE t1.k1 = ?",
            (res) -> {
                assertPartitions(
                    parititon("t1", "1")
                );
                assertEquals(1, res.size());
                assertEquals("1", res.get(0).get(0));
            },
            "1"
        );

        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON 1=1 WHERE t1.k1 = ?",
            (res) -> {
                assertPartitions(
                    parititon("t1", "1")
                );
                assertEquals(1, res.size());
                assertEquals("1", res.get(0).get(0));
            },
            "1"
        );

        // Right table, should work.
        executeCombinations("SELECT * FROM t1, t2 WHERE t2.ak2 = ?",
            (res) -> {
                assertPartitions(
                    parititon("t2", "2")
                );
                assertEquals(1, res.size());
                assertEquals("2", res.get(0).get(0));
            },
            "2"
        );

        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON 1=1 WHERE t2.ak2 = ?",
            (res) -> {
                assertPartitions(
                    parititon("t2", "2")
                );
                assertEquals(1, res.size());
                assertEquals("2", res.get(0).get(0));
            },
            "2"
        );

        executeCombinations("SELECT * FROM t1, t2 WHERE t1.k1=? AND t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "3", "3"
        );

        // Two tables, should not work.
        executeCombinations("SELECT * FROM t1, t2 WHERE t1.k1=? AND t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "3", "3"
        );

        executeCombinations("SELECT * FROM t1 INNER JOIN t2 ON 1=1 WHERE t1.k1=? AND t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "3", "3"
        );
    }

    /**
     * Test non-equijoins.
     */
    @Test
    public void testThetaJoin() {
        // TODO
    }

    /**
     * Test joins with REPLICTED caches.
     */
    @Test
    public void testJoinWithReplicated() {
        // TODO
    }

    /**
     * Test joins with different affinity functions.
     */
    @Test
    public void testJoinWithDifferentAffinityFunctions() {
        // Partition count.
        // TODO

        // Different affinity functions.
        // TODO

        // Node filters.
        // TODO
    }

    /**
     * Test joins with subqueries.
     */
    @Test
    public void testJoinWithSubquery() {
        // TODO
    }

    /**
     * Test joins when explicit partitions are set.
     */
    @Test
    public void testExplicitParititons() {
        // TODO
    }

    /**
     * Test outer joins.
     */
    @Test
    public void testOuterJoin() {
        // TODO
    }

    /**
     * Create PARTITIONED table.
     *
     * @param name Name.
     * @param cols Columns.
     */
    private void createPartitionedTable(String name, Object... cols) {
        createTable0(name, false, cols);
    }

    /**
     * Create REPLICATED table.
     *
     * @param name Name.
     * @param cols Columns.
     */
    private void createReplicatedTable(String name, Object... cols) {
        createTable0(name, true, cols);
    }

    /**
     * Internal CREATE TABLE routine.
     *
     * @param name Name.
     * @param replicated Replicated table flag.
     * @param cols Columns.
     */
    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    private void createTable0(String name, boolean replicated, Object... cols) {
        List<String> pkCols = new ArrayList<>();

        String affCol = null;

        StringBuilder sql = new StringBuilder("CREATE TABLE ").append(name).append("(");
        for (Object col : cols) {
            Column col0 = col instanceof Column ? (Column)col : new Column((String)col, false, false);

            sql.append(col0.name()).append(" VARCHAR, ");

            if (col0.pk())
                pkCols.add(col0.name());

            if (col0.affinity()) {
                if (affCol != null)
                    throw new IllegalStateException("Only one affinity column is allowed: " + col0.name());

                affCol = col0.name();
            }
        }

        if (pkCols.isEmpty())
            throw new IllegalStateException("No PKs!");

        sql.append("PRIMARY KEY (");

        boolean firstPkCol = true;

        for (String pkCol : pkCols) {
            if (firstPkCol)
                firstPkCol = false;
            else
                sql.append(", ");

            sql.append(pkCol);
        }

        sql.append(")");

        sql.append(") WITH \"template=" + (replicated ? "replicated" : "partitioned"));
        sql.append(", CACHE_NAME=" + name);

        if (affCol != null) {
            sql.append(", AFFINITY_KEY=" + affCol);
            sql.append(", KEY_TYPE=" + name + "_key");
        }

        sql.append("\"");

        execute(sql.toString());
    }

    /**
     * Execute query with all possible combinations of argument placeholders.
     *
     * @param sql SQL.
     * @param resConsumer Result consumer.
     * @param args Arguments.
     */
    public void executeCombinations(String sql, Consumer<List<List<?>>> resConsumer, Object... args) {
        System.out.println(">>> TEST COMBINATION: " + sql);

        // Execute query as is.
        List<List<?>> res = execute(sql, args);

        resConsumer.accept(res);

        // Start filling arguments recursively.
        if (args != null && args.length > 0)
            executeCombinations0(sql, resConsumer, new HashSet<>(), args);

        System.out.println();
    }

    /**
     * Execute query with all possible combinations of argument placeholders.
     *
     * @param sql SQL.
     * @param resConsumer Result consumer.
     * @param executedSqls Already executed SQLs.
     * @param args Arguments.
     */
    // TODO: Fix combinatorial checks!
    public void executeCombinations0(
        String sql,
        Consumer<List<List<?>>> resConsumer,
        Set<String> executedSqls,
        Object... args
    ) {
        assert args != null && args.length > 0;

        // Get argument positions.
        List<Integer> paramPoss = new ArrayList<>();

        int pos = 0;

        while (true) {
            int paramPos = sql.indexOf('?', pos);

            if (paramPos == -1)
                break;

            paramPoss.add(paramPos);

            pos = paramPos + 1;
        }

        for (int i = 0; i < args.length; i++) {
            // Prepare new SQL and arguments.
            int paramPos = paramPoss.get(i);

            String newSql = sql.substring(0, paramPos) + args[i] + sql.substring(paramPos + 1);

            Object[] newArgs = new Object[args.length - 1];

            int newArgsPos = 0;

            for (int j = 0; j < args.length; j++) {
                if (j != i)
                    newArgs[newArgsPos++] = args[j];
            }

            // Execute if this combination was never executed before.
            if (executedSqls.add(newSql)) {
                List<List<?>> res = execute(newSql, newArgs);

                resConsumer.accept(res);
            }

            // Continue recursively.
            if (newArgs.length > 0)
                executeCombinations0(newSql, resConsumer, executedSqls, newArgs);
        }
    }

    /**
     * Execute SQL query.
     *
     * @param sql SQL.
     */
    private List<List<?>> execute(String sql, Object... args) {
        clearIoState();

        if (args == null || args.length == 0)
            System.out.println(">>> " + sql);
        else
            System.out.println(">>> " + sql + " " + Arrays.toString(args));

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return client().context().query().querySqlFields(qry, false).getAll();
    }

    /**
     * @return Client node.
     */
    private IgniteEx client() {
        return grid(CLI_NAME);
    }

    /**
     * Clear partitions.
     */
    private static void clearIoState() {
        INTERCEPTED_REQS.set(0);
        INTERCEPTED_PARTS.clear();
    }

    /**
     * Make sure that expected partitions are logged.
     *
     * @param expParts Expected partitions.
     */
    private static void assertPartitions(int... expParts) {
        Collection<Integer> expParts0 = new TreeSet<>();

        for (int expPart : expParts)
            expParts0.add(expPart);

        assertPartitions(expParts0);
    }

    /**
     * Make sure that expected partitions are logged.
     *
     * @param expParts Expected partitions.
     */
    private static void assertPartitions(Collection<Integer> expParts) {
        TreeSet<Integer> expParts0 = new TreeSet<>(expParts);
        TreeSet<Integer> actualParts = new TreeSet<>(INTERCEPTED_PARTS);

        assertEquals("Unexpected partitions [exp=" + expParts + ", actual=" + actualParts + ']',
            expParts0, actualParts);
    }

    /**
     * Make sure that no partitions were extracted.
     */
    private static void assertNoPartitions() {
        assertTrue("No requests were sent.", INTERCEPTED_REQS.get() > 0);
        assertTrue("Partitions are not empty: " + INTERCEPTED_PARTS, INTERCEPTED_PARTS.isEmpty());
    }

    /**
     * Make sure there were no requests sent because we determined empty partition set.
     */
    private static void assertNoRequests() {
        assertEquals("Requests were sent: " + INTERCEPTED_REQS.get(), 0, INTERCEPTED_REQS.get());
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @return Partition.
     */
    private int parititon(String cacheName, Object key) {
        return client().affinity(cacheName).partition(key);
    }

    /**
     * TCP communication SPI which will track outgoing query requests.
     */
    private static class TrackingTcpCommunicationSpi extends TcpCommunicationSpi {
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
            if (msg instanceof GridIoMessage) {
                GridIoMessage msg0 = (GridIoMessage)msg;

                if (msg0.message() instanceof GridH2QueryRequest) {
                    INTERCEPTED_REQS.incrementAndGet();

                    GridH2QueryRequest req = (GridH2QueryRequest)msg0.message();

                    int[] parts = req.queryPartitions();

                    if (!F.isEmpty(parts)) {
                        for (int part : parts)
                            INTERCEPTED_PARTS.add(part);
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }

    /**
     * @param name Name.
     * @return PK column.
     */
    public Column pkColumn(String name) {
        return new Column(name, true, false);
    }

    /**
     * @param name Name.
     * @return Affintiy column.
     */
    public Column affinityColumn(String name) {
        return new Column(name, true, true);
    }

    /**
     * Column.
     */
    private static class Column {
        /** Name. */
        private final String name;

        /** PK. */
        private final boolean pk;

        /** Affinity key. */
        private final boolean aff;

        /**
         * Constructor.
         *
         * @param name Name.
         * @param pk PK flag.
         * @param aff Affinity flag.
         */
        public Column(String name, boolean pk, boolean aff) {
            this.name = name;
            this.pk = pk;
            this.aff = aff;
        }

        /**
         * @return Name.
         */
        public String name() {
            return name;
        }

        /**
         * @return PK flag.
         */
        public boolean pk() {
            return pk;
        }

        /**
         * @return Affintiy flag.
         */
        public boolean affinity() {
            return aff;
        }
    }
}
