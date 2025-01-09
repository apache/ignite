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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableList;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartRequest;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.junit.Test;

/** */
public class PartitionPruneTest extends AbstractBasicIntegrationTest {
    /** */
    private static final int ENTRIES_COUNT = 10000;

    /** */
    private static final int LONG_QUERY_WARNING_TIMEOUT = 20000;

    /** */
    private static final LongAdder INTERCEPTED_START_REQUEST_COUNT = new LongAdder();

    /** */
    private static final ConcurrentSkipListSet<Integer> INTERCEPTED_PARTS = new ConcurrentSkipListSet<>();

    /** */
    private static final ConcurrentSkipListSet<ClusterNode> INTERCEPTED_NODES = new ConcurrentSkipListSet<>(
            Comparator.comparing(ClusterNode::id));

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());
        cfg.getSqlConfiguration().setLongQueryWarningTimeout(LONG_QUERY_WARNING_TIMEOUT);

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            /** {@inheritDoc} */
            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                assert msg != null;

                if (msg instanceof GridIoMessage) {
                    GridIoMessage msg0 = (GridIoMessage)msg;

                    if (msg0.message() instanceof QueryStartRequest) {
                        INTERCEPTED_START_REQUEST_COUNT.increment();
                        INTERCEPTED_NODES.add(node);

                        QueryStartRequest startReq = (QueryStartRequest)msg0.message();

                        assertNotNull(startReq.fragmentDescription());

                        FragmentMapping mapping = startReq.fragmentDescription().mapping();

                        assertNotNull(mapping);

                        List<ColocationGroup> grps = mapping.colocationGroups();

                        for (ColocationGroup grp: grps) {
                            int[] parts = grp.partitions(node.id());

                            if (!F.isEmpty(parts)) {
                                for (int part : parts)
                                    INTERCEPTED_PARTS.add(part);
                            }
                        }
                    }
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        sql("CREATE TABLE T1(ID INT, IDX_VAL VARCHAR, VAL VARCHAR, PRIMARY KEY(ID)) WITH cache_name=t1_cache,backups=1");
        sql("CREATE TABLE T2(ID INT, IDX_VAL VARCHAR, VAL VARCHAR, PRIMARY KEY(ID)) WITH cache_name=t2_cache,backups=1");
        sql("CREATE TABLE DICT(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH template=replicated,cache_name=dict_cache");

        sql("CREATE INDEX T1_IDX ON T1(IDX_VAL)");
        sql("CREATE INDEX T2_IDX ON T2(IDX_VAL)");
        sql("CREATE INDEX DICT_IDX ON DICT(IDX_VAL)");

        Stream.of("T1", "T2", "DICT").forEach(tableName -> {
            StringBuilder sb = new StringBuilder("INSERT INTO ").append(tableName)
                    .append("(ID, IDX_VAL, VAL) VALUES ");

            for (int i = 0; i < ENTRIES_COUNT; ++i) {
                sb.append("(").append(i).append(", ")
                        .append("'name_").append(i).append("', ")
                        .append("'name_").append(i).append("')");

                if (i < ENTRIES_COUNT - 1)
                    sb.append(",");
            }

            sql(sb.toString());

            assertEquals(ENTRIES_COUNT, client.getOrCreateCache(tableName + "_CACHE").size(CachePeekMode.PRIMARY));
        });

        sql("ANALYZE PUBLIC.T1(ID), PUBLIC.T2(ID), PUBLIC.DICT(ID) WITH \"NULLS=0,DISTINCT=" + ENTRIES_COUNT +
                ",TOTAL=" + ENTRIES_COUNT + "\"");

        clearIntercepted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        clearIntercepted();
    }

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 8;
    }

    /** */
    @Test
    public void testSimple() {
        execute("select count(*) from T1 where T1.ID = ?",
            res -> {
                assertPartitions(partition("T1_CACHE", 123));
                assertNodes(node("T1_CACHE", 123));

                assertEquals(1, res.size());
                assertEquals(1L, res.get(0).get(0));
            },
            123);

        execute("select * from T1 where T1.ID = ?",
            res -> {
                assertPartitions(partition("T1_CACHE", 123));
                assertNodes(node("T1_CACHE", 123));

                assertEquals(1, res.size());
                assertEquals("name_123", res.get(0).get(1));
            },
            123);

        execute("select * from T1 where ? = T1.ID",
            res -> {
                assertPartitions(partition("T1_CACHE", 123));
                assertNodes(node("T1_CACHE", 123));

                assertEquals(1, res.size());
                assertEquals("name_123", res.get(0).get(1));
            },
            123);

        execute("select VAL, IDX_VAL, ID from T1 where T1.ID = ?",
            res -> {
                assertPartitions(partition("T1_CACHE", 123));
                assertNodes(node("T1_CACHE", 123));

                assertEquals(1, res.size());
                assertEquals("name_123", res.get(0).get(1));
            },
            123);

        execute("select * from T1 where T1.ID = ? and T1.ID = ?",
            res -> {
                assertPartitions(partition("T1_CACHE", 123));
                assertNodes(node("T1_CACHE", 123));

                assertEquals(1, res.size());
                assertEquals("name_123", res.get(0).get(1));
            },
            123, 123);
    }

    /** */
    @Test
    public void testNullsInCondition() {
        execute("select * from T1 where T1.ID is NULL",
            res -> {
                assertPartitions();
                assertNodes();

                assertTrue(res.isEmpty());
            });

        execute("select * from T1 where T1.ID = ?",
            res -> {
                assertPartitions();
                assertNodes();

                assertTrue(res.isEmpty());
            }, new Object[]{ null });

        execute("select * from T1 where T1.ID is NULL and T1.ID = ?",
            res -> {
                assertPartitions();
                assertNodes();

                assertTrue(res.isEmpty());
            }, 123);

        execute("select * from T1 where T1.ID = ? and T1.ID = ?",
            res -> {
                assertPartitions();
                assertNodes();

                assertTrue(res.isEmpty());
            }, null, 123);

        execute("select * from T1 where T1.ID is NULL or T1.ID = ?",
            res -> {
                assertPartitions(partition("T1_CACHE", 123));
                assertNodes(node("T1_CACHE", 123));

                assertEquals(1, res.size());
                assertEquals("name_123", res.get(0).get(1));
            }, 123);

        execute("select * from T1 where T1.ID = ? or T1.ID = ?",
            res -> {
                assertPartitions(partition("T1_CACHE", 123));
                assertNodes(node("T1_CACHE", 123));

                assertEquals(1, res.size());
                assertEquals("name_123", res.get(0).get(1));
            }, null, 123);
    }

    /** */
    @Test
    public void testEmptyConditions() {
        execute("select * from T1 where T1.ID = ? and T1.ID = ?",
            res -> {
                assertPartitions();
                assertNodes();

                assertTrue(res.isEmpty());
            },
            123, 518);

        execute("select * from T1 where T1.ID = ? and (T1.ID = ? OR T1.ID = ? OR T1.ID = ?)",
            res -> {
                assertPartitions();
                assertNodes();

                assertTrue(res.isEmpty());
            },
            123, 518, 781, 295);

        execute("select * from T1 where (T1.ID = ? OR T1.ID = ?) AND (T1.ID = ? OR T1.ID = ?)",
            res -> {
                assertPartitions();
                assertNodes();

                assertTrue(res.isEmpty());
            },
            123, 518, 781, 295);

        execute("select * from T1 where (T1.ID = ? AND T1.ID = ?) OR (T1.ID = ? AND T1.ID = ?)",
            res -> {
                assertPartitions();
                assertNodes();

                assertTrue(res.isEmpty());
            },
            123, 518, 781, 295);
    }

    /** */
    @Test
    public void testSelectIn() {
        IntStream.of(2, 6).forEach(i -> {
            testSelect(i, true, "_KEY");
            testSelect(i, true, "ID");
        });
    }


    /** */
    @Test
    public void testSelectOr() {
        testSelect(1, false, "_KEY");
        testSelect(1, false, "ID");

        IntStream.of(2, 6).forEach(i -> {
            testSelect(i, false, "_KEY");
            testSelect(i, false, "ID");
        });
    }

    /** */
    @Test
    public void testSetOperations() {
        execute("SELECT ID, VAL FROM T1 WHERE T1.ID = ? UNION SELECT ID, VAL FROM T1 WHERE T1.ID = ?",
            (res) -> {
                assertPartitions(partition("T1_CACHE", 123), partition("T1_CACHE", 125));
                assertNodes(node("T1_CACHE", 123), node("T1_CACHE", 125));
                assertEquals(2, res.size());
                assertEquals(ImmutableList.of(123, 125), res.stream().map(row -> (Integer)row.get(0)).sorted()
                    .collect(Collectors.toList()));
            },
            123, 125
        );

        execute("SELECT ID, VAL FROM T1 WHERE T1.ID = ? INTERSECT SELECT ID, VAL FROM T1 WHERE T1.ID = ?",
            (res) -> {
                assertPartitions(partition("T1_CACHE", 123), partition("T1_CACHE", 125));
                assertNodes(node("T1_CACHE", 123), node("T1_CACHE", 125));
                assertEquals(0, res.size());
            },
            123, 125
        );

        execute("SELECT ID, VAL FROM T1 WHERE T1.ID = ? UNION SELECT ID, VAL FROM T2 WHERE T2.ID = ?",
            (res) -> {
                assertPartitions(partition("T1_CACHE", 123), partition("T2_CACHE", 125));
                assertNodes(node("T1_CACHE", 123), node("T2_CACHE", 125));
                assertEquals(2, res.size());
                assertEquals(ImmutableList.of(123, 125), res.stream().map(row -> (Integer)row.get(0)).sorted()
                        .collect(Collectors.toList()));
            },
            123, 125
        );

        execute("SELECT ID, VAL FROM T1 WHERE T1.ID = ? INTERSECT SELECT ID, VAL FROM T2 WHERE T2.ID = ?",
            (res) -> {
                assertPartitions(partition("T1_CACHE", 123), partition("T2_CACHE", 125));
                assertNodes(node("T1_CACHE", 123), node("T2_CACHE", 125));
                assertEquals(0, res.size());
            },
            123, 125
        );

        execute("SELECT ID, VAL FROM T1 WHERE T1.ID = ? UNION SELECT ID, VAL FROM DICT WHERE DICT.ID = ?",
            (res) -> {
                assertPartitions(partition("T1_CACHE", 123));
                assertContainsNodes(node("T1_CACHE", 123));

                assertEquals(2, res.size());
                assertEquals(ImmutableList.of(123, 125), res.stream().map(row -> (Integer)row.get(0)).sorted()
                        .collect(Collectors.toList()));
            },
            123, 125
        );

        execute("SELECT ID, VAL FROM T1 WHERE T1.ID = ? INTERSECT SELECT ID, VAL FROM DICT WHERE DICT.ID = ?",
            (res) -> {
                assertPartitions(partition("T1_CACHE", 123));
                assertContainsNodes(node("T1_CACHE", 123));

                assertEquals(0, res.size());
            },
            123, 125
        );
    }

    /** */
    private void testSelect(int sz, boolean withIn, String column) {
        assertTrue(sz >= 1);
        int[] values = ThreadLocalRandom.current().ints(0, ENTRIES_COUNT).distinct().limit(sz).toArray();

        StringBuilder qry;

        if (!withIn || sz == 1)
            qry = new StringBuilder("select * from T1 where ");
        else
            qry = new StringBuilder("select * from T1 where T1.").append(column).append(" in (");

        for (int i = 0; i < sz; ++i) {
            if (!withIn || sz == 1)
                qry.append("T1.").append(column).append("= ?");
            else
                qry.append('?');

            if (sz == 1)
                break;

            if (i == sz - 1)
                qry.append(!withIn ? "" : ")");
            else
                qry.append(!withIn ? " OR " : ", ");
        }

        execute(qry.toString(),
            res -> {
                assertPartitions(IntStream.of(values).map(i -> partition("T1_CACHE", i)).toArray());
                assertNodes(IntStream.of(values).mapToObj(i -> node("T1_CACHE", i)).toArray(ClusterNode[]::new));

                assertEquals(values.length, res.size());

                assertEquals(
                    IntStream.of(values).sorted().boxed().collect(Collectors.toList()),
                    res.stream().map(row -> row.get(0)).sorted().collect(Collectors.toList())
                );
            },
            IntStream.of(values).boxed().toArray(Integer[]::new));
    }

    /** */
    public void execute(String sql, Consumer<List<List<?>>> resConsumer, Object... args) {
        log.info(">>> TEST COMBINATION: \"" + sql + "\"");

        // Execute query as is.
        log.info("Execute \"" + sql + "\" with args " + Arrays.toString(args));

        List<List<?>> res = sql(sql, args);

        resConsumer.accept(res);
        clearIntercepted();

        // Start filling arguments recursively.
        if (args != null && args.length > 0)
            executeCombinations0(sql, resConsumer, new HashSet<>(), args);
    }

    /** */
    private void executeCombinations0(
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

            String newSql = sql.substring(0, paramPos) + (args[i] instanceof String ? "'" + args[i] + "'" : args[i])
                    + sql.substring(paramPos + 1);

            Object[] newArgs = new Object[args.length - 1];

            int newArgsPos = 0;

            for (int j = 0; j < args.length; j++) {
                if (j != i)
                    newArgs[newArgsPos++] = args[j];
            }

            // Execute if this combination was never executed before.
            if (executedSqls.add(newSql)) {
                log.info("Execute sql \"" + newSql + "\"");

                List<List<?>> res = sql(newSql, newArgs);

                resConsumer.accept(res);
                clearIntercepted();
            }

            // Continue recursively.
            if (newArgs.length > 0)
                executeCombinations0(newSql, resConsumer, executedSqls, newArgs);
        }
    }

    /** */
    protected static void assertPartitions(int... expParts) {
        Collection<Integer> expParts0 = new TreeSet<>();

        if (!F.isEmpty(expParts)) {
            for (int expPart : expParts)
                expParts0.add(expPart);
        }

        TreeSet<Integer> actualParts = new TreeSet<>(INTERCEPTED_PARTS);

        assertEquals("Unexpected partitions [exp=" + expParts0 + ", actual=" + actualParts + ']',
                expParts0, actualParts);
    }

    /** */
    protected int partition(String cacheName, Object key) {
        return client.affinity(cacheName).partition(key);
    }

    /** */
    protected int[] allPartitions(String cacheName) {
        return IntStream.range(0, client.affinity(cacheName).partitions()).toArray();
    }

    /** */
    protected ClusterNode node(String cacheName, Object key) {
        return G.allGrids().stream()
            .filter(ign -> ign.affinity(cacheName).isPrimary(ign.cluster().localNode(), key))
            .map(ign -> ign.cluster().localNode()).findFirst().orElse(null);
    }

    /** */
    protected ClusterNode[] allNodes(String cacheName) {
        return G.allGrids().stream()
            .map(ign -> ign.cluster().localNode())
            .filter(n -> client.affinity(cacheName).allPartitions(n).length != 0)
            .toArray(ClusterNode[]::new);
    }

    /** */
    protected static void assertContainsNodes(ClusterNode... expNodes) {
        TreeSet<ClusterNode> actualNodes = new TreeSet<>(INTERCEPTED_NODES);

        if (!F.isEmpty(expNodes)) {
            for (ClusterNode node: expNodes)
                assertTrue("Actual nodes doesn't contain node [actual=" + actualNodes + ", node= " + node,
                    actualNodes.contains(node));
        }
    }

    /** */
    protected static void assertNodes(ClusterNode... expNodes) {
        Collection<ClusterNode> expNodes0 = new TreeSet<>(Comparator.comparing(ClusterNode::id));

        if (!F.isEmpty(expNodes))
            expNodes0.addAll(Arrays.asList(expNodes));

        TreeSet<ClusterNode> actualNodes = new TreeSet<>(INTERCEPTED_NODES);

        assertEquals("Unexpected nodes [exp=" + Arrays.toString(expNodes) + ", actual=" + actualNodes + ']',
                expNodes0, actualNodes);
    }

    /** */
    protected static void clearIntercepted() {
        INTERCEPTED_START_REQUEST_COUNT.reset();
        INTERCEPTED_PARTS.clear();
        INTERCEPTED_NODES.clear();
    }
}
