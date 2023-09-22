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
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Stream;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.junit.Test;

/** */
public class PartitionPruneTest extends AbstractBasicIntegrationTest {
    /** */
    private static final int ENTRIES_COUNT = 10000;

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

                        List<ColocationGroup> groups = U.field(mapping, "colocationGroups");

                        assertEquals(1, F.size(groups));

                        int[] parts = F.first(groups).partitions(node.id());

                        if (!F.isEmpty(parts)) {
                            for (int part: parts)
                                INTERCEPTED_PARTS.add(part);
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
        sql("CREATE TABLE T2(ID INT, T1_ID INT, IDX_VAL VARCHAR, VAL VARCHAR, PRIMARY KEY(ID, T1_ID)) WITH " +
                "cache_name=t2_cache,backups=1,affinity_key=t1_id");
        sql("CREATE TABLE DICT(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH template=replicated,cache_name=dict_cache");

        sql("CREATE INDEX T1_IDX ON T1(IDX_VAL)");
        sql("CREATE INDEX T2_IDX ON T2(IDX_VAL)");
        sql("CREATE INDEX T2_AFF ON T2(T1_ID)");
        sql("CREATE INDEX DICT_IDX ON DICT(IDX_VAL)");

        Stream.of("T1", "DICT").forEach(tableName -> {
            StringBuilder sb = new StringBuilder("INSERT INTO ").append(tableName)
                    .append("(ID, IDX_VAL, VAL) VALUES ");

            for (int i = 0; i < 10000; ++i) {
                sb.append("(").append(i).append(", ")
                        .append("'name_").append(i).append("', ")
                        .append("'name_").append(i).append("')");

                if (i < ENTRIES_COUNT - 1)
                    sb.append(",");
            }

            sql(sb.toString());

            assertEquals(ENTRIES_COUNT, client.getOrCreateCache(tableName + "_CACHE").size(CachePeekMode.PRIMARY));
        });

        INTERCEPTED_START_REQUEST_COUNT.reset();
        INTERCEPTED_PARTS.clear();
        INTERCEPTED_NODES.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        INTERCEPTED_START_REQUEST_COUNT.reset();
        INTERCEPTED_PARTS.clear();
        INTERCEPTED_NODES.clear();
    }

    /** */
    @Test
    public void testSimplePlanning() {
//        String sqlStr = "select * from T1 join T2 " +
//                " on T1.ID=T2.ID where T1.ID = 10 and T2.IDX_VAL <= 'test_1'";

//        String sqlStr = "select * from T1, T2 " +
//                " where T1._KEY = ? and T2.T1_ID = ? and T2.IDX_VAL <= ?";
        //String sqlStr = "select * from T1 where T1.ID = ? AND (T1.ID = ? or T1.ID = ? or T1.ID = ?)";

//        execute("select * from T1 where ID = ? OR ID = ?",
//            res -> {
//                assertPartitions(partition("T1_CACHE", 123), partition("T1_CACHE", 125));
//
//                assertTrue(res.size() == 2);
//            },
//            123, 125);

        execute("select * from T1 join DICT on T1.ID = DICT.ID where T1.ID in (?, ?)",
            res -> {
                //assertPartitions(partition("T1_CACHE", 123), partition("T1_CACHE", 125));
                assertPartitions(partition("T1_CACHE", 123), partition("T1_CACHE", 125));
                assertTrue(res.size() == 2);
            },
            123, 125);
    }

    /** */
    @Test
    public void testSelectIn() {
        execute("select * from T1 where T1.ID in (?, ?)",
            res -> {
                assertPartitions(partition("T1_CACHE", 123), partition("T1_CACHE", 125));

                assertTrue(res.size() == 2);
            },
            123, 125);

        execute("select * from T1 where T1.ID in (?, ?)",
            res -> {
                assertPartitions(partition("T1_CACHE", 123), partition("T1_CACHE", 125));

                assertTrue(res.size() == 2);
            },
            123, 125);

        execute("select * from T1 where T1.ID in (?, ?, ?)",
            res -> {
                assertPartitions(partition("T1_CACHE", 123), partition("T1_CACHE", 125), partition("T1_CACHE", 127));

                assertTrue(res.size() == 3);
            },
            123, 125, 127);

        execute("select * from T1 where T1.ID in (?, ?, ?, ?)",
                res -> {
                    assertPartitions(partition("T1_CACHE", 123), partition("T1_CACHE", 125), partition("T1_CACHE", 127),
                            partition("T1_CACHE", 128));

                    assertTrue(res.size() == 4);
                },
                123, 125, 127, 128);
    }

    /** */
    public void execute(String sql, Consumer<List<List<?>>> resConsumer, Object... args) {
        log.info(">>> TEST COMBINATION: \"" + sql + "\"");

        // Execute query as is.
        log.info("Execute \"" + sql + "\"");

        List<List<?>> res = sql(sql, args);

        resConsumer.accept(res);

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

            String newSql = sql.substring(0, paramPos) + args[i] + sql.substring(paramPos + 1);

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

        assertEquals("Unexpected partitions [exp=" + Arrays.toString(expParts) + ", actual=" + actualParts + ']',
                expParts0, actualParts);
    }

    /** */
    protected int partition(String cacheName, Object key) {
        return client.affinity(cacheName).partition(key);
    }

    /** */
    protected ClusterNode node(String cacheName, Object key) {
        return G.allGrids().stream()
            .filter(ign -> ign.affinity(cacheName).isPrimary(ign.cluster().node(), key))
            .map(ign -> ign.cluster().localNode()).findFirst().orElse(null);
    }

    /** */
    protected static void assertNodes(ClusterNode... expNodes) {
        Collection<ClusterNode> expNodes0 = new TreeSet<>(Comparator.comparing(ClusterNode::id));

        if (!F.isEmpty(expNodes)) {
            expNodes0.addAll(Arrays.asList(expNodes));
        }

        TreeSet<ClusterNode> actualNodes = new TreeSet<>(INTERCEPTED_NODES);

        assertEquals("Unexpected nodes [exp=" + Arrays.toString(expNodes) + ", actual=" + actualNodes + ']',
                expNodes0, actualNodes);
    }

}
