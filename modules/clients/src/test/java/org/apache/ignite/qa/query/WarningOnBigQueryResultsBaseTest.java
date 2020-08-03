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

package org.apache.ignite.qa.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;

/**
 * Tests for log print for long running query.
 */
public class WarningOnBigQueryResultsBaseTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    protected static final int KEYS_PER_NODE = 1000;

    /** Keys count. */
    protected static final int CLI_PORT = 10850;

    /** Node attribute for test0 cache. */
    protected static final String TEST0_ATTR = "TEST0_ATTR";

    /** Node attribute for test0 cache. */
    protected static final String TEST1_ATTR = "TEST1_ATTR";

    /** Cache name 0. */
    protected static final String CACHE0 = "test0";

    /** Cache name 1. */
    protected static final String CACHE1 = "test1";

    /** Log message pattern. */
    private static final Pattern logPtrn = Pattern.compile(
        "fetched=([0-9]+), duration=([0-9]+)ms, type=(MAP|LOCAL|REDUCE), distributedJoin=(true|false), enforceJoinOrder=(true|false), lazy=(true|false), schema=(\\S+), sql");

    /** Test log. */
    private static Map<String, BigResultsLogListener> logListeners = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName).
            setCacheConfiguration(
                new CacheConfiguration()
                    .setName(CACHE0)
                    .setSqlSchema("TEST0")
                    .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                        .setTableName("test0")
                        .addQueryField("id", Long.class.getName(), null)
                        .addQueryField("val", Long.class.getName(), null)
                        .setKeyFieldName("id")
                        .setValueFieldName("val")))
                    .setAffinity(new RendezvousAffinityFunction(false, 10))
                    .setNodeFilter((IgnitePredicate<ClusterNode>)node ->
                        node.attribute(TEST0_ATTR) != null && (boolean)node.attribute(TEST0_ATTR)),
                new CacheConfiguration()
                    .setName(CACHE1)
                    .setSqlSchema("TEST1")
                    .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                        .setTableName("test1")
                        .addQueryField("id", Long.class.getName(), null)
                        .addQueryField("val", Long.class.getName(), null)
                        .setKeyFieldName("id")
                        .setValueFieldName("val")))
                    .setAffinity(new RendezvousAffinityFunction(false, 10))
                    .setNodeFilter((IgnitePredicate<ClusterNode>)node ->
                        node.attribute(TEST1_ATTR) != null && (boolean)node.attribute(TEST1_ATTR)));

        if (igniteInstanceName.startsWith("cli")) {
            cfg.setClientMode(true)
                .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                    .setPort(CLI_PORT));
        }
        else {
            cfg.setUserAttributes(Collections.singletonMap(
                getTestIgniteInstanceIndex(igniteInstanceName) < 2 ? TEST0_ATTR : TEST1_ATTR, true));
        }

        ListeningTestLogger testLog = new ListeningTestLogger(false, log);
        BigResultsLogListener lst = new BigResultsLogListener();

        testLog.registerListener(lst);

        logListeners.put(igniteInstanceName, lst);

        cfg.setGridLogger(new ListeningTestLogger(false, testLog));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        // Starts the first node.
        startGrid(0);

        // The client must be connected to grid0 according with test plan.
        startGrid("cli");

        // Starts other server nodes.
        startGrid(1);
        startGrid(2);
        startGrid(3);

        awaitPartitionMapExchange();

        // Populate data to test0 cache.
        for (int i = 0; i < 2; ++i) {
            List<Integer> keys = primaryKeys(grid(i).cache(CACHE0), KEYS_PER_NODE, 0);

            assertEquals(KEYS_PER_NODE, keys.size());

            for (int k : keys)
                grid(i).cache(CACHE0).put((long)k, (long)k);
        }

        // Populate data to test1 cache.
        for (int i = 2; i < 4; ++i) {
            List<Integer> keys = primaryKeys(grid(i).cache(CACHE1), KEYS_PER_NODE, 0);

            assertEquals(KEYS_PER_NODE, keys.size());

            for (int k : keys)
                grid(i).cache(CACHE1).put((long)k, (long)k);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        setBigResultThreshold(grid("cli"), 10, 3);

        setBigResultThreshold(grid(0), 10, 3);
        setBigResultThreshold(grid(1), 25, 2);
        setBigResultThreshold(grid(2), 50, 2);
        setBigResultThreshold(grid(3), 100, 1);

        logListeners.values().forEach(BigResultsLogListener::reset);
    }

    /**
     *
     */
    protected void setBigResultThreshold(IgniteEx node, long threshold, int thresholdMult) throws Exception {
        GridTestUtils.setJmxAttribute(node, "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThreshold", threshold);
        GridTestUtils.setJmxAttribute(
            node, "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThresholdMultiplier", thresholdMult);
    }

    /**
     * Checks that duration is increase between messages.
     */
    protected void checkDurations(List<Long> durations) {
        assertFalse(F.isEmpty(durations));

        assertTrue("Invalid durations: " + durations,durations.get(0) >= 0);

        for (int i = 0; i < durations.size() - 1; ++i) {
            assertTrue("Invalid durations: " + durations,durations.get(i + 1) >= 0);
            assertTrue("Invalid durations: " + durations, durations.get(i) <= durations.get(i + 1));
        }
    }

    /**
     * @return Log listener is assigned to specified node.
     */
    protected BigResultsLogListener listener(Ignite node) {
        assert logListeners.get(node.name()) != null;

        return logListeners.get(node.name());
    }

    /**
     *
     */
    public static class BigResultsLogListener extends LogListener {
        /** Fetched. */
        ArrayList<Long> fetched = new ArrayList<>();

        /** Duration. */
        ArrayList<Long> duration = new ArrayList<>();

        /** Lazy flag. */
        boolean lazy;

        /** Enforce join order flag. */
        boolean enforceJoinOrder;

        /** Distributed join flag. */
        boolean distributedJoin;

        /** Query type. */
        String type;

        /** Query schema. */
        String schema;

        /** Sql query. */
        String sql;

        /** Query plan. */
        String plan;

        /** {@inheritDoc} */
        @Override public boolean check() {
            return messageCount() > 0;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            duration.clear();
            fetched.clear();
        }

        /** {@inheritDoc} */
        @Override public void accept(String s) {
            if (s.contains("Query produced big result set")) {
                Matcher m = logPtrn.matcher(s);

                assertTrue(m.find());

                fetched.add(Long.parseLong(m.group(1)));
                duration.add(Long.parseLong(m.group(2)));
                type = m.group(3);
                distributedJoin = Boolean.parseBoolean(m.group(4));
                enforceJoinOrder = Boolean.parseBoolean(m.group(5));
                lazy = Boolean.parseBoolean(m.group(6));
                schema = m.group(7);

                sql = s.substring(s.indexOf(", sql='") + 7, s.indexOf("', plan="));
                if ("REDUCE".equals(type))
                    plan = s.substring(s.indexOf("', plan=") + 8, s.indexOf(", reqId="));
                else
                    plan = s.substring(s.indexOf("', plan=") + 8, s.indexOf(", node="));

                assertTrue(sql.contains("SELECT"));
                assertTrue(plan.contains("SELECT"));
            }
        }

        /**
         * @return Count of
         */
        public int messageCount() {
            return fetched.size();
        }
    }
}
