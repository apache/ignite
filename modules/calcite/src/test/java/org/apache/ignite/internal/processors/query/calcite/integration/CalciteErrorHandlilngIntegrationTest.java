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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.exec.LogicalRelImplementor.CNLJ_NOT_SUPPORTED_JOIN_ASSERTION_MSG;

/** */
@WithSystemProperty(key = "calcite.debug", value = "false")
public class CalciteErrorHandlilngIntegrationTest extends GridCommonAbstractTest {
    /** */
    @After
    public void cleanUp() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSqlConfiguration(
            new SqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));
    }

    /**
     * Test verifies that AssertionError on fragment deserialization phase doesn't lead to execution freezing.
     * <ol>
     *     <li>Start several nodes.</li>
     *     <li>Replace CommunicationSpi to one that modifies messages (replace join type inside a QueryStartRequest).</li>
     *     <li>Execute query that requires CNLJ.</li>
     *     <li>Verify that query failed with proper exception.</li>
     * </ol>
     */
    @Test
    public void assertionOnDeserialization() throws Exception {
        Supplier<TcpCommunicationSpi> spiLsnrSupp = () -> new TcpCommunicationSpi() {
            /** {@inheritDoc} */
            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof QueryStartRequest) {
                    QueryStartRequest req = (QueryStartRequest)((GridIoMessage)msg).message();

                    String root = GridTestUtils.getFieldValue(req, "root");

                    GridTestUtils.setFieldValue(req, "root",
                        root.replace("\"joinType\":\"inner\"", "\"joinType\":\"full\""));
                }

                super.sendMessage(node, msg, ackC);
            }
        };

        startGrid(createConfiguration(1, false).setCommunicationSpi(spiLsnrSupp.get()));
        startGrid(createConfiguration(2, false).setCommunicationSpi(spiLsnrSupp.get()));

        IgniteEx client = startGrid(createConfiguration(0, true).setCommunicationSpi(spiLsnrSupp.get()));

        sql(client, "create table test (id int primary key, val varchar)");

        String sql = "select /*+ CNL_JOIN */ t1.id " +
            "from test t1, test t2 where t1.id = t2.id";

        Throwable t = GridTestUtils.assertThrowsWithCause(() -> sql(client, sql), AssertionError.class);
        assertEquals(CNLJ_NOT_SUPPORTED_JOIN_ASSERTION_MSG, t.getCause().getMessage());
    }

    /**
     * Test verifies that exception on fragment deserialization phase doesn't lead to execution freezing.
     * <ol>
     *     <li>Start several nodes.</li>
     *     <li>Replace CommunicationSpi to one that modifies messages (invalid fragment JSON).</li>
     *     <li>Execute query.</li>
     *     <li>Verify that query failed with proper exception.</li>
     * </ol>
     */
    @Test
    public void assertionOnDeserializationInvalidFragment() throws Exception {
        Supplier<TcpCommunicationSpi> spiLsnrSupp = () -> new TcpCommunicationSpi() {
            /** {@inheritDoc} */
            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof QueryStartRequest) {
                    QueryStartRequest req = (QueryStartRequest)((GridIoMessage)msg).message();

                    String root = GridTestUtils.getFieldValue(req, "root");

                    GridTestUtils.setFieldValue(req, "root",
                        root.replace("\"table\"", "\"invalidTag\""));
                }

                super.sendMessage(node, msg, ackC);
            }
        };

        startGrid(createConfiguration(1, false).setCommunicationSpi(spiLsnrSupp.get()));
        startGrid(createConfiguration(2, false).setCommunicationSpi(spiLsnrSupp.get()));

        IgniteEx client = startGrid(createConfiguration(0, true).setCommunicationSpi(spiLsnrSupp.get()));

        sql(client, "create table test (id int primary key, val varchar)");

        String sql = "select id from test";

        GridTestUtils.assertThrowsWithCause(() -> sql(client, sql), NullPointerException.class);
    }

    /**
     * Test verifies that a Exception during index look up doesn't lead to execution freezing.
     * <ol>
     *     <li>Start several nodes.</li>
     *     <li>Inject tree's action wrapper that throws exception on demand.</li>
     *     <li>Execute query that do index look up.</li>
     *     <li>Verify that query failed with proper exception.</li>
     *     <li>Verify that FailureHandler was triggered.</li>
     * </ol>
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void assertionOnTreeLookup() throws Exception {
        AtomicBoolean shouldThrow = new AtomicBoolean();

        BPlusTree.testHndWrapper = (tree, hnd) -> {
            if (hnd instanceof BPlusTree.Search) {
                PageHandler<Object, BPlusTree.Result> delegate = (PageHandler<Object, BPlusTree.Result>)hnd;

                return new PageHandler<Object, BPlusTree.Result>() {
                    @Override public BPlusTree.Result run(
                        int cacheId,
                        long pageId,
                        long page,
                        long pageAddr,
                        PageIO io,
                        Boolean walPlc,
                        Object arg,
                        int intArg,
                        IoStatisticsHolder statHolder
                    ) throws IgniteCheckedException {
                        BPlusTree.Result res =
                            delegate.run(cacheId, pageId, page, pageAddr, io, walPlc, arg, intArg, statHolder);

                        if (shouldThrow.get() && (tree instanceof InlineIndexTree))
                            throw new RuntimeException("test exception");

                        return res;
                    }
                };
            }
            else
                return hnd;
        };

        try {
            CountDownLatch latch = new CountDownLatch(1);

            FailureHandler failureHnd = (ignite, failureCtx) -> {
                latch.countDown();

                return false;
            };

            startGrid(createConfiguration(1, false).setFailureHandler(failureHnd));
            startGrid(createConfiguration(2, false).setFailureHandler(failureHnd));

            IgniteEx client = startGrid(createConfiguration(0, true));

            sql(client, "create table test (id integer primary key, val varchar)");
            sql(client, "create index test_id_idx on test (id)");

            awaitPartitionMapExchange(true, true, null);

            shouldThrow.set(true);

            List<String> sqls = F.asList(
                "select /*+ DISABLE_RULE('LogicalTableScanConverterRule') */ id from test where id > -10",
                "select /*+ DISABLE_RULE('LogicalTableScanConverterRule') */ max(id) from test where id > -10"
            );

            for (String sql : sqls) {
                GridTestUtils.assertThrowsWithCause(() -> sql(client, sql), CorruptedTreeException.class);

                assertTrue("Failure handler was not invoked", latch.await(5, TimeUnit.SECONDS));
            }
        }
        finally {
            BPlusTree.testHndWrapper = null;
        }
    }

    /** */
    private List<List<?>> sql(IgniteEx ignite, String sql, Object... args) {
        return ignite.context().query().querySqlFields(
            new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(args), false).getAll();
    }

    /** */
    private IgniteConfiguration createConfiguration(int id, boolean client) throws Exception {
        return getConfiguration(client ? "client" : "server-" + id).setClientMode(client);
    }
}
