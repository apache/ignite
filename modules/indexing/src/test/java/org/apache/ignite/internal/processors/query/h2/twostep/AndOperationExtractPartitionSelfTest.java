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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.h2.twostep.JoinSqlTestHelper.ORG;
import static org.apache.ignite.internal.processors.query.h2.twostep.JoinSqlTestHelper.ORG_COUNT;

/**
 * Partition pruning tests for AND operation.
 */
public class AndOperationExtractPartitionSelfTest extends AbstractIndexingCommonTest {
    /** */
    private static final int NODES_COUNT = 8;

    /** */
    private static IgniteCache<String, JoinSqlTestHelper.Organization> orgCache;

    /** */
    private static LongAdder cnt = new LongAdder();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            /** {@inheritDoc} */
            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                assert msg != null;

                if (GridIoMessage.class.isAssignableFrom(msg.getClass())) {
                    GridIoMessage gridMsg = (GridIoMessage)msg;

                    if (GridH2QueryRequest.class.isAssignableFrom(gridMsg.message().getClass()))
                        cnt.increment();
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_COUNT, false);

        orgCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, JoinSqlTestHelper.Organization>(ORG)
            .setCacheMode(CacheMode.PARTITIONED)
            .setQueryEntities(JoinSqlTestHelper.organizationQueryEntity())
        );

        awaitPartitionMapExchange();

        JoinSqlTestHelper.populateDataIntoOrg(orgCache);

        try (FieldsQueryCursor<List<?>> cur = orgCache.query(new SqlFieldsQuery(
            "SELECT * FROM Organization org WHERE org.id = '" + ORG + 0 + "'"))) {

            assert cur != null;

            List<List<?>> rows = cur.getAll();

            assert rows.size() == 1;
        }

        try (FieldsQueryCursor<List<?>> cur = orgCache.query(new SqlFieldsQuery(
            "SELECT * FROM Organization org WHERE org.id = ?").setArgs(ORG + 0))) {

            assert cur != null;

            List<List<?>> rows = cur.getAll();

            assert rows.size() == 1;
        }
    }

    /** */
    @Test
    public void testAlternativeUsageOfIn() {
        try (FieldsQueryCursor<List<?>> cur = orgCache.query(new SqlFieldsQuery(
            "SELECT * FROM Organization org WHERE org._KEY = 'org1' AND " +
                "org._KEY IN (SELECT subOrg._KEY FROM Organization subOrg)"))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());
        }
    }

    /** */
    @Test
    public void testEmptyList() {
        testAndOperator(Collections.emptyList(), null, 0L, NODES_COUNT - 1);
    }

    /** */
    @Test
    public void testSingleValueList() {
        testAndOperator(Collections.singletonList(ORG + 0), null, 0L, 0);
        testAndOperator(Collections.singletonList(ORG + 1), null, 1L, 1);
        testAndOperator(Collections.singletonList("ORG"), null, 0L, 0);
        testAndOperator(Collections.singletonList("?"), new String[] {ORG + 0}, 0L, 0);
        testAndOperator(Collections.singletonList("?"), new String[] {ORG + 2}, 1L, 1);
        testAndOperator(Collections.singletonList("?"), new String[] {"ORG"}, 0L, 0);
        testBothSidesParameterized(Collections.singletonList("?"), new String[] {"ORG", ORG + 1}, 0L, 0);
        testBothSidesParameterized(Collections.singletonList("?"), new String[] {"org2", ORG + 1}, 1L, 1);
    }

    /** */
    @Test
    public void testMultipleValueList() {
        testAndOperator(Arrays.asList(ORG + 0, ORG + 3, ORG + String.valueOf(ORG_COUNT - 1)), null, 1, 1);
        testAndOperator(Arrays.asList("ORG", ORG + 0, ORG + 4, ORG + String.valueOf(ORG_COUNT - 1)), null, 0, 0);
        testAndOperator(Arrays.asList(ORG + 0, ORG + 2, ORG + String.valueOf(ORG_COUNT - 1), "ORG"), null, 1, 1);
        testAndOperator(Arrays.asList(ORG + 0, ORG + 1, "MID", ORG + String.valueOf(ORG_COUNT - 1), ORG + 3), null, 2, 2);

        final List<String> allArgs3 = Arrays.asList("?", "?", "?");
        final List<String> allArgs4 = Arrays.asList("?", "?", "?", "?");

        testAndOperator(allArgs3, new String[] {ORG + 0, ORG + 1, ORG + String.valueOf(ORG_COUNT - 1)}, 1, 1);
        testAndOperator(allArgs4, new String[] {"ORG", ORG + 2, ORG + 8, ORG + String.valueOf(ORG_COUNT - 1)}, 1, 1);
        testAndOperator(allArgs4, new String[] {ORG + 1, ORG + 3, ORG + String.valueOf(ORG_COUNT - 1), "ORG"}, 2, 2);
        testAndOperator(allArgs4, new String[] {ORG + 0, "MID", ORG + String.valueOf(ORG_COUNT - 1), "ORG"}, 0, 0);

        testAndOperator(
            Arrays.asList("?", ORG + 2, ORG + String.valueOf(ORG_COUNT - 1), "?"),
            new String[] {ORG + 1, "ORG"},
            2,
            2
        );

        testAndOperator(
            Arrays.asList("?", ORG + 9, ORG + String.valueOf(ORG_COUNT - 1), "?"),
            new String[] {ORG + 1, "ORG"},
            1,
            1
        );
        testAndOperator(
            Arrays.asList("?", "?", ORG + String.valueOf(ORG_COUNT - 1), "ORG"),
            new String[] {ORG + 0, "MID"},
            0,
            0
        );

        testBothSidesParameterized(allArgs3, new String[] {ORG + 0, ORG + 1, ORG + String.valueOf(ORG_COUNT - 1), ORG + 1}, 1, 1);
        testBothSidesParameterized(allArgs4, new String[] {"ORG", ORG + 2, ORG + 8, ORG + String.valueOf(ORG_COUNT - 1), ORG + 1}, 1, 1);
        testBothSidesParameterized(allArgs4, new String[] {ORG + 1, ORG + 3, ORG + String.valueOf(ORG_COUNT - 1), "ORG", ORG + 1}, 2, 2);
        testBothSidesParameterized(allArgs4, new String[] {ORG + 0, "MID", ORG + String.valueOf(ORG_COUNT - 1), "ORG", ORG + 1}, 0, 0);

        testAndOperator(
            Arrays.asList("?", ORG + 10, ORG + String.valueOf(ORG_COUNT - 1), "?"),
            new String[] {ORG + 1, ORG + 2},
            2,
            2
        );

        testBothSidesParameterized(
            Arrays.asList("?", ORG + 9, ORG + String.valueOf(ORG_COUNT - 1), "?"),
            new String[] {ORG + 2, "ORG", ORG + 1},
            1,
            1
        );
        testBothSidesParameterized(
            Arrays.asList("?", "?", ORG + String.valueOf(ORG_COUNT - 1), "ORG"),
            new String[] {ORG + 0, "MID", ORG + 1},
            0,
            0
        );
    }

    /**
     *
     * @param cnst Constants and parameters('?').
     * @param args Values of parameters.
     * @param expRes Expected result.
     * @param maxReq Maximum number of requests to process query.
     */
    private void testAndOperator(List<String> cnst, Object[] args, long expRes, int maxReq) {
        int curIdx = cnt.intValue();

        String toIn = cnst.isEmpty() ? "" : String.valueOf("'" + String.join("','", cnst) + "'")
            .replace("'?'", "?");

        try (FieldsQueryCursor<List<?>> cur = orgCache.query(new SqlFieldsQuery(
            "SELECT * FROM Organization org WHERE org._KEY IN (" + toIn + ") AND org._KEY IN ('org1','org2','org3')").setArgs(args))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(expRes, rows.size());

            assertTrue(cnt.intValue() - curIdx <= maxReq);
        }
    }

    /**
     *
     * @param cnst Constants and parameters('?').
     * @param args Values of parameters.
     * @param expRes Expected result.
     * @param maxReq Maximum number of requests to process query.
     */
    private void testBothSidesParameterized(List<String> cnst, Object[] args, long expRes, int maxReq) {
        int curIdx = cnt.intValue();

        String toIn = cnst.isEmpty() ? "" : String.valueOf("'" + String.join("','", cnst) + "'")
            .replace("'?'", "?");

        try (FieldsQueryCursor<List<?>> cur = orgCache.query(new SqlFieldsQuery(
            "SELECT * FROM Organization org WHERE org._KEY IN (" + toIn + ") AND org._KEY IN (?,'org2','org3')").setArgs(args))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(expRes, rows.size());

            assertTrue(cnt.intValue() - curIdx <= maxReq);
        }
    }
}

