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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test checks partition extraction for between (where x between 10 and 17) and simple range (where x > 10 and x < 17)
 * expressions.
 */
public class BetweenOperationExtractPartitionSelfTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_COUNT = 8;

    /** Organizations count. */
    private static final int ORG_COUNT = 10000;

    /** Organizations cache name. */
    private static final String ORG_CACHE_NAME = "orgBetweenTest";

    /** Empty partitions array. */
    private static final int[] EMPTY_PARTITIONS_ARRAY = new int[]{};

    /** Between query. */
    private static final String BETWEEN_QRY = "select * from Organization org where org._KEY between %d and %d";

    /** Range query. */
    private static final String RANGE_QRY = "select * from Organization org where org._KEY %s %d and org._KEY %s %d";

    /** And + between query. */
    private static final String AND_BETWEEN_QRY =
        "select * from Organization org where org._KEY > 10 and org._KEY between %d and %d";

    /** And + range query. */
    private static final String AND_RANGE_QRY =
        "select * from Organization org where org._KEY > 10 and org._KEY %s %d and org._KEY %s %d";

    /** Between + and query. */
    private static final String BETWEEN_AND_QRY =
        "select * from Organization org where org._KEY between %d and %d and org._KEY > 10";

    /** Range + and query. */
    private static final String RANGE_AND_QRY =
        "select * from Organization org where org._KEY %s %d and org._KEY %s %d and org._KEY > 10";

    /** Between + between query. */
    private static final String BETWEEN_AND_BETWEEN_QRY =
        "select * from Organization org where org._KEY between %d and %d and org._KEY between 10 and 20";

    /** Range + Range query. */
    private static final String RANGE_AND_RANGE_QRY =
        "select * from Organization org where org._KEY %s %d and org._KEY %s %d and org._KEY >= 10 and org._KEY <= 20";

    /** Between + and + between query. */
    private static final String BETWEEN_AND_AND_AND_BETWEEN_QRY =
        "select * from Organization org where org._KEY between %d and %d and org._KEY < 30 and org._KEY" +
            " between 10 and 20";

    /** Range + and + Range query. */
    private static final String RANGE_AND_AND_AND_RANGE_QRY =
        "select * from Organization org where org._KEY %s %d and org._KEY %s %d and org._KEY < 30 and" +
            " org._KEY >= 10 and org._KEY <= 20";

    /** Between + or query. */
    private static final String BETWEEN_OR_QRY =
        "select * from Organization org where org._KEY between %d and %d or org._KEY < 5";

    /** Range + or query. */
    private static final String RANGE_OR_QRY =
        "select * from Organization org where org._KEY %s %d and org._KEY %s %d or org._KEY < 5";

    /** Between + or query. */
    private static final String OR_BETWEEN_QRY =
        "select * from Organization org where org._KEY < 5 or org._KEY between %d and %d";

    /** Range + or query. */
    private static final String OR_RANGE_QRY =
        "select * from Organization org where org._KEY < 5 or org._KEY %s %d and org._KEY %s %d";

    /** Between or between query. */
    private static final String BETWEEN_OR_BETWEEN_QRY =
        "select * from Organization org where org._KEY between %d and %d or org._KEY between 20 and 25";

    /** Range or range query. */
    private static final String RANGE_OR_RANGE_QRY =
        "select * from Organization org where org._KEY %s %d and org._KEY %s %d or org._KEY >= 20 and org._KEY <= 25";

    /** Range or range query. */
    private static final String RANGE_OR_BETWEEN_QRY =
        "select * from Organization org where org._KEY %s %d and org._KEY %s %d or org._KEY between 20 and 25";

    /** Empty Range. */
    private static final String EMPTY_RANGE_QRY =
        "select * from Organization org where org._KEY %s %d and org._KEY %s %d";

    /** Organizations cache. */
    private static IgniteCache<Integer, JoinSqlTestHelper.Organization> orgCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new BetweenOperationExtractPartitionSelfTest.TestCommunicationSpi());

        return cfg;
    }

    /**
     * @return Query entity for Organization.
     */
    private static Collection<QueryEntity> organizationQueryEntity() {
        QueryEntity entity = new QueryEntity(Integer.class, JoinSqlTestHelper.Organization.class);

        entity.setKeyFieldName("ID");
        entity.getFields().put("ID", String.class.getName());

        return Collections.singletonList(entity);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_COUNT - 1, false);

        startClientGrid(NODES_COUNT);

        orgCache = ignite(NODES_COUNT).getOrCreateCache(new CacheConfiguration<Integer, JoinSqlTestHelper.Organization>(ORG_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setQueryEntities(organizationQueryEntity())
        );

        awaitPartitionMapExchange();

        populateDataIntoOrg();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        orgCache = null;

        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Check between expression with constant values.
     */
    @Test
    public void testBetweenConst() {
        // select * from Organization org where org._KEY between %d and %d.
        //
        //     between
        //
        testBetweenConstOperator(BETWEEN_QRY, 1, 3, 3);
        testBetweenConstOperator(BETWEEN_QRY, 5, 5, 1);
        testBetweenConstOperator(BETWEEN_QRY, 7, 8, 2);

        // select * from Organization org where org._KEY > 10 and org._KEY between %d and %d
        //
        //       and
        //      /   \
        //     /     \
        //    >    between
        //
        testBetweenConstOperator(AND_BETWEEN_QRY, 11, 13, 3);
        testBetweenConstOperator(AND_BETWEEN_QRY, 15, 15, 1);
        testBetweenConstOperator(AND_BETWEEN_QRY, 17, 18, 2);

        // select * from Organization org where org._KEY between %d and %d and org._KEY > 10
        //
        //       and
        //      /   \
        //     /     \
        // between    >
        //
        testBetweenConstOperator(BETWEEN_AND_QRY, 11, 13, 3);
        testBetweenConstOperator(BETWEEN_AND_QRY, 15, 15, 1);
        testBetweenConstOperator(BETWEEN_AND_QRY, 17, 18, 2);

        // select * from Organization org where org._KEY between %d and %d and org._KEY between 10 and 20
        //
        //       and
        //      /   \
        //     /     \
        // between between
        //
        testBetweenConstOperator(BETWEEN_AND_BETWEEN_QRY, 11, 13, 3);
        testBetweenConstOperator(BETWEEN_AND_BETWEEN_QRY, 15, 15, 1);
        testBetweenConstOperator(BETWEEN_AND_BETWEEN_QRY, 17, 18, 2);

        // select * from Organization org where org._KEY between %d and %d and org._KEY < 30
        // and org._KEY between 10 and 20
        //
        //       and
        //      /   \
        //     /     \
        // between   and
        //          /   \
        //         /     \
        //        <     between
        //
        testBetweenConstOperator(BETWEEN_AND_AND_AND_BETWEEN_QRY, 11, 13, 3);
        testBetweenConstOperator(BETWEEN_AND_AND_AND_BETWEEN_QRY, 15, 15, 1);
        testBetweenConstOperator(BETWEEN_AND_AND_AND_BETWEEN_QRY, 17, 18, 2);

        // select * from Organization org where org._KEY between %d and %d or org._KEY < 5
        //
        //        or
        //      /   \
        //     /     \
        // between    <
        //
        testBetweenConstOperator(BETWEEN_OR_QRY, 11, 13, 8, EMPTY_PARTITIONS_ARRAY);

        // select * from Organization org where org._KEY < 5 or org._KEY between %d and %d
        //
        //        or
        //      /   \
        //     /     \
        //    <     between
        //
        testBetweenConstOperator(OR_BETWEEN_QRY, 11, 13, 8, EMPTY_PARTITIONS_ARRAY);

        // select * from Organization org where org._KEY between %d and %d or between 20 and 25
        //
        //        or
        //      /   \
        //     /     \
        // between   between
        //
        testBetweenConstOperator(BETWEEN_OR_BETWEEN_QRY, 11, 13, 9,
            11, 12, 13, 20, 21, 22, 23, 24, 25);
    }

    /**
     * Check range expression with constant values.
     */
    @Test
    public void testRangeConst() {
        // select * from Organization org where org._KEY %s %d and org._KEY %s %d
        //
        //     >(=) <(=)
        //
        testRangeConstOperator(RANGE_QRY, 1, 3, 3, false);
        testRangeConstOperator(RANGE_QRY, 5, 5, 1, false);
        testRangeConstOperator(RANGE_QRY, 7, 8, 2, false);

        // At the moment between-based-partition-pruning doesn't support range expression
        // with any extra expressions because of optimisations that change expressions order:
        // org where org._KEY > 10 and org._KEY > 11 and org._KEY < 13 converts to
        // ((ORG__Z0._KEY < 13) AND ((ORG__Z0._KEY > 10) AND (ORG__Z0._KEY > 11)))
        // So bellow we only check expected result rows count and not expected partitions matching.

        // select * from Organization org where org._KEY > 10 and org._KEY %s %d and org._KEY %s %d
        //
        //       and
        //      /   \
        //     /     \
        //    >      and
        //          /   \
        //         /     \
        //        >(=)  <(=)
        //
        testRangeConstOperator(AND_RANGE_QRY, 11, 13, 3, true);
        testRangeConstOperator(AND_RANGE_QRY, 15, 15, 1, true);
        testRangeConstOperator(AND_RANGE_QRY, 17, 18, 2, true);

        // select * from Organization org where org._KEY %s %d and org._KEY %s %d and org._KEY > 10
        //
        //          and
        //         /   \
        //        /     \
        //      and      >
        //     /  \
        //    /    \
        //  >(=)   <(=)
        //
        testRangeConstOperator(RANGE_AND_QRY, 11, 13, 3, true);
        testRangeConstOperator(RANGE_AND_QRY, 15, 15, 1, true);
        testRangeConstOperator(RANGE_AND_QRY, 17, 18, 2, true);

        // select * from Organization org where org._KEY %s %d and org._KEY %s %d and org._KEY >= 10 and org._KEY <= 20
        //
        //          and
        //         /   \
        //        /     \
        //       /       \
        //     and       and
        //    /   \      /  \
        //   /     \    /    \
        // >(=)  <(=) >(=)  <(=)
        //
        testRangeConstOperator(RANGE_AND_RANGE_QRY, 11, 13, 3, true);
        testRangeConstOperator(RANGE_AND_RANGE_QRY, 15, 15, 1, true);
        testRangeConstOperator(RANGE_AND_RANGE_QRY, 17, 18, 2, true);

        // select * from Organization org where org._KEY %s %d and org._KEY %s %d and org._KEY < 30 and
        // org._KEY >= 10 and org._KEY <= 20
        //
        //          and
        //         /   \
        //        /     \
        //       /       \
        //     and       and
        //    /   \      /  \
        //   /     \    /    \
        // >(=)  <(=) >(=)  and
        //                 /   \
        //                /     \
        //               >(=)   <(=)
        //
        testRangeConstOperator(RANGE_AND_AND_AND_RANGE_QRY, 11, 13, 3, true);
        testRangeConstOperator(RANGE_AND_AND_AND_RANGE_QRY, 15, 15, 1, true);
        testRangeConstOperator(RANGE_AND_AND_AND_RANGE_QRY, 17, 18, 2, true);

        // select * from Organization org where org._KEY %s %d and org._KEY %s %d or org._KEY < 5
        //
        //           or
        //         /   \
        //        /     \
        //      and      <
        //     /  \
        //    /    \
        //  >(=)   <(=)
        //
        testRangeConstOperator(RANGE_OR_QRY, 11, 13, ">", "<", 6,
            EMPTY_PARTITIONS_ARRAY);
        testRangeConstOperator(RANGE_OR_QRY, 11, 13, ">=", "<", 7,
            EMPTY_PARTITIONS_ARRAY);
        testRangeConstOperator(RANGE_OR_QRY, 11, 13, ">", "<=", 7,
            EMPTY_PARTITIONS_ARRAY);
        testRangeConstOperator(RANGE_OR_QRY, 11, 13, ">=", "<=", 8,
            EMPTY_PARTITIONS_ARRAY);

        // select * from Organization org where org._KEY < 5 or org._KEY %s %d and org._KEY %s %d
        //
        //       and
        //      /   \
        //     /     \
        //    <      and
        //          /   \
        //         /     \
        //        >(=)  <(=)
        //
        testRangeConstOperator(OR_RANGE_QRY, 11, 13, ">", "<", 6,
            EMPTY_PARTITIONS_ARRAY);
        testRangeConstOperator(OR_RANGE_QRY, 11, 13, ">=", "<", 7,
            EMPTY_PARTITIONS_ARRAY);
        testRangeConstOperator(OR_RANGE_QRY, 11, 13, ">", "<=", 7,
            EMPTY_PARTITIONS_ARRAY);
        testRangeConstOperator(OR_RANGE_QRY, 11, 13, ">=", "<=", 8,
            EMPTY_PARTITIONS_ARRAY);

        // select * from Organization org where org._KEY %s %d and org._KEY %s %d or org._KEY >= 20 and org._KEY <= 25
        //
        //           or
        //         /   \
        //        /     \
        //       /       \
        //     and       and
        //    /   \      /  \
        //   /     \    /    \
        // >(=)  <(=) >(=)  <(=)
        //
        testRangeConstOperator(RANGE_OR_RANGE_QRY, 11, 13, ">", "<", 7,
            12, 20, 21, 22, 23, 24, 25);
        testRangeConstOperator(RANGE_OR_RANGE_QRY, 11, 13, ">=", "<", 8,
            11, 12, 20, 21, 22, 23, 24, 25);
        testRangeConstOperator(RANGE_OR_RANGE_QRY, 11, 13, ">", "<=", 8,
            12, 13, 20, 21, 22, 23, 24, 25);
        testRangeConstOperator(RANGE_OR_RANGE_QRY, 11, 13, ">=", "<=", 9,
            11, 12, 13, 20, 21, 22, 23, 24, 25);

        // select * from Organization org where org._KEY %s %d and org._KEY %s %d or org._KEY between 20 and 25
        //
        //           or
        //         /   \
        //        /     \
        //       /       \
        //     and       and
        //    /   \      /  \
        //   /     \    /    \
        // >(=)  <(=) >(=)  <(=)
        //
        testRangeConstOperator(RANGE_OR_BETWEEN_QRY, 11, 13, ">", "<", 7,
            12, 20, 21, 22, 23, 24, 25);
        testRangeConstOperator(RANGE_OR_BETWEEN_QRY, 11, 13, ">=", "<", 8,
            11, 12, 20, 21, 22, 23, 24, 25);
        testRangeConstOperator(RANGE_OR_BETWEEN_QRY, 11, 13, ">", "<=", 8,
            12, 13, 20, 21, 22, 23, 24, 25);
        testRangeConstOperator(RANGE_OR_BETWEEN_QRY, 11, 13, ">=", "<=", 9,
            11, 12, 13, 20, 21, 22, 23, 24, 25);

        // select * from Organization org where org._KEY < %d and org._KEY > %d
        //
        // Empty range < >
        //
        testRangeConstOperator(EMPTY_RANGE_QRY, 11, 13, "<", ">", 0,
            EMPTY_PARTITIONS_ARRAY);
    }

    /**
     * Check between expression against non-affinity column.
     */
    @Test
    public void testBetweenConstAgainstNonAffinityColumn() {
        testBetweenConstOperator("select * from Organization org where org.debtCapital between %d and %d",
            1, 3, 3, EMPTY_PARTITIONS_ARRAY);
    }

    /**
     * Check range expression against different columns.
     */
    @Test
    public void testBetweenConstAgainstDifferentColumns() {
        testRangeConstOperator("select * from Organization org where org._key %s %d and org.debtCapital %s %d",
            1, 3, ">=", "<=", 3, EMPTY_PARTITIONS_ARRAY);
    }

    /**
     * Check default partitions limit exceeding.
     */
    @Test
    public void testBetweenPartitionsDefaultLimitExceeding() {
        // Default limit (16) not exceeded.
        testBetweenConstOperator(BETWEEN_QRY, 1, 16, 16);

        // Default limit (16) exceeded.
        testBetweenConstOperator(BETWEEN_QRY, 1, 17, 17, EMPTY_PARTITIONS_ARRAY);
    }

    /**
     * Check range expression with constant values.
     */
    @Test
    public void testRevertedRangeConst() {
        // select * from Organization org where org._KEY %s %d and org._KEY %s %d
        //
        //     <(=) >(=)
        //
        testRevertedRangeConstOperator(3, 1, 3);
        testRevertedRangeConstOperator(5, 5, 1);
        testRevertedRangeConstOperator(8, 7, 2);
    }

    /**
     * Check that given sql query with between expression returns expect rows count and that expected partitions set
     * matches used one.
     *
     * @param sqlQry SQL query
     * @param from Between from const.
     * @param to Between to const.
     * @param expResCnt Expected result rows count.
     */
    private void testBetweenConstOperator(String sqlQry, int from, int to, int expResCnt) {
        TestCommunicationSpi commSpi = runQuery(sqlQry, from, to, expResCnt);

        assertEquals(extractExpectedPartitions(from, to), commSpi.partitionsSet());
    }

    /**
     * Check that given sql query with between expression returns expect rows count and that expected partitions set
     * matches used one.
     *
     * @param sqlQry SQL query
     * @param from Between from const.
     * @param to Between to const.
     * @param expResCnt Expected result rows count.
     * @param expPartitions Expected partitions.
     */
    private void testBetweenConstOperator(String sqlQry, int from, int to, int expResCnt, int... expPartitions) {
        TestCommunicationSpi commSpi = runQuery(sqlQry, from, to, expResCnt);

        Set<Integer> expPartitionsSet = new HashSet<>();

        for (int expPartition: expPartitions)
            expPartitionsSet.add(expPartition);

        assertEquals(expPartitionsSet, commSpi.partitionsSet());
    }

    /**
     * Check that given sql query with between expression returns expect rows count and that expected partitions set
     * matches used one.
     *
     * @param sqlQry SQL query
     * @param const1 Range const1 const.
     * @param const2 Range const2 const.
     * @param expResCnt Expected result rows count.
     * @param skipPartitionsCheck Skip partitions matching check.
     */
    private void testRangeConstOperator(String sqlQry, int const1, int const2, int expResCnt,
        boolean skipPartitionsCheck) {
        // Range: > <.
        TestCommunicationSpi commSpi = runQuery(sqlQry, const1, const2, ">", "<",
            expResCnt - 2);

        if (!skipPartitionsCheck)
            assertEquals(extractExpectedPartitions(const1 + 1, const2 - 1), commSpi.partitionsSet());

        // Range: >= <.
        commSpi = runQuery(sqlQry, const1, const2, ">=", "<", expResCnt - 1);

        if (!skipPartitionsCheck)
            assertEquals(extractExpectedPartitions(const1, const2 - 1), commSpi.partitionsSet());

        // Range: > <=.
        commSpi = runQuery(sqlQry, const1, const2, ">", "<=", expResCnt - 1);

        if (!skipPartitionsCheck)
            assertEquals(extractExpectedPartitions(const1 + 1, const2), commSpi.partitionsSet());

        // Range: >= <=.
        commSpi = runQuery(sqlQry, const1, const2, ">=", "<=", expResCnt);

        if (!skipPartitionsCheck)
            assertEquals(extractExpectedPartitions(const1, const2), commSpi.partitionsSet());
    }

    /**
     * Check that given sql query with reverted range expression returns expect rows count and that expected partitions
     * set matches used one.
     *
     * @param const1 Range const1 const.
     * @param const2 Range const2 const.
     * @param expResCnt Expected result rows count.
     */
    private void testRevertedRangeConstOperator(int const1, int const2, int expResCnt) {
        // Range: < >.
        TestCommunicationSpi commSpi = runQuery(RANGE_QRY, const1, const2, "<", ">",
            expResCnt - 2);

        assertEquals(extractExpectedPartitions(const2 + 1, const1 - 1), commSpi.partitionsSet());

        // Range: <= >.
        commSpi = runQuery(RANGE_QRY, const1, const2, "<=", ">", expResCnt - 1);

        assertEquals(extractExpectedPartitions(const2 + 1, const1), commSpi.partitionsSet());

        // Range: < >=.
        commSpi = runQuery(RANGE_QRY, const1, const2, "<", ">=", expResCnt - 1);

        assertEquals(extractExpectedPartitions(const2, const1 - 1), commSpi.partitionsSet());

        // Range: <= >=.
        commSpi = runQuery(RANGE_QRY, const1, const2, "<=", ">=", expResCnt);

        assertEquals(extractExpectedPartitions(const2, const1), commSpi.partitionsSet());
    }

    /**
     * Check that given sql query with range expression returns expect rows count and that expected partitions set
     * matches used one.
     *
     * @param sqlQry SQL query
     * @param from Range from const.
     * @param to Range to const.
     * @param expResCnt Expected result rows count.
     * @param expPartitions Expected partitions.
     */
    private void testRangeConstOperator(String sqlQry, int from, int to, String leftOp, String rightOp,
        int expResCnt, int... expPartitions) {
        TestCommunicationSpi commSpi = runQuery(sqlQry, from, to, leftOp, rightOp, expResCnt);

        assertEquals(Arrays.stream(expPartitions).boxed().collect(Collectors.toSet()), commSpi.partitionsSet());
    }

    /**
     * Runs query and checks that given sql query with between expression returns expect rows count.
     *
     * @param sqlQry SQL query
     * @param from Between from const.
     * @param to Between to const.
     * @param expResCnt Expected result rows count.
     * @return Communication SPI for further assertions.
     */
    private BetweenOperationExtractPartitionSelfTest.TestCommunicationSpi runQuery(String sqlQry, int from, int to,
        int expResCnt) {
        TestCommunicationSpi commSpi =
            (TestCommunicationSpi)grid(NODES_COUNT).configuration().
                getCommunicationSpi();

        commSpi.resetPartitions();

        try (FieldsQueryCursor<List<?>> cur = orgCache.query(new SqlFieldsQuery(String.format(sqlQry, from, to)))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(expResCnt, rows.size());
        }
        return commSpi;
    }

    /**
     * Runs query and checks that given sql query with between expression returns expect rows count.
     *
     * @param sqlQry SQL query
     * @param from Between from const.
     * @param to Between to const.
     * @param expResCnt Expected result rows count.
     * @return Communication SPI for further assertions.
     */
    private BetweenOperationExtractPartitionSelfTest.TestCommunicationSpi runQuery(String sqlQry, int from, int to,
        String leftRangeOperand, String rightRangeOperand, int expResCnt) {

        TestCommunicationSpi commSpi =
            (TestCommunicationSpi)grid(NODES_COUNT).configuration().
                getCommunicationSpi();

        commSpi.resetPartitions();

        try (FieldsQueryCursor<List<?>> cur = orgCache.query(new SqlFieldsQuery(String.format(sqlQry,
            leftRangeOperand, from, rightRangeOperand, to)))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(Math.max(expResCnt, 0), rows.size());
        }

        return commSpi;
    }

    /**
     * Extract expected partitions set from between from/to keys.
     *
     * @param keyFrom Key from.
     * @param keyTo Key to.
     * @return Expected set of partitions.
     */
    private Set<Integer> extractExpectedPartitions(int keyFrom, int keyTo) {
        Set<Integer> partitions = new HashSet<>();

        for (int i = keyFrom; i <= keyTo; i++)
            partitions.add(ignite(0).affinity(ORG_CACHE_NAME).partition(i));

        return partitions;
    }

    /**
     * Populate organization cache with test data.
     */
    private void populateDataIntoOrg() {
        for (int i = 0; i < ORG_COUNT; i++) {
            JoinSqlTestHelper.Organization org = new JoinSqlTestHelper.Organization();

            org.setName("Organization #" + i);
            org.debtCapital(i);

            orgCache.put(i, org);
        }
    }

    /**
     * Test communication SPI.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Used partitions. */
        Set<Integer> partitions = ConcurrentHashMap.newKeySet();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {

            if (((GridIoMessage)msg).message() instanceof GridH2QueryRequest) {
                GridH2QueryRequest gridH2QryReq = (GridH2QueryRequest)((GridIoMessage)msg).message();

                if (gridH2QryReq.queryPartitions() != null) {
                    for (int partition : gridH2QryReq.queryPartitions())
                        partitions.add(partition);
                }
            }

            super.sendMessage(node, msg, ackC);
        }

        /**
         * @return Used partitons set.
         */
        Set<Integer> partitionsSet() {
            return partitions;
        }

        /**
         * Clear partitions set.
         */
        void resetPartitions() {
            partitions.clear();
        }
    }
}
