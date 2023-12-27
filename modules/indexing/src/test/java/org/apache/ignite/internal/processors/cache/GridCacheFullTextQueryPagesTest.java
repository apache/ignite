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

package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.lang.IgnitePredicate;
import org.junit.Test;

/**
 * Test pages loading for text queries tests.
 */
public class GridCacheFullTextQueryPagesTest extends GridCacheFullTextQueryAbstractTest {
    /** Cache size. */
    private static final int MAX_ITEM_COUNT = 10_000;

    /** Nodes count. */
    private static final int NODES = 4;

    /** Query page size. */
    private static final int PAGE_SIZE = 100;

    /** Limitation to query response size */
    private static final int QUERY_LIMIT = 100;

    /** Client node to start query. */
    private static IgniteEx client;

    /** */
    private static TestRecordingCommunicationSpi spi;

    /** */
    private static int dataCnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODES);

        client = startClientGrid();

        populateCache(client, MAX_ITEM_COUNT, (IgnitePredicate<Integer>)x -> String.valueOf(x).startsWith("1"));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        spi = TestRecordingCommunicationSpi.spi(client);

        spi.record((n, m) -> m instanceof GridCacheQueryRequest);
    }

    /** Test that there is no cancel queries and number of requests corresponds to count of data rows on remote nodes. */
    @Test
    public void testTextQueryMultiplePagesNoLimit() {
        checkTextQuery("1*", 0, PAGE_SIZE);

        // Send 2 additional load requests on each node.
        checkPages(NODES, NODES * 2, 0);
    }

    /** Test that do not send cache page request after limit exceeded. */
    @Test
    public void testTextQueryLimitedMultiplePages() {
        checkTextQuery("1*", QUERY_LIMIT, 30);

        // We hold 2 pre-loaded pages per node. Then we send additional load request for every node on beginning,
        // and 2 more while iterating over data and finish preloaded pages (depends on page size).
        checkPages(NODES, NODES + 2, NODES);
    }

    /** Test that rerequest some pages but then send a cancel query after limit exceeded. */
    @Test
    public void testTextQueryHighLimitedMultiplePages() {
        checkTextQuery("1*", QUERY_LIMIT, 20);

        // We hold 2 pre-loaded pages per node. Then we send additional load request for every node on beginning,
        // and 4 more while iterating over data and finish preloaded pages (depends on page size).
        checkPages(NODES, NODES + 4, NODES);
    }

    /** */
    private void checkPages(int expInitCnt, int expLoadCnt, int expCancelCnt) {
        List<Object> msgs = spi.recordedMessages(true);

        for (Object msg: msgs) {
            GridCacheQueryRequest req = (GridCacheQueryRequest)msg;

            if (req.cancel())
                expCancelCnt--;
            else if (req.clause() != null)
                expInitCnt--;
            else
                expLoadCnt--;
        }

        assertEquals(0, expInitCnt);
        assertEquals(0, expLoadCnt);
        assertEquals(0, expCancelCnt);
    }

    /**
     * Fill cache.
     *
     * @throws IgniteCheckedException if failed.
     */
    private void populateCache(IgniteEx ignite, int cnt, IgnitePredicate<Integer> expectedEntryFilter) throws IgniteCheckedException {
        IgniteInternalCache<Integer, Person> cache = ignite.cachex(PERSON_CACHE);

        Affinity<Integer> aff = cache.affinity();

        Map<UUID, Integer> nodeToCnt = new HashMap<>();

        Set<String> vals = new HashSet<>();

        for (int i = 0; i < cnt; i++) {
            cache.put(i, new Person(String.valueOf(i)));

            if (expectedEntryFilter.apply(i)) {
                vals.add(String.valueOf(i));

                UUID nodeId = aff.mapKeyToNode(i).id();

                if (nodeId.equals(ignite.localNode().id()))
                    continue;

                nodeToCnt.putIfAbsent(nodeId, 0);

                nodeToCnt.compute(nodeId, (k, v) -> v + 1);
            }
        }

        dataCnt = vals.size();
    }

    /**
     * @param clause Query clause.
     * @param limit limits response size.
     */
    private void checkTextQuery(String clause, int limit, int pageSize) {
        TextQuery<Integer, Person> qry = new TextQuery<Integer, Person>(Person.class, clause)
            .setLimit(limit).setPageSize(pageSize);

        IgniteCache<Integer, Person> cache = client.cache(PERSON_CACHE);

        List<Cache.Entry<Integer, Person>> result = cache.query(qry).getAll();

        int expRes = qry.getLimit() == 0 ? dataCnt : qry.getLimit();

        assertEquals(expRes, result.size());
    }
}
