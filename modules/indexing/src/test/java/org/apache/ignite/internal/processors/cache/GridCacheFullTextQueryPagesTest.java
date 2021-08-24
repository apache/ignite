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

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.GridCacheDistributedQueryManager;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test pages loading for text queries tests.
 */
public class GridCacheFullTextQueryPagesTest extends GridCommonAbstractTest {
    /** Cache size. */
    private static final int MAX_ITEM_COUNT = 10_000;

    /** Nodes count. */
    private static final int NODES = 4;

    /** Cache name */
    private static final String PERSON_CACHE = "Person";

    /** Query page size. */
    private static final int PAGE_SIZE = 100;

    /** Limitation to query response size */
    private static final int QUERY_LIMIT = 100;

    /** Client node to start query. */
    private static IgniteEx client;

    /** */
    private static TestStats testStats;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Person> cacheCfg = new CacheConfiguration<Integer, Person>()
            .setName(PERSON_CACHE)
            .setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODES);

        client = startClientGrid();

        testStats = new TestStats();

        populateCache(client, MAX_ITEM_COUNT, (IgnitePredicate<Integer>)x -> String.valueOf(x).startsWith("1"));
    }

    /** Test that there is no cancel queries and number of requests corresponds to count of data rows on remote nodes. */
    @Test
    public void testTextQueryMultiplePagesNoLimit() throws Exception {
        CountDownLatch latch = new CountDownLatch(testStats.nodesPagesCnt);

        PageStats stats = cacheQueryRequestsCount(latch);

        checkTextQuery("1*", 0, PAGE_SIZE);

        latch.await(1, TimeUnit.SECONDS);

        assertEquals(testStats.nodesPagesCnt, stats.totalReq());
    }

    /** Test that do not send cache page request after limit exceeded. */
    @Test
    public void testTextQueryLimitedMultiplePages() throws Exception {
        CountDownLatch latch = new CountDownLatch(14);

        PageStats stats = cacheQueryRequestsCount(latch);

        checkTextQuery("1*", QUERY_LIMIT, 30);

        // 2 additional load pages due to PriorityQueue algorithm of poll/add - in case of equal score for all data
        // it iterates over 2 streams only (first and last in the queue).
        checkPages(latch, stats, 4, 6, 4);
    }

    /** Test that rerequest some pages but then send a cancel query after limit exceeded. */
    @Test
    public void testTextQueryHighLimitedMultiplePages() throws Exception {
        CountDownLatch latch = new CountDownLatch(16);

        PageStats stats = cacheQueryRequestsCount(latch);

        checkTextQuery("1*", QUERY_LIMIT, 20);

        // 4 additional load pages due to PriorityQueue algorithm of poll/add - in case of equal score for all data
        // it iterates over 2 streams only (first and last in the queue).
        checkPages(latch, stats, 4, 8, 4);
    }

    /** */
    private void checkPages(CountDownLatch pagesLatch, PageStats stats,
        int expInitCnt, int expLoadCnt, int expCancelCnt) throws Exception {

        pagesLatch.await(1, TimeUnit.SECONDS);

        assertEquals(expInitCnt, stats.initCnt.get());
        assertEquals(expLoadCnt, stats.loadCnt.get());
        assertEquals(expCancelCnt, stats.cancelCnt.get());
    }

    /** */
    private PageStats cacheQueryRequestsCount(CountDownLatch allReqLatch) {
        PageStats stats = new PageStats();

        for (int i = 0; i < NODES; i++) {
            IgniteEx node = grid(i);

            GridCacheContext cctx = node.cachex(PERSON_CACHE).context();

            cctx.io().removeCacheHandlers(cctx.cacheId());

            cctx.io().addCacheHandler(cctx.cacheId(), GridCacheQueryRequest.class, new CI2<UUID, GridCacheQueryRequest>() {
                @Override public void apply(UUID nodeId, GridCacheQueryRequest req) {
                    if (req.cancel())
                        stats.cancelCnt.incrementAndGet();
                    else if (req.limit() != 0)
                        stats.initCnt.incrementAndGet();
                    else
                        stats.loadCnt.incrementAndGet();

                    allReqLatch.countDown();

                    ((GridCacheDistributedQueryManager) cctx.queries()).processQueryRequest(nodeId, req);
                }
            });
        }

        return stats;
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

        for (UUID nodeId: nodeToCnt.keySet()) {
            int rowsCnt = nodeToCnt.get(nodeId);

            int pagesCnt = rowsCnt / PAGE_SIZE + (rowsCnt % PAGE_SIZE == 0 ? 0 : 1);

            testStats.nodesPagesCnt += pagesCnt;
        }

        testStats.dataCnt = vals.size();
    }

    /**
     * @param clause Query clause.
     * @param limit limits response size.
     */
    private void checkTextQuery(String clause, int limit, int pageSize) {
        TextQuery<Integer, Person> qry = new TextQuery<Integer, Person>(Person.class, clause)
            .setLimit(limit).setPageSize(pageSize);

        validateQueryResults(qry);
    }

    /**
     * Check query results.
     */
    private void validateQueryResults(TextQuery<Integer, Person> qry) {
        IgniteCache<Integer, Person> cache = client.cache(PERSON_CACHE);

        List<Cache.Entry<Integer, Person>> result = cache.query(qry).getAll();

        int expRes = qry.getLimit() == 0 ? testStats.dataCnt : qry.getLimit();

        assertEquals(expRes, result.size());
    }

    /**
     * Test model class.
     */
    public static class Person implements Serializable {
        /** */
        @QueryTextField
        private final String name;

        /**
         * Constructor
         */
        public Person(String name) {
            this.name = name;
        }
    }

    /** */
    private static class PageStats {
        /** Counter of init requests. */
        private final AtomicInteger initCnt = new AtomicInteger();

        /** Counter of load page requests. */
        private final AtomicInteger loadCnt = new AtomicInteger();

        /** Counter of cancel query requests. */
        private final AtomicInteger cancelCnt = new AtomicInteger();

        /** */
        private int totalReq() {
            return initCnt.get() + loadCnt.get() + cancelCnt.get();
        }
    }

    /** */
    private static class TestStats {
        /** */
        private int nodesPagesCnt;

        /** */
        private int dataCnt;
    }
}
