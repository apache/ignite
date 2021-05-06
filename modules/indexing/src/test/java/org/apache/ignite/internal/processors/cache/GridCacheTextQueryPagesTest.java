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
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
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

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test pages loading for text queries tests.
 */
public class GridCacheTextQueryPagesTest extends GridCommonAbstractTest {
    /** Cache size. */
    private static final int MAX_ITEM_COUNT = 10_000;

    /** Cache name */
    private static final String PERSON_CACHE = "Person";

    /** Query page size. */
    private static final int PAGE_SIZE = 100;

    /** Limitation to query response size */
    private static final int QUERY_LIMIT = 100;

    /** */
    private int nodesPagesCnt;

    /** */
    private int dataCnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeEventTypes();

        cfg.setConnectorConfiguration(null);

        CacheConfiguration<Integer, Person> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(PERSON_CACHE)
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setBackups(0)
            .setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        int grids = 4;

        startGrids(grids);

        populateCache(grid(0), MAX_ITEM_COUNT, (IgnitePredicate<Integer>)x -> String.valueOf(x).startsWith("1"));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** Test that there is no cancel queries and number of requests corresponds to count of data rows on remote nodes. */
    @Test
    public void testTextQueryMultiplePagesNoLimit() throws Exception {
        CountDownLatch latch = new CountDownLatch(nodesPagesCnt);

        AtomicInteger cnt = cacheQueryRequestsCount(nodesPagesCnt + 1, latch);

        checkTextQuery("1*", 0, PAGE_SIZE);

        latch.await(1, TimeUnit.SECONDS);

        assertEquals(nodesPagesCnt, cnt.get());
    }

    /** Test that do not send cache page request after limit exceeded. */
    @Test
    public void testTextQueryLimitedMultiplePages() throws Exception {
        // There are 3 remote nodes, so there are requests: 3 on load pages, 3 on cancel query.
        int expCacheReqCnt = 6;

        CountDownLatch latch = new CountDownLatch(expCacheReqCnt);

        AtomicInteger cnt = cacheQueryRequestsCount(4, latch);

        checkTextQuery("1*", QUERY_LIMIT, 35);

        latch.await(1, TimeUnit.SECONDS);

        assertEquals(expCacheReqCnt, cnt.get());
    }

    /** Test that rerequest some pages but then send a cancel query after limit exceeded. */
    @Test
    public void testTextQueryHighLimitedMultiplePages() throws Exception {
        // There are 3 remote nodes, so there are requests: 6 on load pages, 3 on cancel query.
        int expCacheReqCnt = 9;

        CountDownLatch latch = new CountDownLatch(expCacheReqCnt);

        AtomicInteger cnt = cacheQueryRequestsCount(7, latch);

        checkTextQuery("1*", QUERY_LIMIT, 20);

        latch.await(1, TimeUnit.SECONDS);

        assertEquals(expCacheReqCnt, cnt.get());
    }

    /** */
    private AtomicInteger cacheQueryRequestsCount(int expCancelStartIdx, CountDownLatch allReqLatch) {
        AtomicInteger cacheQryReqCnt = new AtomicInteger();

        for (int i = 1; i < 4; i++) {
            IgniteEx node = grid(i);

            GridCacheContext cctx = node.cachex(PERSON_CACHE).context();

            cctx.io().removeCacheHandlers(cctx.cacheId());

            cctx.io().addCacheHandler(cctx.cacheId(), GridCacheQueryRequest.class, new CI2<UUID, GridCacheQueryRequest>() {
                @Override public void apply(UUID nodeId, GridCacheQueryRequest req) {
                    assertEquals(req.cancel(), cacheQryReqCnt.incrementAndGet() >= expCancelStartIdx);

                    allReqLatch.countDown();

                    ((GridCacheDistributedQueryManager) cctx.queries()).processQueryRequest(nodeId, req);
                }
            });
        }

        return cacheQryReqCnt;
    }

    /**
     * Fill cache.
     *
     * @throws IgniteCheckedException if failed.
     */
    void populateCache(IgniteEx ignite, int cnt, IgnitePredicate<Integer> expectedEntryFilter) throws IgniteCheckedException {
        IgniteInternalCache<Integer, Person> cache = ignite.cachex(PERSON_CACHE);

        assertNotNull(cache);

        Random rand = new Random();

        Affinity<Integer> aff = cache.affinity();

        Map<UUID, Integer> nodeToCnt = new HashMap<>();

        Set<String> vals = new HashSet<>();

        for (int i = 0; i < cnt; i++) {
            int val = rand.nextInt(cnt);

            cache.put(val, new Person(String.valueOf(val)));

            if (expectedEntryFilter.apply(val)) {
                boolean notContain = vals.add(String.valueOf(val));

                UUID nodeId = aff.mapKeyToNode(val).id();

                if (nodeId.equals(ignite.localNode().id()))
                    continue;

                nodeToCnt.putIfAbsent(nodeId, 0);

                if (notContain)
                    nodeToCnt.compute(nodeId, (k, v) -> v + 1);
            }
        }

        for (UUID nodeId: nodeToCnt.keySet()) {
            int rowsCnt = nodeToCnt.get(nodeId);

            int pagesCnt = rowsCnt / PAGE_SIZE + (rowsCnt % PAGE_SIZE == 0 ? 0 : 1);

            nodesPagesCnt += pagesCnt;
        }

        dataCnt = vals.size();
    }

    /**
     * @param clause Query clause.
     * @param limit limits response size.
     */
    private void checkTextQuery(String clause, int limit, int pageSize) {
        final IgniteEx ignite = grid(0);

        TextQuery qry = new TextQuery<>(Person.class, clause).setLimit(limit).setPageSize(pageSize);

        validateQueryResults(ignite, qry);
    }

    /**
     * Check query results.
     *
     * @throws IgniteCheckedException if failed.
     */
    private void validateQueryResults(IgniteEx ignite, TextQuery qry) {
        IgniteCache<Integer, Person> cache = ignite.cache(PERSON_CACHE);

        List result = cache.query(qry).getAll();

        int expRes = qry.getLimit() == 0 ? dataCnt : qry.getLimit();

        assertEquals(expRes, result.size());
    }

    /**
     * Test model class.
     */
    public static class Person implements Serializable {
        /** */
        @QueryTextField
        String name;

        /**
         * Constructor
         */
        public Person(String name) {
            this.name = name;
        }
    }
}
