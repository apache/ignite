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

package org.apache.ignite.util;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.QueryMXBeanImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.metric.SqlViewExporterSpiTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.mxbean.QueryMXBean;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.queryProcessor;
import static org.apache.ignite.internal.util.lang.GridFunc.isEmpty;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class KillCommandTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    private static final int PAGE_SZ = 5;

    /** */
    private static final int  NODES_CNT = 3;

    /** @throws Exception If failed. */
    @Test
    public void testCancelScanQuery() throws Exception {
        injectTestSystemOut();

        IgniteEx ignite0 = startGrids(NODES_CNT);
        IgniteEx client = startClientGrid("client");

        ignite0.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);
        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        QueryCursor<Cache.Entry<Object, Object>> qry1 = cache.query(new ScanQuery<>().setPageSize(PAGE_SZ));
        Iterator<Cache.Entry<Object, Object>> iter1 = qry1.iterator();

        // Fetch first entry and therefore caching first page.
        assertNotNull(iter1.next());

        List<List<?>> scanQries0 = SqlViewExporterSpiTest.execute(ignite0,
            "SELECT ORIGIN_NODE_ID, CACHE_NAME, QUERY_ID FROM SYS.SCAN_QUERIES");

        assertEquals(1, scanQries0.size());

        QueryMXBean qryMBean = getMxBean(client.name(), "Query",
            QueryMXBeanImpl.class.getSimpleName(), QueryMXBean.class);

        UUID originNodeId = (UUID)scanQries0.get(0).get(0);
        String cacheName = (String)scanQries0.get(0).get(1);
        long qryId = (Long)scanQries0.get(0).get(2);

        // Opens second query.
        QueryCursor<Cache.Entry<Object, Object>> qry2 = cache.query(new ScanQuery<>().setPageSize(PAGE_SZ));
        Iterator<Cache.Entry<Object, Object>> iter2 = qry2.iterator();

        // Fetch first entry and therefore caching first page.
        assertNotNull(iter2.next());

        // Cancel first query.
        qryMBean.cancelScan(originNodeId.toString(), cacheName, qryId);

        // Fetch all cached entries. It's size equal to the {@code PAGE_SZ}.
        for (int i = 0; i < PAGE_SZ * NODES_CNT - 1; i++)
            assertNotNull(iter1.next());

        // Fetch of the next page should throw the exception.
        assertThrowsWithCause(iter1::next, IgniteCheckedException.class);

        // Checking that second query works fine after canceling first.
        for (int i = 0; i < PAGE_SZ * PAGE_SZ - 1; i++)
            assertNotNull(iter2.next());

        // Checking all server node objects cleared after cancel.
        for (int i=0; i<NODES_CNT; i++) {
            IgniteEx ignite = grid(i);

            int cacheId = CU.cacheId(DEFAULT_CACHE_NAME);

            GridCacheContext<?, ?> ctx = ignite.context().cache().context().cacheContext(cacheId);

            ConcurrentMap<UUID, ? extends GridCacheQueryManager<?, ?>.RequestFutureMap> qryIters =
                ctx.queries().queryIterators();

            assertTrue(qryIters.size() <= 1);

            if (qryIters.size() == 0)
                return;

            GridCacheQueryManager<?, ?>.RequestFutureMap futs = qryIters.get(client.localNode().id());

            assertNotNull(futs);
            assertFalse(futs.containsKey(qryId));
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelSQLQuery() throws Exception {
        IgniteEx ignite0 = startGrids(NODES_CNT);
        IgniteEx client = startClientGrid("client");

        ignite0.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, Integer.class));

        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT _KEY, _VAL FROM INTEGER")
            .setSchema("default")
            .setPageSize(PAGE_SZ);

        Iterator<List<?>> iter = queryProcessor(client).querySqlFields(qry, true).iterator();

        List row = iter.next();

        assertNotNull(row);
        assertFalse(isEmpty(row));

        List<List<?>> sqlQries0 = SqlViewExporterSpiTest.execute(ignite0, "SELECT QUERY_ID FROM SYS.SQL_QUERIES");

        assertEquals(1, sqlQries0.size());

        QueryMXBean qryMBean = getMxBean(client.name(), "Query",
            QueryMXBeanImpl.class.getSimpleName(), QueryMXBean.class);

        String qryId = (String)sqlQries0.get(0).get(0);
        SqlViewExporterSpiTest.execute(ignite0, "KILL QUERY " + qryId);

//        qryMBean.cancelSQL(qryId);

        while(iter.hasNext()) {
            row = iter.next();
            assertNotNull(row);
            assertFalse(isEmpty(row));
        }

        fail("You shouldn't be here!");
    }
}
