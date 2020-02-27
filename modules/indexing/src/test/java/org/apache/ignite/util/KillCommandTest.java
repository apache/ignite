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
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.metric.SqlViewExporterSpiTest;
import org.apache.ignite.mxbean.QueryMXBean;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

public class KillCommandTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** @throws Exception If failed. */
    @Test
    public void testCancelSQLQuery() throws Exception {
        injectTestSystemOut();

        IgniteEx ignite0 = startGrids(1);

        Ignite client = startClientGrid("client");

        ignite0.cluster().state(ACTIVE);

        createCacheAndPreload(ignite0, 20);

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        QueryCursor<Cache.Entry<Object, Object>> qry1 = cache.query(new ScanQuery<>().setPageSize(10));
        Iterator<Cache.Entry<Object, Object>> iter1 = qry1.iterator();

        List<List<?>> scanQries0 = SqlViewExporterSpiTest.execute(ignite0, "SELECT ORIGIN_NODE_ID, QUERY_ID FROM SYS.SCAN_QUERIES");

        assertEquals(1, scanQries0.size());

        QueryMXBean qryMBean = getMxBean(ignite0, "", QueryMXBean.class.getSimpleName(), QueryMXBean.class);

    }
}
