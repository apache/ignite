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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.internal.processors.query.h2.opt.H2TableScanIndex.SCAN_INDEX_NAME_SUFFIX;

/**
 *
 */
public class IndexedCacheFileRebalancingTest extends IgniteCacheFileRebalancingTxTest {
    /** {@inheritDoc} */
    @Override protected <V> void verifyCache(IgniteEx node, LoadParameters<V> cfg) throws Exception {
        super.verifyCache(node, cfg);

        String name = cfg.cacheName();

        if (!name.equals(INDEXED_CACHE))
            return;

        assert node.cachex(name).context().isQueryEnabled();

        int cnt = cfg.entriesCnt();
        boolean removes = cfg.checkRemoves();

        int expSize = removes ? node.cache(name).size() : cnt;
        String tbl = "\"" + name + "\"." + TestValue.class.getSimpleName();
        String sql = "select COUNT(V1) from " + tbl + " where V1 >= 0 and V1 < 2147483647";

        for (Ignite g : G.allGrids()) {
            log.info("Index validation [cache=" + name + ", node=" + g.cluster().localNode().id());

            g.cache(name).indexReadyFuture().get(15_000);

            UUID nodeId = g.cluster().localNode().id();

            boolean idxUsed = GridTestUtils.waitForCondition(() ->
                isIndexUsed(((IgniteEx)g).context().query(), "V1", tbl, "V1"), 15_000);

            assertTrue("node=" + nodeId, idxUsed);

            IgniteCache cache0 = g.cache(name);

            FieldsQueryCursor<List<Long>> cur = cache0.query(new SqlFieldsQuery(sql));

            long idxCnt = cur.getAll().get(0).get(0);

            assertEquals("node=" + nodeId, expSize, idxCnt);
        }
    }

    /** */
    private boolean isIndexUsed(GridQueryProcessor qryProc, String idxName, String tblName, String... reqFlds) {
        int len = reqFlds.length;
        String sql = "explain select * from " + tblName + " where ";

        for (int i = 0; i < len; ++i)
            sql += reqFlds[i] + " > 0 and " + reqFlds[i] + " < 2147483647" + ((i < len - 1) ? " and " : "");

        String plan = qryProc.querySqlFields(new SqlFieldsQuery(sql), true)
            .getAll().get(0).get(0).toString().toUpperCase();

        return !plan.contains(SCAN_INDEX_NAME_SUFFIX) && plan.contains(idxName.toUpperCase());
    }
}
