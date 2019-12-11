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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.opt.H2TableScanIndex.SCAN_INDEX_NAME_SUFFIX;

/**
 *
 */
public class IndexedCacheFileRebalancingTest extends IgnitePdsCacheFileRebalancingTxTest {
    /** {@inheritDoc} */
    @Override protected void verifyCacheContent(IgniteEx node, String cacheName, int entriesCnt, boolean removes) throws Exception {
        super.verifyCacheContent(node, cacheName, entriesCnt, removes);

        IgniteInternalCache cache = node.cachex(cacheName);

        if (!cache.context().isQueryEnabled())
            return;

        log.info("Index validation");

        int expSize = removes ? cache.size() : entriesCnt;

        String tbl = "\"" + cacheName + "\"." + TestValue.class.getSimpleName();

        for (Ignite g : G.allGrids()) {
            boolean idxUsed = isIndexUsed(((IgniteEx)g).context().query(), "V1", tbl, "V1");

            assertTrue("node=" + node.cluster().localNode().id(), idxUsed);
        }

        String sql = "select count(V1) from TESTVALUE where V1 >= 0 and V1 < 2147483647";

        for (Ignite g : G.allGrids()) {
            IgniteCache cache0 = g.cache(cacheName);

            FieldsQueryCursor<List<Long>> cur = cache0.query(new SqlFieldsQuery(sql));

            long cnt = cur.getAll().get(0).get(0);

            assertEquals("node=" + g.cluster().localNode().id(), expSize, cnt);
        }

        // Validate indexes consistency.
        ValidateIndexesClosure clo = new ValidateIndexesClosure(Collections.singleton(INDEXED_CACHE), 0, 0);

        node.cluster().active(false);

        for (Ignite g : G.allGrids()) {
            ((IgniteEx)g).context().resource().injectGeneric(clo);

            VisorValidateIndexesJobResult res = clo.call();

            assertFalse(res.hasIssues());
        }
    }

    /** */
    private boolean isIndexUsed(GridQueryProcessor qryProc, @Nullable String idxName, String tblName, String... reqFlds) {
        String sql = "explain select * from " + tblName + " where ";

        for (int i = 0; i < reqFlds.length; ++i)
            sql += reqFlds[i] + " > 0 and " + reqFlds[i] + " < 2147483647" + ((i < reqFlds.length - 1) ? " and " : "");

        String plan = qryProc.querySqlFields(new SqlFieldsQuery(sql), true)
            .getAll().get(0).get(0).toString().toUpperCase();

        return idxName != null ? (!plan.contains(SCAN_INDEX_NAME_SUFFIX) && plan.contains(idxName.toUpperCase())) : !plan.contains(SCAN_INDEX_NAME_SUFFIX);
    }
}
