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
 *
 */

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.sources.CacheGroupMetricSource;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singleton;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.metric.sources.CacheGroupMetricSource.CACHE_GROUP_METRICS_PREFIX;
import static org.apache.ignite.internal.util.lang.GridFunc.iterator;

/**
 * System view of cache group IO statistics.
 */
public class SqlSystemViewCacheGroupsIOStatistics extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewCacheGroupsIOStatistics(GridKernalContext ctx) {
        super("LOCAL_CACHE_GROUPS_IO", "Local node IO statistics for cache groups", ctx, "CACHE_GROUP_NAME",
            newColumn("CACHE_GROUP_ID", Value.INT),
            newColumn("CACHE_GROUP_NAME"),
            newColumn("PHYSICAL_READS", Value.LONG),
            newColumn("LOGICAL_READS", Value.LONG)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition nameCond = conditionForColumn("CACHE_GROUP_NAME", first, last);

        if (nameCond.isEquality()) {
            String grpName = nameCond.valueForEquality().getString();

            String srcName = metricName(CACHE_GROUP_METRICS_PREFIX, grpName);

            CacheGroupMetricSource src = ctx.metric().source(srcName);

            if (src == null)
                return emptyIterator();

            int grpId = src.groupId();

            return singleton(toRow(ses, grpId, grpName, src)).iterator();

        }
        else {
            Collection<CacheGroupContext> grpCtxs = ctx.cache().cacheGroups();

            GridMetricManager mmgr = ctx.metric();

            return iterator(grpCtxs,
                grpCtx -> {
                    String metricName = metricName(CACHE_GROUP_METRICS_PREFIX, grpCtx.cacheOrGroupName());

                    CacheGroupMetricSource src = mmgr.source(metricName);

                    return toRow(ses, grpCtx.groupId(), grpCtx.cacheOrGroupName(), src);
                },
                true,
                grpCtx -> !grpCtx.systemCache());
        }
    }

    /** */
    private Row toRow(Session ses, int grpId, String grpName, CacheGroupMetricSource src) {
        return createRow(
            ses,
            grpId,
            grpName,
            src.physicalReads(),
            src.logicalReads()
        );
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ctx.cache().cacheGroups().size();
    }
}
