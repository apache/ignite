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
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singleton;
import static org.apache.ignite.internal.metric.IoStatisticsHolderCache.LOGICAL_READS;
import static org.apache.ignite.internal.metric.IoStatisticsHolderCache.PHYSICAL_READS;
import static org.apache.ignite.internal.metric.IoStatisticsType.CACHE_GROUP;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
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
            String cacheGrpName = nameCond.valueForEquality().getString();

            MetricRegistry mreg = ctx.metric().registry(metricName(CACHE_GROUP.metricGroupName(), cacheGrpName));

            IntMetric grpId = mreg.findMetric("grpId");
            ObjectMetric<String> grpName = mreg.findMetric("name");

            if (grpId == null)
                emptyIterator();

            if (mreg != null) {
                return singleton(toRow(ses,
                    grpId.value(),
                    grpName.value(),
                    mreg)
                ).iterator();
            }
        }
        else {
            Collection<CacheGroupContext> grpCtxs = ctx.cache().cacheGroups();

            GridMetricManager mmgr = ctx.metric();

            return iterator(grpCtxs,
                grpCtx -> toRow(ses,
                    grpCtx.groupId(),
                    grpCtx.cacheOrGroupName(),
                    mmgr.registry(metricName(CACHE_GROUP.metricGroupName(), grpCtx.cacheOrGroupName()))),
                true,
                grpCtx -> !grpCtx.systemCache());
        }

        return emptyIterator();
    }

    /** */
    private Row toRow(Session ses, int grpId, String grpName, MetricRegistry mreg) {
        IntMetric grpIdMetric = mreg.findMetric("grpId");

        if (grpIdMetric == null)
            return createRow(ses, grpId, grpName, 0, 0);

        return createRow(
            ses,
            grpId,
            grpName,
            mreg.<LongMetric>findMetric(PHYSICAL_READS).value(),
            mreg.<LongMetric>findMetric(LOGICAL_READS).value()
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
