/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
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

            MetricRegistry mset = ctx.metric().registry().withPrefix(CACHE_GROUP.metricGroupName(), cacheGrpName);

            IntMetric grpId = (IntMetric)mset.findMetric("grpId");
            ObjectMetric<String> grpName = (ObjectMetric<String>)mset.findMetric("name");

            if (grpId == null)
                emptyIterator();

            if (mset != null) {
                return singleton(toRow(ses,
                    grpId.value(),
                    grpName.value(),
                    mset)
                ).iterator();
            }
        }
        else {
            Collection<CacheGroupContext> grpCtxs = ctx.cache().cacheGroups();

            MetricRegistry mset = ctx.metric().registry().withPrefix(CACHE_GROUP.metricGroupName());

            return iterator(grpCtxs,
                grpCtx -> toRow(ses,
                    grpCtx.groupId(),
                    grpCtx.cacheOrGroupName(),
                    mset.withPrefix(grpCtx.cacheOrGroupName())),
                true,
                grpCtx -> !grpCtx.systemCache());
        }

        return emptyIterator();
    }

    /** */
    private Row toRow(Session ses, int grpId, String grpName, MetricRegistry mset) {
        IntMetric grpIdMetric = (IntMetric)mset.findMetric("grpId");

        if (grpIdMetric == null)
            return createRow(ses, grpId, grpName, 0, 0);

        return createRow(
            ses,
            grpId,
            grpName,
            ((LongMetric)mset.findMetric(PHYSICAL_READS)).value(),
            ((LongMetric)mset.findMetric(LOGICAL_READS)).value()
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
