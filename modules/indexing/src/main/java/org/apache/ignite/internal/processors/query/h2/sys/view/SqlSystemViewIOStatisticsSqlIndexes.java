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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.stat.IoStatisticsHolder;
import org.apache.ignite.internal.stat.IoStatisticsHolderIndex;
import org.apache.ignite.internal.stat.IoStatisticsHolderKey;
import org.apache.ignite.internal.stat.IoStatisticsType;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view of indexes IO statistics.
 */
public class SqlSystemViewIOStatisticsSqlIndexes extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewIOStatisticsSqlIndexes(GridKernalContext ctx) {
        super("STATIO_IDX", "IO statistics for SQL indexes", ctx, new String[] {"CACHE_GRP_NAME", "IDX_NAME"},
            newColumn("CACHE_GRP_NAME"),
            newColumn("IDX_NAME"),
            newColumn("PHYSICAL_READ", Value.LONG),
            newColumn("LOGICAL_READ", Value.LONG),
            newColumn("LEAF_PHYSICAL_READ", Value.LONG),
            newColumn("LEAF_LOGICAL_READ", Value.LONG),
            newColumn("INNER_PHYSICAL_READ", Value.LONG),
            newColumn("INNER_LOGICAL_READ", Value.LONG)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition cacheGrpNameCond = conditionForColumn("CACHE_GRP_NAME", first, last);

        SqlSystemViewColumnCondition idxNameCond = conditionForColumn("IDX_NAME", first, last);

        String cacheGrpName = null;

        if(cacheGrpNameCond.isEquality())
            cacheGrpName = cacheGrpNameCond.valueForEquality().getString();

        String idxName = null;

        if(idxNameCond.isEquality())
            idxName = idxNameCond.valueForEquality().getString();

        Map<IoStatisticsHolderKey, IoStatisticsHolder> statsHashIdx = ctx.ioStats().statistics(IoStatisticsType.HASH_INDEX);

        Map<IoStatisticsHolderKey, IoStatisticsHolder> statsSortedIdx = ctx.ioStats().statistics(IoStatisticsType.SORTED_INDEX);

        List<Row> rows = new ArrayList<>();

        fillRows(ses, statsHashIdx, rows, cacheGrpName, idxName);

        fillRows(ses, statsSortedIdx, rows, cacheGrpName, idxName);

        return rows.iterator();
    }

    /**
     * @param ses Session.
     * @param statMap Map with IO statistics.
     * @param outList Output list.
     * @param filterCacheGrpName Value to filter by name of cache groups.
     * @param filterIdxName Value to filter by name of indexes.
     */
    private void fillRows(Session ses, Map<IoStatisticsHolderKey, IoStatisticsHolder> statMap, List<Row> outList,
        String filterCacheGrpName, String filterIdxName) {
        for (Map.Entry<IoStatisticsHolderKey, IoStatisticsHolder> entry : statMap.entrySet()) {
            IoStatisticsHolderKey key = entry.getKey();

            if(filterCacheGrpName != null && !key.name().equals(filterCacheGrpName))
                continue;

            if(filterIdxName != null && !key.subName().equals(filterIdxName))
                continue;

            IoStatisticsHolder val = entry.getValue();

            outList.add(
                createRow(ses, outList.size(),
                    key.name(),
                    key.subName(),
                    val.physicalReads(),
                    val.logicalReads(),
                    val.physicalReadsMap().get(IoStatisticsHolderIndex.PHYSICAL_READS_LEAF),
                    val.logicalReadsMap().get(IoStatisticsHolderIndex.LOGICAL_READS_LEAF),
                    val.physicalReadsMap().get(IoStatisticsHolderIndex.PHYSICAL_READS_INNER),
                    val.logicalReadsMap().get(IoStatisticsHolderIndex.LOGICAL_READS_INNER)
                )
            );
        }
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ctx.ioStats().statistics(IoStatisticsType.HASH_INDEX).size() +
            ctx.ioStats().statistics(IoStatisticsType.SORTED_INDEX).size();
    }
}
