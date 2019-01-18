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
import org.apache.ignite.internal.stat.IoStatisticsHolderKey;
import org.apache.ignite.internal.stat.IoStatisticsType;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view of cache group IO statistics.
 */
public class SqlSystemViewIOStatisticsCacheGroup extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewIOStatisticsCacheGroup(GridKernalContext ctx) {
        super("STATIO_CACHE_GRP", "IO statistics for cache groups", ctx, "CACHE_GRP_NAME",
            newColumn("CACHE_GRP_NAME"),
            newColumn("PHYSICAL_READ", Value.LONG),
            newColumn("LOGICAL_READ", Value.LONG)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition nameCond = conditionForColumn("CACHE_GRP_NAME", first, last);

        Map<IoStatisticsHolderKey, IoStatisticsHolder> stats = ctx.ioStats().statistics(IoStatisticsType.CACHE_GROUP);

        List<Row> rows = new ArrayList<>();

        if (nameCond.isEquality()) {
            String cacheGrpName = nameCond.valueForEquality().getString();

            IoStatisticsHolder statHolder = stats.get(new IoStatisticsHolderKey(cacheGrpName, null));

            if (statHolder != null) {
                rows.add(
                    createRow(ses, 0,
                        cacheGrpName,
                        statHolder.physicalReads(),
                        statHolder.logicalReads()
                    )
                );
            }
        }
        else {
            for (Map.Entry<IoStatisticsHolderKey, IoStatisticsHolder> entry : stats.entrySet()) {
                rows.add(
                    createRow(ses, rows.size(),
                        entry.getKey().name(),
                        entry.getValue().physicalReads(),
                        entry.getValue().logicalReads()
                    )
                );
            }
        }

        return rows.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ctx.ioStats().statistics(IoStatisticsType.CACHE_GROUP).size();
    }
}
