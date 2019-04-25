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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * View that contains information about all the sql tables in the cluster.
 */
public class SqlSystemViewTables extends SqlAbstractLocalSystemView {
    /** Name of the column that contains names of sql tables. */
    private static final String TABLE_NAME = "TABLE_NAME";

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /**
     * Creates view with columns.
     *
     * @param ctx kernal context.
     */
    public SqlSystemViewTables(GridKernalContext ctx, SchemaManager schemaMgr) {
        super("TABLES", "Ignite tables", ctx, TABLE_NAME,
            newColumn("CACHE_GROUP_ID", Value.INT),
            newColumn("CACHE_GROUP_NAME"),
            newColumn("CACHE_ID", Value.INT),
            newColumn("CACHE_NAME"),
            newColumn("SCHEMA_NAME"),
            newColumn(TABLE_NAME),
            newColumn("AFFINITY_KEY_COLUMN"),
            newColumn("KEY_ALIAS"),
            newColumn("VALUE_ALIAS"),
            newColumn("KEY_TYPE_NAME"),
            newColumn("VALUE_TYPE_NAME")
        );

        this.schemaMgr = schemaMgr;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition nameCond = conditionForColumn(TABLE_NAME, first, last);

        Predicate<GridH2Table> filter;

        if (nameCond.isEquality()) {
            String tblName = nameCond.valueForEquality().getString();

            filter = tbl -> tblName.equals(tbl.getName());
        }
        else
            filter = tab -> true;

        List<Row> rows = new ArrayList<>();

        schemaMgr.dataTables().stream()
            .filter(filter)
            .forEach(tbl -> {
                    int cacheGrpId = tbl.cacheInfo().groupId();

                    CacheGroupDescriptor cacheGrpDesc = ctx.cache().cacheGroupDescriptors().get(cacheGrpId);

                    // We should skip table in case in case regarding cache group has been removed.
                    if (cacheGrpDesc == null)
                        return;

                    Object[] data = new Object[] {
                        cacheGrpId,
                        cacheGrpDesc.cacheOrGroupName(),
                        tbl.cacheId(),
                        tbl.cacheName(),
                        tbl.getSchema().getName(),
                        tbl.getName(),
                        computeAffinityColumn(tbl),
                        tbl.rowDescriptor().type().keyFieldAlias(),
                        tbl.rowDescriptor().type().valueFieldAlias(),
                        tbl.rowDescriptor().type().keyTypeName(),
                        tbl.rowDescriptor().type().valueTypeName()
                    };

                    rows.add(createRow(ses, data));
                }
            );

        return rows.iterator();
    }

    /**
     * Computes affinity column for the specified table.
     *
     * @param tbl Table.
     * @return "_KEY" for default (all PK), {@code null} if custom mapper specified or name of the desired column
     * otherwise.
     */
    private @Nullable String computeAffinityColumn(GridH2Table tbl) {
        IndexColumn affCol = tbl.getAffinityKeyColumn();

        if (affCol == null)
            return null;

        // Only explicit affinity column should be shown. Do not do this for _KEY or it's alias.
        if (tbl.rowDescriptor().isKeyColumn(affCol.column.getColumnId()))
            return null;

        return affCol.columnName;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return schemaMgr.dataTables().size();
    }
}
