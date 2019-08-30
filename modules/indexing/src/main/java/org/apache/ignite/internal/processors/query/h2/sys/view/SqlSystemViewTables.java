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

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.TableType;
import org.h2.value.Value;

/**
 * View that contains information about all the sql tables in the cluster.
 */
public class SqlSystemViewTables extends SqlAbstractLocalSystemView {
    /** Name of the column that contains names of sql tables. */
    private static final String TABLE_NAME = "TABLE_NAME";

    /**
     * Creates view with columns.
     *
     * @param ctx kernal context.
     */
    public SqlSystemViewTables(GridKernalContext ctx) {
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
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition nameCond = conditionForColumn(TABLE_NAME, first, last);

        String tblNamePtrn = nameCond.isEquality() ? nameCond.valueForEquality().getString() : null;

        List<Row> rows = ctx.query().getIndexing().tablesInformation(null, tblNamePtrn, TableType.TABLE.name())
            .stream()
            .map(tbl ->
                createRow(ses,
                    new Object[] {
                        tbl.cacheGrpId(),
                        tbl.cacheGrpName(),
                        tbl.cacheId(),
                        tbl.cacheName(),
                        tbl.schemaName(),
                        tbl.tableName(),
                        tbl.affinityKeyColumn(),
                        tbl.keyAlias(),
                        tbl.valueAlias(),
                        tbl.keyTypeName(),
                        tbl.valueTypeName()
                    }
                )
            ).collect(Collectors.toList());

        return rows.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ctx.query().getIndexing().tablesInformation(null, null, TableType.TABLE.name()).size();
    }
}
