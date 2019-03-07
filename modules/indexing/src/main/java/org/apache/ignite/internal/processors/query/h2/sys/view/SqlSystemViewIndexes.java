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
import java.util.function.Predicate;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.database.IndexInformation;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * View that contains information about all the sql tables in the cluster.
 */
public class SqlSystemViewIndexes extends SqlAbstractLocalSystemView {

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /**
     * Creates view with columns.
     *
     * @param ctx kernal context.
     */
    public SqlSystemViewIndexes(GridKernalContext ctx, SchemaManager schemaMgr) {
        super("INDEXES", "Ignite SQL indexes", ctx, "TABLE_NAME",
            newColumn("CACHE_GROUP_ID", Value.INT),
            newColumn("CACHE_GROUP_NAME"),
            newColumn("CACHE_ID", Value.INT),
            newColumn("CACHE_NAME"),
            newColumn("SCHEMA_NAME"),
            newColumn("TABLE_NAME"),
            newColumn("INDEX_NAME"),
            newColumn("INDEX_TYPE"),
            newColumn("COLUMNS"),
            newColumn("IS_PK", Value.BOOLEAN),
            newColumn("IS_UNIQUE", Value.BOOLEAN),
            newColumn("INLINE_SIZE", Value.INT)
        );

        this.schemaMgr = schemaMgr;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition tblNameCond = conditionForColumn("TABLE_NAME", first, last);

        Predicate<GridH2Table> filter;

        if (tblNameCond.isEquality()) {
            String tblName = tblNameCond.valueForEquality().getString();

            filter = tbl -> tblName.equals(tbl.getName());
        }
        else
            filter = tbl -> true;

        List<Row> rows = new ArrayList<>();

        schemaMgr.dataTables().stream().filter(filter).forEach(tbl -> {
            String schema = tbl.getSchema().getName();
            String tblName = tbl.getName();
            int cacheGrpId = tbl.cacheInfo().groupId();
            String cacheGrpName = ctx.cache().cacheGroupDescriptors().get(cacheGrpId).cacheOrGroupName();
            int cacheId = tbl.cacheId();
            String cacheName = tbl.cacheName();

            List<IndexInformation> idxInfoList = tbl.indexesInformation();

            for (IndexInformation idxInfo : idxInfoList) {
                Object[] data = new Object[] {
                    cacheGrpId,
                    cacheGrpName,
                    cacheId,
                    cacheName,
                    schema,
                    tblName,
                    idxInfo.name(),
                    idxInfo.type(),
                    idxInfo.keySql(),
                    idxInfo.pk(),
                    idxInfo.unique(),
                    idxInfo.inlineSize()
                };

                rows.add(createRow(ses, data));
            }
        });

        return rows.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return schemaMgr.dataTables().stream().mapToInt(t -> t.indexesInformation().size()).sum();
    }
}
