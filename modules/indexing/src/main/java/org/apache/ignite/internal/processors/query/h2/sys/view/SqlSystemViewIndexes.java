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
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
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
    /** H2 Indexing. */
    private final IgniteH2Indexing idx;

    /**
     * Creates view with columns.
     *
     * @param ctx Kernal context.
     * @param idx Indexing.
     */
    public SqlSystemViewIndexes(GridKernalContext ctx, IgniteH2Indexing idx) {
        super("INDEXES", "Ignite SQL indexes", ctx, "TABLE_NAME",
            newColumn("SCHEMA_NAME"),
            newColumn("TABLE_NAME"),
            newColumn("INDEX_NAME"),
            newColumn("COLUMNS"),
            newColumn("INDEX_TYPE"),
            newColumn("IS_PK", Value.BOOLEAN),
            newColumn("IS_UNIQUE", Value.BOOLEAN),
            newColumn("CACHE_ID", Value.INT),
            newColumn("CACHE_NAME"),
            newColumn("GROUP_ID", Value.INT),
            newColumn("GROUP_NAME"),
            newColumn("INLINE_SIZE", Value.INT)
        );

        this.idx = idx;
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

        idx.dataTables().stream().filter(filter).forEach(tbl -> {
            String schema = tbl.getSchema().getName();
            String tblName = tbl.getName();
            int grpId = tbl.cache().groupId();
            String grpName = ctx.cache().cacheGroupDescriptors().get(grpId).cacheOrGroupName();
            int cacheId = tbl.cacheId();
            String cacheName = tbl.cacheName();

            List<IndexInformation> idxInfoList = tbl.indexesInformation();

            for (IndexInformation idxInfo : idxInfoList) {
                Object[] data = new Object[] {
                    schema,
                    tblName,
                    idxInfo.name(),
                    idxInfo.keySql(),
                    idxInfo.type(),
                    idxInfo.pk(),
                    idxInfo.unique(),
                    cacheId,
                    cacheName,
                    grpId,
                    grpName,
                    idxInfo.inlineSize()
                };

                rows.add(createRow(ses, rows.size(), data));
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
        return idx.dataTables().stream().mapToInt(t -> t.indexesInformation().size()).sum();
    }
}
