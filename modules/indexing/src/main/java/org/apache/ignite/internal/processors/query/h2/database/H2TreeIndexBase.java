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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.InlineIndexColumnFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.engine.Session;
import org.h2.index.IndexType;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;

/**
 * H2 tree index base.
 */
public abstract class H2TreeIndexBase extends GridH2IndexBase {
    /** Default value for {@code IGNITE_MAX_INDEX_PAYLOAD_SIZE} */
    public static final int IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT = 10;

    /**
     * Constructor.
     *
     * @param tbl Table.
     */
    protected H2TreeIndexBase(GridH2Table tbl, String name, IndexColumn[] cols, IndexType type) {
        super(tbl, name, cols, type);
    }

    /**
     * @return Inline size.
     */
    public abstract int inlineSize();

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
        HashSet<Column> allColumnsSet) {
        long rowCnt = getRowCountApproximation();

        double baseCost = getCostRangeIndexEx(masks, rowCnt, filters, filter, sortOrder, false, allColumnsSet);

        int mul = getDistributedMultiplier(ses, filters, filter);

        return mul * baseCost;
    }

    /**
     * Creates inline helper list for provided column list.
     *
     * @param affinityKey Affinity key.
     * @param cacheName Cache name.
     * @param idxName Index name.
     * @param log Logger.
     * @param pk Pk.
     * @param tbl Table.
     * @param cols Columns.
     * @param factory Factory.
     * @param inlineObjHashSupported Whether hash inlining is supported or not.
     * @return List of {@link InlineIndexColumn} objects.
     */
    static List<InlineIndexColumn> getAvailableInlineColumns(boolean affinityKey, String cacheName,
        String idxName, IgniteLogger log, boolean pk, Table tbl, IndexColumn[] cols,
        InlineIndexColumnFactory factory, boolean inlineObjHashSupported) {
        ArrayList<InlineIndexColumn> res = new ArrayList<>(cols.length);

        for (IndexColumn col : cols) {
            if (!InlineIndexColumnFactory.typeSupported(col.column.getType())) {
                String idxType = pk ? "PRIMARY KEY" : affinityKey ? "AFFINITY KEY (implicit)" : "SECONDARY";

                U.warn(log, "Column cannot be inlined into the index because it's type doesn't support inlining, " +
                    "index access may be slow due to additional page reads (change column type if possible) " +
                    "[cacheName=" + cacheName +
                    ", tableName=" + tbl.getName() +
                    ", idxName=" + idxName +
                    ", idxType=" + idxType +
                    ", colName=" + col.columnName +
                    ", columnType=" + InlineIndexColumnFactory.nameTypeByCode(col.column.getType()) + ']'
                );

                res.trimToSize();

                break;
            }

            res.add(factory.createInlineHelper(col.column, inlineObjHashSupported));
        }

        return res;
    }

    /**
     * @param inlineIdxs Inline index helpers.
     * @param cfgInlineSize Inline size from cache config.
     * @param maxInlineSize Max inline size.
     * @return Inline size.
     */
    protected static int computeInlineSize(
        List<InlineIndexColumn> inlineIdxs,
        int cfgInlineSize,
        int maxInlineSize
    ) {
        if (cfgInlineSize == 0)
            return 0;

        if (F.isEmpty(inlineIdxs))
            return 0;

        if (cfgInlineSize != -1)
            return Math.min(PageIO.MAX_PAYLOAD_SIZE, cfgInlineSize);

        int propSize = maxInlineSize == -1
            ? IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE, IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT)
            : maxInlineSize;

        int size = 0;

        for (InlineIndexColumn idxHelper : inlineIdxs) {
            if (idxHelper.size() <= 0) {
                size = propSize;
                break;
            }

            // 1 byte type + size
            size += idxHelper.size() + 1;
        }

        return Math.min(PageIO.MAX_PAYLOAD_SIZE, size);
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return true;
    }
}
