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
 */

package org.apache.ignite.internal.processors.query.h2.index;

import org.apache.ignite.cache.query.index.sorted.NullsOrder;
import org.apache.ignite.cache.query.index.sorted.Order;
import org.apache.ignite.cache.query.index.sorted.SortOrder;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexSchema;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.table.IndexColumn;

/**
 * Schema for QueryIndex.
 */
public class QueryIndexSchema implements SortedIndexSchema {
    /** Key definitions. */
    private final IndexKeyDefinition[] idxKeyDefinitions;

    /** H2 index columns. */
    private final IndexColumn[] h2IdxColumns;

    /** Cache descriptor. */
    private final GridH2RowDescriptor cacheDesc;

    /** Table. */
    private final GridH2Table table;

    /** */
    public QueryIndexSchema(GridH2Table table, IndexColumn[] h2IndexColumns) {
        this.table = table;

        cacheDesc = table.rowDescriptor();

        idxKeyDefinitions = new IndexKeyDefinition[h2IndexColumns.length];

        this.h2IdxColumns = h2IndexColumns.clone();

        for (int i = 0; i < h2IndexColumns.length; ++i) {
            IndexColumn c = h2IndexColumns[i];

            addKeyDefinition(i, c.column.getType(), c.sortType);
        }

        IndexColumn.mapColumns(h2IndexColumns, table);
    }

    /** */
    private void addKeyDefinition(int i, int idxKeyType, int h2SortType) {
        idxKeyDefinitions[i] = new IndexKeyDefinition(idxKeyType, getSortOrder(h2SortType));;
    }

    /** */
    private Order getSortOrder(int sortType) {
        Order o = new Order();

        if ((sortType & 1) != 0)
            o.setSortOrder(SortOrder.DESC);
        else
            o.setSortOrder(SortOrder.ASC);

        if ((sortType & 2) != 0)
            o.setNullsOrder(NullsOrder.NULLS_FIRST);
        else if ((sortType & 4) != 0)
            o.setNullsOrder(NullsOrder.NULLS_LAST);

        return o;
    }


    /** {@inheritDoc} */
    @Override public IndexKeyDefinition[] getKeyDefinitions() {
        return idxKeyDefinitions.clone();
    }

    /** {@inheritDoc} */
    @Override public Object getIndexKey(int idx, CacheDataRow row) {
        int cacheIdx = h2IdxColumns[idx].column.getColumnId();

        switch (cacheIdx) {
            case QueryUtils.KEY_COL:
                return key(row);

            case QueryUtils.VAL_COL:
                return value(row);

            default:
                if (cacheDesc.isKeyAliasColumn(cacheIdx))
                    return key(row);

                else if (cacheDesc.isValueAliasColumn(cacheIdx))
                    return value(row);

                // columnValue ignores default columns (_KEY, _VAL), so make this shift.
                return cacheDesc.columnValue(row.key(), row.value(), cacheIdx - QueryUtils.DEFAULT_COLUMNS_COUNT);
        }
    }

     /** {@inheritDoc} */
    @Override public int partition(CacheDataRow row) {
        Object key = key(row);

        return cacheDesc.context().affinity().partition(key);
    }

    /** */
    GridH2Table getTable() {
        return table;
    }

    /** */
    private Object key(CacheDataRow row) {
        KeyCacheObject key = row.key();

        if (key instanceof BinaryObjectImpl) {
            // TODO: check column is JAVA_OBJECT?
            ((BinaryObjectImpl)key).detachAllowed(true);
            key = ((BinaryObjectImpl)key).detach();
            return key;
        }

        CacheObjectContext coctx = cacheDesc.context().cacheObjectContext();

        return key.value(coctx, false);
    }

    /** */
    private Object value(CacheDataRow row) {
        CacheObjectContext coctx = cacheDesc.context().cacheObjectContext();

        return row.value().value(coctx, false);
    }
}
