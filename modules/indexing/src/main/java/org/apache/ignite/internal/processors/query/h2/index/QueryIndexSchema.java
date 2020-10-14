package org.apache.ignite.internal.processors.query.h2.index;

import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.cache.query.index.sorted.NullsOrder;
import org.apache.ignite.cache.query.index.sorted.Order;
import org.apache.ignite.cache.query.index.sorted.SortOrder;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexSchema;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.table.IndexColumn;


public class QueryIndexSchema implements SortedIndexSchema {

    private final IndexKeyDefinition[] indexKeyDefinitions;

    private IndexKeyDefinition[] inlineKeys;

    private final IndexColumn[] h2IndexColumns;

    private final GridH2RowDescriptor cacheDesc;

    protected final GridH2Table table;

    public QueryIndexSchema(GridH2Table table, IndexColumn[] h2IndexColumns) {
        this.table = table;

        cacheDesc = table.rowDescriptor();

        indexKeyDefinitions = new IndexKeyDefinition[h2IndexColumns.length];
        inlineKeys = new IndexKeyDefinition[h2IndexColumns.length];

        this.h2IndexColumns = h2IndexColumns.clone();

        for (int i = 0; i < h2IndexColumns.length; ++i) {
            IndexColumn c = h2IndexColumns[i];

            addKeyDefinition(i, c.column.getType(), c.sortType);
        }

        int inlineKeysCnt = stopInlineIdx != -1 ? stopInlineIdx : h2IndexColumns.length;
        inlineKeys = new IndexKeyDefinition[inlineKeysCnt];
        for (int i = 0; i < inlineKeysCnt; i++)
            inlineKeys[i] = indexKeyDefinitions[i];

        IndexColumn.mapColumns(h2IndexColumns, table);
    }

    private int stopInlineIdx = -1;

    private void addKeyDefinition(int i, int idxKeyType, int h2SortType) {
        IndexKeyDefinition def;

        // Stop inline if a key does not support inlining.
        if (stopInlineIdx < 0 && !InlineIndexKeyTypeRegistry.supportInline(idxKeyType))
            stopInlineIdx = i;

        def = new IndexKeyDefinition(idxKeyType, stopInlineIdx < 0, getSortOrder(h2SortType));

        indexKeyDefinitions[i] = def;
    }

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


    @Override public IndexKeyDefinition[] getKeyDefinitions() {
        return indexKeyDefinitions.clone();
    }

    @Override public IndexKeyDefinition[] getInlineKeys() {
        return inlineKeys;
    }

    @Override public Object getIndexKey(int idx, CacheDataRow row) {
        int cacheIdx = h2IndexColumns[idx].column.getColumnId();

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

    /**
     */
    @Override public int partition(CacheDataRow row) {
        Object key = key(row);

        return cacheDesc.context().affinity().partition(key);
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

/*
    protected int segmentForRow(GridCacheContext ctx, SearchRow row) {
        assert row != null;

        if (segmentsCount() == 1 || ctx == null)
            return 0;

        CacheObject key;

        final Value keyColValue = row.getValue(QueryUtils.KEY_COL);

        assert keyColValue != null;

        final Object o = keyColValue.getObject();

        if (o instanceof CacheObject)
            key = (CacheObject)o;
        else
            key = ctx.toCacheKeyObject(o);

        return segmentForPartition(ctx.affinity().partition(key));
    }
*/


    /** */
    private Object value(CacheDataRow row) {
        CacheObjectContext coctx = cacheDesc.context().cacheObjectContext();

        return row.value().value(coctx, false);
    }
}
