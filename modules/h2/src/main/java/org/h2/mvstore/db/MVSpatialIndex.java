/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.index.SpatialIndex;
import org.h2.index.SpatialTreeIndex;
import org.h2.message.DbException;
import org.h2.mvstore.db.TransactionStore.Transaction;
import org.h2.mvstore.db.TransactionStore.TransactionMap;
import org.h2.mvstore.db.TransactionStore.VersionedValue;
import org.h2.mvstore.db.TransactionStore.VersionedValueType;
import org.h2.mvstore.rtree.MVRTreeMap;
import org.h2.mvstore.rtree.MVRTreeMap.RTreeCursor;
import org.h2.mvstore.rtree.SpatialKey;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * This is an index based on a MVRTreeMap.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class MVSpatialIndex extends BaseIndex implements SpatialIndex, MVIndex {

    /**
     * The multi-value table.
     */
    final MVTable mvTable;

    private final String mapName;
    private final TransactionMap<SpatialKey, Value> dataMap;
    private final MVRTreeMap<VersionedValue> spatialMap;

    /**
     * Constructor.
     *
     * @param db the database
     * @param table the table instance
     * @param id the index id
     * @param indexName the index name
     * @param columns the indexed columns (only one geometry column allowed)
     * @param indexType the index type (only spatial index)
     */
    public MVSpatialIndex(
            Database db, MVTable table, int id, String indexName,
            IndexColumn[] columns, IndexType indexType) {
        if (columns.length != 1) {
            throw DbException.getUnsupportedException(
                    "Can only index one column");
        }
        IndexColumn col = columns[0];
        if ((col.sortType & SortOrder.DESCENDING) != 0) {
            throw DbException.getUnsupportedException(
                    "Cannot index in descending order");
        }
        if ((col.sortType & SortOrder.NULLS_FIRST) != 0) {
            throw DbException.getUnsupportedException(
                    "Nulls first is not supported");
        }
        if ((col.sortType & SortOrder.NULLS_LAST) != 0) {
            throw DbException.getUnsupportedException(
                    "Nulls last is not supported");
        }
        if (col.column.getType() != Value.GEOMETRY) {
            throw DbException.getUnsupportedException(
                    "Spatial index on non-geometry column, "
                    + col.column.getCreateSQL());
        }
        this.mvTable = table;
        initBaseIndex(table, id, indexName, columns, indexType);
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        mapName = "index." + getId();
        ValueDataType vt = new ValueDataType(null, null, null);
        VersionedValueType valueType = new VersionedValueType(vt);
        MVRTreeMap.Builder<VersionedValue> mapBuilder =
                new MVRTreeMap.Builder<VersionedValue>().
                valueType(valueType);
        spatialMap = db.getMvStore().getStore().openMap(mapName, mapBuilder);
        Transaction t = mvTable.getTransactionBegin();
        dataMap = t.openMap(spatialMap);
        t.commit();
    }

    @Override
    public void addRowsToBuffer(List<Row> rows, String bufferName) {
        throw DbException.throwInternalError();
    }

    @Override
    public void addBufferedRows(List<String> bufferNames) {
        throw DbException.throwInternalError();
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void add(Session session, Row row) {
        TransactionMap<SpatialKey, Value> map = getMap(session);
        SpatialKey key = getKey(row);

        if (key.isNull()) {
            return;
        }

        if (indexType.isUnique()) {
            // this will detect committed entries only
            RTreeCursor cursor = spatialMap.findContainedKeys(key);
            Iterator<SpatialKey> it = map.wrapIterator(cursor, false);
            while (it.hasNext()) {
                SpatialKey k = it.next();
                if (k.equalsIgnoringId(key)) {
                    throw getDuplicateKeyException(key.toString());
                }
            }
        }
        try {
            map.put(key, ValueLong.get(0));
        } catch (IllegalStateException e) {
            throw mvTable.convertException(e);
        }
        if (indexType.isUnique()) {
            // check if there is another (uncommitted) entry
            RTreeCursor cursor = spatialMap.findContainedKeys(key);
            Iterator<SpatialKey> it = map.wrapIterator(cursor, true);
            while (it.hasNext()) {
                SpatialKey k = it.next();
                if (k.equalsIgnoringId(key)) {
                    if (map.isSameTransaction(k)) {
                        continue;
                    }
                    map.remove(key);
                    if (map.get(k) != null) {
                        // committed
                        throw getDuplicateKeyException(k.toString());
                    }
                    throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, table.getName());
                }
            }
        }
    }

    @Override
    public void remove(Session session, Row row) {
        SpatialKey key = getKey(row);

        if (key.isNull()) {
            return;
        }

        TransactionMap<SpatialKey, Value> map = getMap(session);
        try {
            Value old = map.remove(key);
            if (old == null) {
                old = map.remove(key);
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                        getSQL() + ": " + row.getKey());
            }
        } catch (IllegalStateException e) {
            throw mvTable.convertException(e);
        }
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return find(filter.getSession());
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find(session);
    }

    private Cursor find(Session session) {
        Iterator<SpatialKey> cursor = spatialMap.keyIterator(null);
        TransactionMap<SpatialKey, Value> map = getMap(session);
        Iterator<SpatialKey> it = map.wrapIterator(cursor, false);
        return new MVStoreCursor(session, it);
    }

    @Override
    public Cursor findByGeometry(TableFilter filter, SearchRow first,
            SearchRow last, SearchRow intersection) {
        Session session = filter.getSession();
        if (intersection == null) {
            return find(session, first, last);
        }
        Iterator<SpatialKey> cursor =
                spatialMap.findIntersectingKeys(getKey(intersection));
        TransactionMap<SpatialKey, Value> map = getMap(session);
        Iterator<SpatialKey> it = map.wrapIterator(cursor, false);
        return new MVStoreCursor(session, it);
    }

    private SpatialKey getKey(SearchRow row) {
        Value v = row.getValue(columnIds[0]);
        if (v == ValueNull.INSTANCE) {
            return new SpatialKey(row.getKey());
        }
        Geometry g = ((ValueGeometry) v.convertTo(Value.GEOMETRY)).getGeometryNoCopy();
        Envelope env = g.getEnvelopeInternal();
        return new SpatialKey(row.getKey(),
                (float) env.getMinX(), (float) env.getMaxX(),
                (float) env.getMinY(), (float) env.getMaxY());
    }

    /**
     * Get the row with the given index key.
     *
     * @param key the index key
     * @return the row
     */
    SearchRow getRow(SpatialKey key) {
        SearchRow searchRow = mvTable.getTemplateRow();
        searchRow.setKey(key.getId());
        return searchRow;
    }

    @Override
    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter[] filters,
            int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        return SpatialTreeIndex.getCostRangeIndex(masks, columns);
    }

    @Override
    public void remove(Session session) {
        TransactionMap<SpatialKey, Value> map = getMap(session);
        if (!map.isClosed()) {
            Transaction t = session.getTransaction();
            t.removeMap(map);
        }
    }

    @Override
    public void truncate(Session session) {
        TransactionMap<SpatialKey, Value> map = getMap(session);
        map.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        if (!first) {
            throw DbException.throwInternalError(
                    "Spatial Index can only be fetch in ascending order");
        }
        return find(session);
    }

    @Override
    public boolean needRebuild() {
        try {
            return dataMap.sizeAsLongMax() == 0;
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getRowCount(Session session) {
        TransactionMap<SpatialKey, Value> map = getMap(session);
        return map.sizeAsLong();
    }

    @Override
    public long getRowCountApproximation() {
        try {
            return dataMap.sizeAsLongMax();
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getDiskSpaceUsed() {
        // TODO estimate disk space usage
        return 0;
    }

    @Override
    public void checkRename() {
        // ok
    }

    /**
     * Get the map to store the data.
     *
     * @param session the session
     * @return the map
     */
    TransactionMap<SpatialKey, Value> getMap(Session session) {
        if (session == null) {
            return dataMap;
        }
        Transaction t = session.getTransaction();
        return dataMap.getInstance(t, Long.MAX_VALUE);
    }

    /**
     * A cursor.
     */
    class MVStoreCursor implements Cursor {

        private final Session session;
        private final Iterator<SpatialKey> it;
        private SpatialKey current;
        private SearchRow searchRow;
        private Row row;

        public MVStoreCursor(Session session, Iterator<SpatialKey> it) {
            this.session = session;
            this.it = it;
        }

        @Override
        public Row get() {
            if (row == null) {
                SearchRow r = getSearchRow();
                if (r != null) {
                    row = mvTable.getRow(session, r.getKey());
                }
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            if (searchRow == null) {
                if (current != null) {
                    searchRow = getRow(current);
                }
            }
            return searchRow;
        }

        @Override
        public boolean next() {
            current = it.next();
            searchRow = null;
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }

    }

}

