/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import static org.h2.util.geometry.GeometryUtils.MAX_X;
import static org.h2.util.geometry.GeometryUtils.MAX_Y;
import static org.h2.util.geometry.GeometryUtils.MIN_X;
import static org.h2.util.geometry.GeometryUtils.MIN_Y;

import java.util.Iterator;
import java.util.List;
import org.h2.api.ErrorCode;
import org.h2.command.dml.AllColumnsForPlan;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.index.SpatialIndex;
import org.h2.index.SpatialTreeIndex;
import org.h2.message.DbException;
import org.h2.mvstore.Page;
import org.h2.mvstore.rtree.MVRTreeMap;
import org.h2.mvstore.rtree.MVRTreeMap.RTreeCursor;
import org.h2.mvstore.rtree.SpatialKey;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.mvstore.tx.VersionedValueType;
import org.h2.value.VersionedValue;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

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
        super(table, id, indexName, columns, indexType);
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
        if (col.column.getType().getValueType() != Value.GEOMETRY) {
            throw DbException.getUnsupportedException(
                    "Spatial index on non-geometry column, "
                    + col.column.getCreateSQL());
        }
        this.mvTable = table;
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        String mapName = "index." + getId();
        ValueDataType vt = new ValueDataType(db, null);
        VersionedValueType valueType = new VersionedValueType(vt);
        MVRTreeMap.Builder<VersionedValue> mapBuilder =
                new MVRTreeMap.Builder<VersionedValue>().
                valueType(valueType);
        spatialMap = db.getStore().getMvStore().openMap(mapName, mapBuilder);
        Transaction t = mvTable.getTransactionBegin();
        dataMap = t.openMap(spatialMap);
        dataMap.map.setVolatile(!table.isPersistData() || !indexType.isPersistent());
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
                StringBuilder builder = new StringBuilder();
                getSQL(builder, false).append(": ").append(row.getKey());
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, builder.toString());
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
        return new MVStoreCursor(session, it, mvTable);
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
        return new MVStoreCursor(session, it, mvTable);
    }

    /**
     * Returns the minimum bounding box that encloses all keys.
     *
     * @param session the session
     * @return the minimum bounding box that encloses all keys, or null
     */
    public Value getBounds(Session session) {
        FindBoundsCursor cursor = new FindBoundsCursor(spatialMap.getRootPage(), new SpatialKey(0), session,
                getMap(session), columnIds[0]);
        while (cursor.hasNext()) {
            cursor.next();
        }
        return cursor.getBounds();
    }

    /**
     * Returns the estimated minimum bounding box that encloses all keys.
     *
     * The returned value may be incorrect.
     *
     * @param session the session
     * @return the estimated minimum bounding box that encloses all keys, or null
     */
    public Value getEstimatedBounds(Session session) {
        Page p = spatialMap.getRootPage();
        int count = p.getKeyCount();
        if (count > 0) {
            SpatialKey key = (SpatialKey) p.getKey(0);
            float bminxf = key.min(0), bmaxxf = key.max(0), bminyf = key.min(1), bmaxyf = key.max(1);
            for (int i = 1; i < count; i++) {
                key = (SpatialKey) p.getKey(i);
                float minxf = key.min(0), maxxf = key.max(0), minyf = key.min(1), maxyf = key.max(1);
                if (minxf < bminxf) {
                    bminxf = minxf;
                }
                if (maxxf > bmaxxf) {
                    bmaxxf = maxxf;
                }
                if (minyf < bminyf) {
                    bminyf = minyf;
                }
                if (maxyf > bmaxyf) {
                    bmaxyf = maxyf;
                }
            }
            return ValueGeometry.fromEnvelope(new double[] {bminxf, bmaxxf, bminyf, bmaxyf});
        }
        return ValueNull.INSTANCE;
    }

    private SpatialKey getKey(SearchRow row) {
        Value v = row.getValue(columnIds[0]);
        double[] env;
        if (v == ValueNull.INSTANCE ||
                (env = ((ValueGeometry) v.convertTo(Value.GEOMETRY)).getEnvelopeNoCopy()) == null) {
            return new SpatialKey(row.getKey());
        }
        return new SpatialKey(row.getKey(),
                (float) env[MIN_X], (float) env[MAX_X],
                (float) env[MIN_Y], (float) env[MAX_Y]);
    }

    @Override
    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter[] filters,
            int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
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
    private TransactionMap<SpatialKey, Value> getMap(Session session) {
        if (session == null) {
            return dataMap;
        }
        Transaction t = session.getTransaction();
        return dataMap.getInstance(t);
    }

    /**
     * A cursor.
     */
    public static class MVStoreCursor implements Cursor {

        private final Session session;
        private final Iterator<SpatialKey> it;
        private final MVTable mvTable;
        private SpatialKey current;
        private SearchRow searchRow;
        private Row row;

        public MVStoreCursor(Session session, Iterator<SpatialKey> it, MVTable mvTable) {
            this.session = session;
            this.it = it;
            this.mvTable = mvTable;
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
                    searchRow = mvTable.getTemplateRow();
                    searchRow.setKey(current.getId());
                }
            }
            return searchRow;
        }

        /**
         * Returns the current key.
         *
         * @return the current key
         */
        public SpatialKey getKey() {
            return current;
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            searchRow = null;
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }

    }

    /**
     * A cursor for getBounds() method.
     */
    private final class FindBoundsCursor extends RTreeCursor {

        private final Session session;

        private final TransactionMap<SpatialKey, Value> map;

        private final int columnId;

        private boolean hasBounds;

        private float bminxf, bmaxxf, bminyf, bmaxyf;

        private double bminxd, bmaxxd, bminyd, bmaxyd;

        FindBoundsCursor(Page root, SpatialKey filter, Session session, TransactionMap<SpatialKey, Value> map,
                int columnId) {
            super(root, filter);
            this.session = session;
            this.map = map;
            this.columnId = columnId;
        }

        @Override
        protected boolean check(boolean leaf, SpatialKey key, SpatialKey test) {
            float minxf = key.min(0), maxxf = key.max(0), minyf = key.min(1), maxyf = key.max(1);
            if (leaf) {
                if (hasBounds) {
                    if ((minxf <= bminxf || maxxf >= bmaxxf || minyf <= bminyf || maxyf >= bmaxyf)
                            && map.containsKey(key)) {
                        double[] env = ((ValueGeometry) mvTable.getRow(session, key.getId()).getValue(columnId))
                                .getEnvelopeNoCopy();
                        double minxd = env[MIN_X], maxxd = env[MAX_X], minyd = env[MIN_Y], maxyd = env[MAX_Y];
                        if (minxd < bminxd) {
                            bminxf = minxf;
                            bminxd = minxd;
                        }
                        if (maxxd > bmaxxd) {
                            bmaxxf = maxxf;
                            bmaxxd = maxxd;
                        }
                        if (minyd < bminyd) {
                            bminyf = minyf;
                            bminyd = minyd;
                        }
                        if (maxyd > bmaxyd) {
                            bmaxyf = maxyf;
                            bmaxyd = maxyd;
                        }
                    }
                } else if (map.containsKey(key)) {
                    hasBounds = true;
                    double[] env = ((ValueGeometry) mvTable.getRow(session, key.getId()).getValue(columnId))
                            .getEnvelopeNoCopy();
                    bminxf = minxf;
                    bminxd = env[MIN_X];
                    bmaxxf = maxxf;
                    bmaxxd = env[MAX_X];
                    bminyf = minyf;
                    bminyd = env[MIN_Y];
                    bmaxyf = maxyf;
                    bmaxyd = env[MAX_Y];
                }
            } else if (hasBounds) {
                if (minxf <= bminxf || maxxf >= bmaxxf || minyf <= bminyf || maxyf >= bmaxyf) {
                    return true;
                }
            } else {
                return true;
            }
            return false;
        }

        Value getBounds() {
            return hasBounds ? ValueGeometry.fromEnvelope(new double[] {bminxd, bmaxxd, bminyd, bmaxyd})
                    : ValueNull.INSTANCE;
        }

    }

}

