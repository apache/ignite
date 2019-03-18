/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.db.TransactionStore.Transaction;
import org.h2.mvstore.db.TransactionStore.TransactionMap;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * A table stored in a MVStore.
 */
public class MVPrimaryIndex extends BaseIndex {

    private final MVTable mvTable;
    private final String mapName;
    private final TransactionMap<Value, Value> dataMap;
    private final AtomicLong lastKey = new AtomicLong(0);
    private int mainIndexColumn = -1;

    public MVPrimaryIndex(Database db, MVTable table, int id,
            IndexColumn[] columns, IndexType indexType) {
        this.mvTable = table;
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
        int[] sortTypes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = SortOrder.ASCENDING;
        }
        ValueDataType keyType = new ValueDataType(null, null, null);
        ValueDataType valueType = new ValueDataType(db.getCompareMode(), db,
                sortTypes);
        mapName = "table." + getId();
        Transaction t = mvTable.getTransactionBegin();
        dataMap = t.openMap(mapName, keyType, valueType);
        t.commit();
        if (!table.isPersistData()) {
            dataMap.map.setVolatile(true);
        }
        Value k = dataMap.map.lastKey();    // include uncommitted keys as well
        lastKey.set(k == null ? 0 : k.getLong());
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public String getPlanSQL() {
        return table.getSQL() + ".tableScan";
    }

    public void setMainIndexColumn(int mainIndexColumn) {
        this.mainIndexColumn = mainIndexColumn;
    }

    public int getMainIndexColumn() {
        return mainIndexColumn;
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void add(Session session, Row row) {
        if (mainIndexColumn == -1) {
            if (row.getKey() == 0) {
                row.setKey(lastKey.incrementAndGet());
            }
        } else {
            long c = row.getValue(mainIndexColumn).getLong();
            row.setKey(c);
        }

        if (mvTable.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                Value v2 = v.copy(database, getId());
                if (v2.isLinkedToTable()) {
                    session.removeAtCommitStop(v2);
                }
                if (v != v2) {
                    row.setValue(i, v2);
                }
            }
        }

        TransactionMap<Value, Value> map = getMap(session);
        Value key = ValueLong.get(row.getKey());
        Value old = map.getLatest(key);
        if (old != null) {
            String sql = "PRIMARY KEY ON " + table.getSQL();
            if (mainIndexColumn >= 0 && mainIndexColumn < indexColumns.length) {
                sql += "(" + indexColumns[mainIndexColumn].getSQL() + ")";
            }
            DbException e = DbException.get(ErrorCode.DUPLICATE_KEY_1, sql);
            e.setSource(this);
            throw e;
        }
        try {
            map.put(key, ValueArray.get(row.getValueList()));
        } catch (IllegalStateException e) {
            throw mvTable.convertException(e);
        }
        // because it's possible to directly update the key using the _rowid_
        // syntax
        if (row.getKey() > lastKey.get()) {
            lastKey.set(row.getKey());
        }
    }

    @Override
    public void remove(Session session, Row row) {
        if (mvTable.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v.isLinkedToTable()) {
                    session.removeAtCommit(v);
                }
            }
        }
        TransactionMap<Value, Value> map = getMap(session);
        try {
            Value old = map.remove(ValueLong.get(row.getKey()));
            if (old == null) {
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                        getSQL() + ": " + row.getKey());
            }
        } catch (IllegalStateException e) {
            throw mvTable.convertException(e);
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        ValueLong min, max;
        if (first == null) {
            min = ValueLong.MIN;
        } else if (mainIndexColumn < 0) {
            min = ValueLong.get(first.getKey());
        } else {
            ValueLong v = (ValueLong) first.getValue(mainIndexColumn);
            if (v == null) {
                min = ValueLong.get(first.getKey());
            } else {
                min = v;
            }
        }
        if (last == null) {
            max = ValueLong.MAX;
        } else if (mainIndexColumn < 0) {
            max = ValueLong.get(last.getKey());
        } else {
            ValueLong v = (ValueLong) last.getValue(mainIndexColumn);
            if (v == null) {
                max = ValueLong.get(last.getKey());
            } else {
                max = v;
            }
        }
        TransactionMap<Value, Value> map = getMap(session);
        return new MVStoreCursor(session, map.entryIterator(min, max));
    }

    @Override
    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public Row getRow(Session session, long key) {
        TransactionMap<Value, Value> map = getMap(session);
        Value v = map.get(ValueLong.get(key));
        if (v == null) {
            throw DbException.get(ErrorCode.ROW_NOT_FOUND_IN_PRIMARY_INDEX,
                    getSQL() + ": " + key);
        }
        ValueArray array = (ValueArray) v;
        Row row = session.createRow(array.getList(), 0);
        row.setKey(key);
        return row;
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        try {
            return 10 * getCostRangeIndex(masks, dataMap.sizeAsLongMax(),
                    filters, filter, sortOrder, true, allColumnsSet);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public int getColumnIndex(Column col) {
        // can not use this index - use the delegate index instead
        return -1;
    }

    @Override
    public boolean isFirstColumn(Column column) {
        return false;
    }

    @Override
    public void remove(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        if (!map.isClosed()) {
            Transaction t = session.getTransaction();
            t.removeMap(map);
        }
    }

    @Override
    public void truncate(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        if (mvTable.getContainsLargeObject()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        map.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        TransactionMap<Value, Value> map = getMap(session);
        ValueLong v = (ValueLong) (first ? map.firstKey() : map.lastKey());
        if (v == null) {
            return new MVStoreCursor(session,
                    Collections.<Entry<Value, Value>> emptyList().iterator());
        }
        Value value = map.get(v);
        Entry<Value, Value> e = new DataUtils.MapEntry<Value, Value>(v, value);
        List<Entry<Value, Value>> list = Collections.singletonList(e);
        MVStoreCursor c = new MVStoreCursor(session, list.iterator());
        c.next();
        return c;
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        return map.sizeAsLong();
    }

    /**
     * The maximum number of rows, including uncommitted rows of any session.
     *
     * @return the maximum number of rows
     */
    public long getRowCountMax() {
        try {
            return dataMap.sizeAsLongMax();
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getRowCountApproximation() {
        return getRowCountMax();
    }

    @Override
    public long getDiskSpaceUsed() {
        // TODO estimate disk space usage
        return 0;
    }

    public String getMapName() {
        return mapName;
    }

    @Override
    public void checkRename() {
        // ok
    }

    /**
     * Get the key from the row.
     *
     * @param row the row
     * @param ifEmpty the value to use if the row is empty
     * @param ifNull the value to use if the column is NULL
     * @return the key
     */
    ValueLong getKey(SearchRow row, ValueLong ifEmpty, ValueLong ifNull) {
        if (row == null) {
            return ifEmpty;
        }
        Value v = row.getValue(mainIndexColumn);
        if (v == null) {
            throw DbException.throwInternalError(row.toString());
        } else if (v == ValueNull.INSTANCE) {
            return ifNull;
        }
        return (ValueLong) v.convertTo(Value.LONG);
    }

    /**
     * Search for a specific row or a set of rows.
     *
     * @param session the session
     * @param first the key of the first row
     * @param last the key of the last row
     * @return the cursor
     */
    Cursor find(Session session, ValueLong first, ValueLong last) {
        TransactionMap<Value, Value> map = getMap(session);
        return new MVStoreCursor(session, map.entryIterator(first, last));
    }

    @Override
    public boolean isRowIdIndex() {
        return true;
    }

    /**
     * Get the map to store the data.
     *
     * @param session the session
     * @return the map
     */
    TransactionMap<Value, Value> getMap(Session session) {
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
        private final Iterator<Entry<Value, Value>> it;
        private Entry<Value, Value> current;
        private Row row;

        public MVStoreCursor(Session session, Iterator<Entry<Value, Value>> it) {
            this.session = session;
            this.it = it;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (current != null) {
                    ValueArray array = (ValueArray) current.getValue();
                    row = session.createRow(array.getList(), 0);
                    row.setKey(current.getKey().getLong());
                }
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }

    }

}
