/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import org.h2.api.ErrorCode;
import org.h2.command.dml.AllColumnsForPlan;
import org.h2.engine.Database;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * A table stored in a MVStore.
 */
public final class MVSecondaryIndex extends BaseIndex implements MVIndex {

    /**
     * The multi-value table.
     */
    final MVTable                             mvTable;
    private final int                         keyColumns;
    private final TransactionMap<Value,Value> dataMap;

    public MVSecondaryIndex(Database db, MVTable table, int id, String indexName,
                IndexColumn[] columns, IndexType indexType) {
        super(table, id, indexName, columns, indexType);
        this.mvTable = table;
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        // always store the row key in the map key,
        // even for unique indexes, as some of the index columns could be null
        keyColumns = columns.length + 1;
        String mapName = "index." + getId();
        assert db.isStarting() || !db.getStore().getMvStore().getMetaMap().containsKey("name." + mapName);
        int[] sortTypes = new int[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = columns[i].sortType;
        }
        sortTypes[keyColumns - 1] = SortOrder.ASCENDING;
        ValueDataType keyType = new ValueDataType(db, sortTypes);
        ValueDataType valueType = new ValueDataType();
        Transaction t = mvTable.getTransactionBegin();
        dataMap = t.openMap(mapName, keyType, valueType);
        dataMap.map.setVolatile(!table.isPersistData() || !indexType.isPersistent());
        t.commit();
        if (!keyType.equals(dataMap.getKeyType())) {
            throw DbException.throwInternalError(
                    "Incompatible key type, expected " + keyType + " but got "
                            + dataMap.getKeyType() + " for index " + indexName);
        }
    }

    @Override
    public void addRowsToBuffer(List<Row> rows, String bufferName) {
        MVMap<ValueArray, Value> map = openMap(bufferName);
        for (Row row : rows) {
            ValueArray key = convertToKey(row, null);
            map.append(key, ValueNull.INSTANCE);
        }
    }

    private static final class Source {
        private final Iterator<ValueArray> iterator;
        ValueArray currentRowData;

        public Source(Iterator<ValueArray> iterator) {
            this.iterator = iterator;
            this.currentRowData = iterator.next();
        }

        public boolean hasNext() {
            boolean result = iterator.hasNext();
            if(result) {
                currentRowData = iterator.next();
            }
            return result;
        }

        public ValueArray next() {
            return currentRowData;
        }

        public static final class Comparator implements java.util.Comparator<Source> {
            private final Mode databaseMode;
            private final CompareMode compareMode;

            public Comparator(Mode databaseMode, CompareMode compareMode) {
                this.databaseMode = databaseMode;
                this.compareMode = compareMode;
            }

            @Override
            public int compare(Source one, Source two) {
                return one.currentRowData.compareTo(two.currentRowData, databaseMode, compareMode);
            }
        }
    }

    @Override
    public void addBufferedRows(List<String> bufferNames) {
        CompareMode compareMode = database.getCompareMode();
        int buffersCount = bufferNames.size();
        Queue<Source> queue = new PriorityQueue<>(buffersCount,
                new Source.Comparator(database.getMode(), compareMode));
        for (String bufferName : bufferNames) {
            Iterator<ValueArray> iter = openMap(bufferName).keyIterator(null);
            if (iter.hasNext()) {
                queue.offer(new Source(iter));
            }
        }

        try {
            while (!queue.isEmpty()) {
                Source s = queue.poll();
                ValueArray rowData = s.next();
                SearchRow row = convertToSearchRow(rowData);

                if (indexType.isUnique() && !mayHaveNullDuplicates(row)) {
                    checkUnique(dataMap, rowData, Long.MIN_VALUE);
                }

                dataMap.putCommitted(rowData, ValueNull.INSTANCE);

                if (s.hasNext()) {
                    queue.offer(s);
                }
            }
        } finally {
            MVStore mvStore = database.getStore().getMvStore();
            for (String tempMapName : bufferNames) {
                mvStore.removeMap(tempMapName);
            }
        }
    }

    private MVMap<ValueArray, Value> openMap(String mapName) {
        int[] sortTypes = new int[keyColumns];
        for (int i = 0; i < indexColumns.length; i++) {
            sortTypes[i] = indexColumns[i].sortType;
        }
        sortTypes[keyColumns - 1] = SortOrder.ASCENDING;
        ValueDataType keyType = new ValueDataType(database, sortTypes);
        ValueDataType valueType = new ValueDataType();
        MVMap.Builder<ValueArray, Value> builder =
                new MVMap.Builder<ValueArray, Value>()
                        .singleWriter()
                        .keyType(keyType).valueType(valueType);
        MVMap<ValueArray, Value> map = database.getStore().
                getMvStore().openMap(mapName, builder);
        if (!keyType.equals(map.getKeyType())) {
            throw DbException.throwInternalError(
                    "Incompatible key type, expected " + keyType + " but got "
                            + map.getKeyType() + " for map " + mapName);
        }
        return map;
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void add(Session session, Row row) {
        TransactionMap<Value, Value> map = getMap(session);
        ValueArray array = convertToKey(row, null);
        boolean checkRequired = indexType.isUnique() && !mayHaveNullDuplicates(row);
        if (checkRequired) {
            checkUnique(map, array, Long.MIN_VALUE);
        }

        try {
            map.put(array, ValueNull.INSTANCE);
        } catch (IllegalStateException e) {
            throw mvTable.convertException(e);
        }

        if (checkRequired) {
            checkUnique(map, array, row.getKey());
        }
    }

    private void checkUnique(TransactionMap<Value, Value> map, ValueArray row, long newKey) {
        Iterator<Value> it = map.keyIterator(convertToKey(row, ValueLong.MIN), convertToKey(row, ValueLong.MAX), true);
        while (it.hasNext()) {
            ValueArray rowData = (ValueArray)it.next();
            Value[] array = rowData.getList();
            Value rowKey = array[array.length - 1];
            long rowId = rowKey.getLong();
            if (newKey != rowId) {
                if (map.get(rowData) != null) {
                    // committed
                    throw getDuplicateKeyException(rowKey.toString());
                }
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, table.getName());
            }
        }
    }

    @Override
    public void remove(Session session, Row row) {
        ValueArray array = convertToKey(row, null);
        TransactionMap<Value, Value> map = getMap(session);
        try {
            Value old = map.remove(array);
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
    public void update(Session session, Row oldRow, Row newRow) {
        if (!rowsAreEqual(oldRow, newRow)) {
            super.update(session, oldRow, newRow);
        }
    }

    private boolean rowsAreEqual(SearchRow rowOne, SearchRow rowTwo) {
        if (rowOne == rowTwo) {
            return true;
        }
        for (int index : columnIds) {
            Value v1 = rowOne.getValue(index);
            Value v2 = rowTwo.getValue(index);
            if (v1 == null ? v2 != null : !v1.equals(v2)) {
                return false;
            }
        }
        return rowOne.getKey() == rowTwo.getKey();
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find(session, first, false, last);
    }

    private Cursor find(Session session, SearchRow first, boolean bigger, SearchRow last) {
        ValueArray min = convertToKey(first, bigger ? ValueLong.MAX : ValueLong.MIN);
        ValueArray max = convertToKey(last, ValueLong.MAX);
        TransactionMap<Value,Value> map = getMap(session);
        return new MVStoreCursor(session, map.keyIterator(min, max, false));
    }

    private static ValueArray convertToKey(ValueArray r, ValueLong key) {
        Value[] values = r.getList().clone();
        values[values.length - 1] = key;
        return ValueArray.get(values);
    }

    private ValueArray convertToKey(SearchRow r, ValueLong key) {
        if (r == null) {
            return null;
        }
        Value[] array = new Value[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            int idx = c.getColumnId();
            Value v = r.getValue(idx);
            if (v != null) {
                array[i] = v.convertTo(c.getType(), database.getMode(), null);
            }
        }
        array[keyColumns - 1] = key != null ? key : ValueLong.get(r.getKey());
        return ValueArray.get(array);
    }

    /**
     * Convert array of values to a SearchRow.
     *
     * @param key the index key
     * @return the row
     */
    SearchRow convertToSearchRow(ValueArray key) {
        Value[] array = key.getList();
        SearchRow searchRow = mvTable.getTemplateRow();
        searchRow.setKey((array[array.length - 1]).getLong());
        Column[] cols = getColumns();
        for (int i = 0; i < array.length - 1; i++) {
            Column c = cols[i];
            int idx = c.getColumnId();
            Value v = array[i];
            searchRow.setValue(idx, v);
        }
        return searchRow;
    }

    @Override
    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        try {
            return 10 * getCostRangeIndex(masks, dataMap.sizeAsLongMax(),
                    filters, filter, sortOrder, false, allColumnsSet);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
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
        map.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        TransactionMap<Value, Value> map = getMap(session);
        Value key = first ? map.firstKey() : map.lastKey();
        while (true) {
            if (key == null) {
                return new MVStoreCursor(session,
                        Collections.<Value>emptyIterator());
            }
            if (((ValueArray) key).getList()[0] != ValueNull.INSTANCE) {
                break;
            }
            key = first ? map.higherKey(key) : map.lowerKey(key);
        }
        MVStoreCursor cursor = new MVStoreCursor(session,
                                Collections.singletonList(key).iterator());
        cursor.next();
        return cursor;
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
        TransactionMap<Value, Value> map = getMap(session);
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
    public boolean canFindNext() {
        return true;
    }

    @Override
    public Cursor findNext(Session session, SearchRow higherThan, SearchRow last) {
        return find(session, higherThan, true, last);
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
    private TransactionMap<Value, Value> getMap(Session session) {
        if (session == null) {
            return dataMap;
        }
        Transaction t = session.getTransaction();
        return dataMap.getInstance(t);
    }

    /**
     * A cursor.
     */
    final class MVStoreCursor implements Cursor {

        private final Session session;
        private final Iterator<Value> it;
        private ValueArray current;
        private Row row;

        MVStoreCursor(Session session, Iterator<Value> it) {
            this.session = session;
            this.it = it;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (current != null) {
                    Value[] values = current.getList();
                    row = mvTable.getRow(session, values[values.length - 1].getLong());
                }
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return current == null ? null : convertToSearchRow(current);
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? (ValueArray)it.next() : null;
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }

}
