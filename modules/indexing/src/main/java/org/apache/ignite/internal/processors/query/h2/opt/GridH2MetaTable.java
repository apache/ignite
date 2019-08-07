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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexCondition;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableBase;
import org.h2.table.TableFilter;
import org.h2.table.TableType;
import org.h2.value.Value;
import org.h2.value.ValueInt;

/**
 * Meta table.
 */
public class GridH2MetaTable extends TableBase {
    /** */
    private static final int ID = 0;

    /** */
    private final MetaIndex index;

    /** */
    private final AtomicLong dataModificationId = new AtomicLong();

    /** */
    private final Set<Session> fakeExclusiveSet = Collections.newSetFromMap(
        new ConcurrentHashMap<Session,Boolean>());

    /**
     * @param data Data.
     */
    public GridH2MetaTable(CreateTableData data) {
        super(data);

        ArrayList<Column> cols = data.columns;
        assert cols.size() == 4 : cols;

        Column id = cols.get(ID);
        assert "ID".equals(id.getName()) && id.getType() == Value.INT : cols;
        assert id.getColumnId() == ID;

        index = new MetaIndex();
    }

    /** {@inheritDoc} */
    @Override public Row getTemplateRow() {
        return new H2PlainRow(4);
    }

    /** {@inheritDoc} */
    @Override public SearchRow getTemplateSimpleRow(boolean singleColumn) {
        if (singleColumn)
            return H2PlainRowFactory.create((Value)null);

        return new H2PlainRow(4);
    }

    /** {@inheritDoc} */
    @Override public boolean lock(Session session, boolean exclusive, boolean forceLockEvenInMvcc) {
        if (fakeExclusiveSet.contains(session))
            return true;

        if (exclusive)
            fakeExclusiveSet.add(session);

        return false;
    }

    /** {@inheritDoc} */
    @Override public void unlock(Session s) {
        fakeExclusiveSet.remove(s);
    }

    /** {@inheritDoc} */
    @Override public void close(Session session) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Index addIndex(Session session, String indexName, int indexId,
        IndexColumn[] cols, IndexType indexType, boolean create, String indexComment) {
        assert cols.length == 1 : "len: " + cols.length;

        int colId = cols[0].column.getColumnId();
        assert colId == ID : "colId: " + colId;

        return index;
    }

    /** {@inheritDoc} */
    @Override public void removeRow(Session session, Row row) {
        dataModificationId.incrementAndGet();
        index.remove(session, row);
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session session) {
        dataModificationId.incrementAndGet();
        index.truncate(session);
    }

    /** {@inheritDoc} */
    @Override public void addRow(Session session, Row row) {
        dataModificationId.incrementAndGet();
        index.add(session, row);
    }

    /** {@inheritDoc} */
    @Override public void checkSupportAlter() {
        throw DbException.getUnsupportedException("alter");
    }

    /** {@inheritDoc} */
    @Override public TableType getTableType() {
        return TableType.SYSTEM_TABLE;
    }

    /** {@inheritDoc} */
    @Override public Index getScanIndex(Session session) {
        return index;
    }

    /** {@inheritDoc} */
    @Override public Index getUniqueIndex() {
        return index;
    }

    /** {@inheritDoc} */
    @Override public ArrayList<Index> getIndexes() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedExclusively() {
        return !fakeExclusiveSet.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedExclusivelyBy(Session s) {
        return fakeExclusiveSet.contains(s);
    }

    /** {@inheritDoc} */
    @Override public long getMaxDataModificationId() {
        return dataModificationId.get();
    }

    /** {@inheritDoc} */
    @Override public boolean isDeterministic() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean canDrop() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session session) {
        return index.getRowCount(session);
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return index.getRowCountApproximation();
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /**
     * Met index.
     */
    private static class MetaIndex extends BaseIndex {
        /** */
        private final ConcurrentMap<ValueInt, Row> rows = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public void checkRename() {
            throw DbException.getUnsupportedException("rename");
        }

        /** {@inheritDoc} */
        @Override public void close(Session session) {
            // No-op.
        }

        /**
         * @param row Row.
         * @return ID.
         */
        private static ValueInt id(SearchRow row) {
            Value id = row.getValue(ID);

            assert id != null;

            return (ValueInt)id;
        }

        /** {@inheritDoc} */
        @Override public void add(Session session, Row row) {
            rows.put(id(row), row);
        }

        /** {@inheritDoc} */
        @Override public void remove(Session session, Row row) {
            rows.remove(id(row), row);
        }

        /** {@inheritDoc} */
        @Override public Cursor find(Session session, SearchRow first, SearchRow last) {
            if (first == null || last == null || !Objects.equals(id(first), id(last)))
                return new GridH2Cursor(rows.values().iterator());

            return new SingleRowCursor(rows.get(id(first)));
        }

        /** {@inheritDoc} */
        @Override public double getCost(Session session, int[] masks, TableFilter[] filters,
            int filter, SortOrder sortOrder, HashSet<Column> cols) {
            if ((masks[ID] & IndexCondition.EQUALITY) == IndexCondition.EQUALITY)
                return 1;

            return 1000 + rows.size();
        }

        /** {@inheritDoc} */
        @Override public void remove(Session session) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void truncate(Session session) {
            rows.clear();
        }

        /** {@inheritDoc} */
        @Override public boolean canGetFirstOrLast() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Cursor findFirstOrLast(Session session, boolean first) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean needRebuild() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public long getRowCount(Session session) {
            return rows.size();
        }

        /** {@inheritDoc} */
        @Override public long getRowCountApproximation() {
            return getRowCount(null);
        }

        /** {@inheritDoc} */
        @Override public long getDiskSpaceUsed() {
            return 0;
        }
    }
}
