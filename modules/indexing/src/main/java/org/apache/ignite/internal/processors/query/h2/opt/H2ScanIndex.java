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
import java.util.HashSet;

import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexLookupBatch;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;

/**
 * Scan index. Do not actually store any information, but rather delegate to some underlying index. The only reason
 * why this index exists is H2 requirement that every table must have the very first index with
 * {@link IndexType#isScan} set to {@code true}. See {@link #TYPE}.
 */
public class H2ScanIndex<D extends BaseIndex> extends BaseIndex {
    /** Type of this index. */
    private static final IndexType TYPE = IndexType.createScan(false);

    /** Underlying index. */
    private final D delegate;

    /**
     * @param delegate Delegate.
     */
    public H2ScanIndex(D delegate) {
        this.delegate = delegate;
    }

    /**
     * @return Delegate.
     */
    protected D delegate() {
        return delegate;
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void add(Session ses, Row row) {
        delegate().add(ses, row);
    }

    /** {@inheritDoc} */
    @Override public boolean canFindNext() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean canScan() {
        return delegate().canScan();
    }

    /** {@inheritDoc} */
    @Override public final void close(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void commit(int operation, Row row) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int compareRows(SearchRow rowData, SearchRow compare) {
        return delegate().compareRows(rowData, compare);
    }

    /** {@inheritDoc} */
    @Override public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return find(filter.getSession(), first, last);
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow first, SearchRow last) {
        return delegate().find(ses, null, null);
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        throw DbException.getUnsupportedException("SCAN");
    }

    /** {@inheritDoc} */
    @Override public Cursor findNext(Session ses, SearchRow higherThan, SearchRow last) {
        throw DbException.throwInternalError();
    }

    /** {@inheritDoc} */
    @Override public int getColumnIndex(Column col) {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public Column[] getColumns() {
        return delegate().getColumns();
    }

    /** {@inheritDoc} */
    @Override public IndexColumn[] getIndexColumns() {
        return delegate().getIndexColumns();
    }

    /** {@inheritDoc} */
    @Override public IndexType getIndexType() {
        return TYPE;
    }

    /** {@inheritDoc} */
    @Override public Row getRow(Session ses, long key) {
        return delegate().getRow(ses, key);
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        return delegate().getRowCount(ses);
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return delegate().getRowCountApproximation();
    }

    /** {@inheritDoc} */
    @Override public Table getTable() {
        return delegate().getTable();
    }

    /** {@inheritDoc} */
    @Override public boolean isRowIdIndex() {
        return delegate().isRowIdIndex();
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses, Row row) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void setSortedInsertMode(boolean sortedInsertMode) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IndexLookupBatch createLookupBatch(TableFilter[] filters, int filter) {
        return delegate().createLookupBatch(filters, filter);
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Schema getSchema() {
        return delegate().getSchema();
    }

    /** {@inheritDoc} */
    @Override public boolean isHidden() {
        return delegate().isHidden();
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public ArrayList<DbObject> getChildren() {
        return delegate().getChildren();
    }

    /** {@inheritDoc} */
    @Override public String getComment() {
        return delegate().getComment();
    }

    /** {@inheritDoc} */
    @Override public String getCreateSQL() {
        return null; // Scan should return null.
    }

    /** {@inheritDoc} */
    @Override public String getCreateSQLForCopy(Table tbl, String quotedName) {
        return delegate().getCreateSQLForCopy(tbl, quotedName);
    }

    /** {@inheritDoc} */
    @Override public Database getDatabase() {
        return delegate().getDatabase();
    }

    /** {@inheritDoc} */
    @Override public String getDropSQL() {
        return delegate().getDropSQL();
    }

    /** {@inheritDoc} */
    @Override public int getId() {
        return delegate().getId();
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return delegate().getSQL();
    }

    /** {@inheritDoc} */
    @Override public int getType() {
        return delegate().getType();
    }

    /** {@inheritDoc} */
    @Override public boolean isTemporary() {
        return delegate().isTemporary();
    }

    /** {@inheritDoc} */
    @Override public void removeChildrenAndResources(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void rename(String newName) {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public void setComment(String comment) {
        throw DbException.getUnsupportedException("comment");
    }

    /** {@inheritDoc} */
    @Override public void setTemporary(boolean temporary) {
        throw DbException.getUnsupportedException("temporary");
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session session, int[] masks, TableFilter[] filters, int filter,
        SortOrder sortOrder, HashSet<Column> allColumnsSet) {
        long rows = getRowCountApproximation();

        return getCostRangeIndex(masks, rows, filters, filter, sortOrder, true, allColumnsSet);
    }
}
