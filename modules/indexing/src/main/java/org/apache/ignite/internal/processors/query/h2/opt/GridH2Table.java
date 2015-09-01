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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableBase;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * H2 Table implementation.
 */
public class GridH2Table extends TableBase {
    /** */
    private final String spaceName;

    /** */
    private final GridH2RowDescriptor desc;

    /** */
    private final ArrayList<Index> idxs;

    /** */
    private final ReadWriteLock lock;

    /** */
    private final Set<Session> sessions = Collections.newSetFromMap(new ConcurrentHashMap8<Session, Boolean>());

    /** */
    private volatile Object[] actualSnapshot;

    /**
     * Creates table.
     *
     * @param createTblData Table description.
     * @param desc Row descriptor.
     * @param idxsFactory Indexes factory.
     * @param spaceName Space name.
     */
    public GridH2Table(CreateTableData createTblData, @Nullable GridH2RowDescriptor desc, IndexesFactory idxsFactory,
        @Nullable String spaceName) {
        super(createTblData);

        assert idxsFactory != null;

        this.desc = desc;
        this.spaceName = spaceName;

        idxs = idxsFactory.createIndexes(this);

        assert idxs != null;
        assert idxs.size() >= 1;

        lock =  new ReentrantReadWriteLock();

        // Add scan index at 0 which is required by H2.
        idxs.add(0, new ScanIndex(index(0)));
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /**
     * @return Row descriptor.
     */
    public GridH2RowDescriptor rowDescriptor() {
        return desc;
    }

    /**
     * Should be called when entry is swapped.
     *
     * @param key Entry key.
     * @return {@code true} If row was found.
     * @throws IgniteCheckedException If failed.
     */
    public boolean onSwap(CacheObject key) throws IgniteCheckedException {
        return onSwapUnswap(key, null);
    }

    /**
     * Should be called when entry is unswapped.
     *
     * @param key Key.
     * @param val Value.
     * @return {@code true} If row was found.
     * @throws IgniteCheckedException If failed.
     */
    public boolean onUnswap(CacheObject key, CacheObject val) throws IgniteCheckedException {
        assert val != null : "Key=" + key;

        return onSwapUnswap(key, val);
    }

    /**
     * Swaps or unswaps row.
     *
     * @param key Key.
     * @param val Value for promote or {@code null} if we have to swap.
     * @return {@code true} if row was found and swapped/unswapped.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    private boolean onSwapUnswap(CacheObject key, @Nullable CacheObject val) throws IgniteCheckedException {
        assert key != null;

        GridH2TreeIndex pk = pk();

        assert desc != null;

        GridH2Row searchRow = desc.createRow(key, null, 0);

        GridUnsafeMemory mem = desc.memory();

        lock.readLock().lock();

        if (mem != null)
            desc.guard().begin();

        try {
            GridH2AbstractKeyValueRow row = (GridH2AbstractKeyValueRow)pk.findOne(searchRow);

            if (row == null)
                return false;

            if (val == null)
                row.onSwap();
            else
                row.onUnswap(val, false);

            return true;
        }
        finally {
            lock.readLock().unlock();

            if (mem != null)
                desc.guard().end();
        }
    }

    /**
     * @return Space name.
     */
    @Nullable String spaceName() {
        return spaceName;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased", "SynchronizationOnLocalVariableOrMethodParameter", "unchecked"})
    @Override public void lock(@Nullable final Session ses, boolean exclusive, boolean force) {
        if (ses != null) {
            if (!sessions.add(ses))
                return;

            synchronized (ses) {
                ses.addLock(this);
            }
        }

        Object[] snapshot;

        for (long waitTime = 100;; waitTime *= 2) { // Increase wait time to avoid starvation.
            snapshot = actualSnapshot;

            if (snapshot != null) {
                // Reuse existing snapshot without locking.
                for (int i = 1, len = idxs.size(); i < len; i++)
                    index(i).takeSnapshot(snapshot[i - 1]);

                return;
            }

            try {
                if (lock.writeLock().tryLock(waitTime, TimeUnit.MILLISECONDS))
                    break;
            }
            catch (InterruptedException e) {
                throw new IgniteException("Thread got interrupted while trying to acquire index lock.", e);
            }
        }

        boolean snapshoted = false;

        try {
            snapshot = actualSnapshot; // Try again inside of the lock.

            if (snapshot == null) {
                snapshot = takeIndexesSnapshot();

                if (desc == null || desc.memory() == null) // This optimization is disabled for off-heap index.
                    actualSnapshot = snapshot;

                snapshoted = true;
            }
        }
        finally {
            lock.writeLock().unlock();
        }

        if (!snapshoted) {
            for (int i = 1, len = idxs.size(); i < len; i++)
                index(i).takeSnapshot(snapshot[i - 1]);
        }
    }

    /**
     * Must be called inside of write lock because when using multiple indexes we have to ensure that all of them have
     * the same contents at snapshot taking time.
     *
     * @return New indexes data snapshot.
     */
    @SuppressWarnings("unchecked")
    private Object[] takeIndexesSnapshot() {
        int len = idxs.size();

        Object[] snapshot = new ConcurrentNavigableMap[len - 1];

        for (int i = 1; i < len; i++) { // Take snapshots on all except first which is scan.
            Object s = index(i).takeSnapshot(null);

            snapshot[i - 1] = s;
        }

        return snapshot;
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        assert !sessions.contains(ses);
    }

    /** {@inheritDoc} */
    @Override public void unlock(@Nullable Session ses) {
        if (ses != null) {
            boolean res = sessions.remove(ses);

            assert res;
        }

        for (int i = 1, len = idxs.size(); i < len; i++)  // Release snapshots on all except first which is scan.
            index(i).releaseSnapshot();
    }

    /**
     * Closes table and releases resources.
     */
    public void close() {
        Lock l = lock.writeLock();

        l.lock();

        try {
            for (int i = 1, len = idxs.size(); i < len; i++)
                index(i).close(null);
        }
        finally {
            l.unlock();
        }
    }

    /**
     * Updates table for given key. If value is null then row with given key will be removed from table,
     * otherwise value and expiration time will be updated or new row will be added.
     *
     * @param key Key.
     * @param val Value.
     * @param expirationTime Expiration time.
     * @param rmv If {@code true} then remove, else update row.
     * @return {@code true} If operation succeeded.
     * @throws IgniteCheckedException If failed.
     */
    public boolean update(CacheObject key, CacheObject val, long expirationTime, boolean rmv)
        throws IgniteCheckedException {
        assert desc != null;

        GridH2Row row = desc.createRow(key, val, expirationTime);

        return doUpdate(row, rmv);
    }

    /**
     * Gets index by index.
     *
     * @param idx Index in list.
     * @return Index.
     */
    private GridH2IndexBase index(int idx) {
        return (GridH2IndexBase)idxs.get(idx);
    }

    /**
     * Gets primary key.
     *
     * @return Primary key.
     */
    private GridH2TreeIndex pk() {
        return (GridH2TreeIndex)idxs.get(1);
    }

    /**
     * For testing only.
     *
     * @param row Row.
     * @param del If given row should be deleted from table.
     * @return {@code True} if operation succeeded.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    boolean doUpdate(final GridH2Row row, boolean del) throws IgniteCheckedException {
        // Here we assume that each key can't be updated concurrently and case when different indexes
        // getting updated from different threads with different rows with the same key is impossible.
        GridUnsafeMemory mem = desc == null ? null : desc.memory();

        lock.readLock().lock();

        if (mem != null)
            desc.guard().begin();

        try {
            GridH2TreeIndex pk = pk();

            if (!del) {
                GridH2Row old = pk.put(row); // Put to PK.

                int len = idxs.size();

                int i = 1;

                // Put row if absent to all indexes sequentially.
                // Start from 2 because 0 - Scan (don't need to update), 1 - PK (already updated).
                while (++i < len) {
                    GridH2IndexBase idx = index(i);

                    assert !idx.getIndexType().isUnique() : "Unique indexes are not supported.";

                    GridH2Row old2 = idx.put(row);

                    if (old2 != null) { // Row was replaced in index.
                        if (!eq(pk, old2, old))
                            throw new IllegalStateException("Row conflict should never happen, unique indexes are " +
                                "not supported.");
                    }
                    else if (old != null) // Row was not replaced, need to remove manually.
                        idx.remove(old);
                }
            }
            else {
                //  index(1) is PK, get full row from there (search row here contains only key but no other columns).
                GridH2Row old = pk.remove(row);

                if (old instanceof GridH2AbstractKeyValueRow) { // Unswap value.
                    Value v = row.getValue(GridH2AbstractKeyValueRow.VAL_COL);

                    if (v != null)
                        ((GridH2AbstractKeyValueRow)old).onUnswap(v.getObject(), true);
                }

                if (old != null) {
                    // Remove row from all indexes.
                    // Start from 2 because 0 - Scan (don't need to update), 1 - PK (already updated).
                    for (int i = 2, len = idxs.size(); i < len; i++) {
                        Row res = index(i).remove(old);

                        assert eq(pk, res, old): "\n" + old + "\n" + res + "\n" + i + " -> " + index(i).getName();
                    }
                }
                else
                    return false;
            }

            // The snapshot is not actual after update.
            actualSnapshot = null;

            return true;
        }
        finally {
            lock.readLock().unlock();

            if (mem != null)
                desc.guard().end();
        }
    }

    /**
     * Check row equality.
     *
     * @param pk Primary key index.
     * @param r1 First row.
     * @param r2 Second row.
     * @return {@code true} if rows are the same.
     */
    private static boolean eq(Index pk, SearchRow r1, SearchRow r2) {
        return r1 == r2 || (r1 != null && r2 != null && pk.compareRows(r1, r2) == 0);
    }

    /**
     * For testing only.
     *
     * @return Indexes.
     */
    ArrayList<GridH2IndexBase> indexes() {
        ArrayList<GridH2IndexBase> res = new ArrayList<>(idxs.size() - 1);

        for (int i = 1, len = idxs.size(); i < len ; i++)
            res.add(index(i));

        return res;
    }

    /**
     * Rebuilds all indexes of this table.
     */
    public void rebuildIndexes() {
        GridUnsafeMemory memory = desc == null ? null : desc.memory();

        lock.writeLock().lock();

        try {
            if (memory == null && actualSnapshot == null)
                actualSnapshot = takeIndexesSnapshot(); // Allow read access while we are rebuilding indexes.

            for (int i = 1, len = idxs.size(); i < len; i++) {
                GridH2IndexBase newIdx = index(i).rebuild();

                idxs.set(i, newIdx);

                if (i == 1) // ScanIndex at 0 and actualSnapshot can contain references to old indexes, reset them.
                    idxs.set(0, new ScanIndex(newIdx));
            }
        }
        catch (InterruptedException ignored) {
            // No-op.
        }
        finally {
            lock.writeLock().unlock();

            actualSnapshot = null;
        }
    }

    /** {@inheritDoc} */
    @Override public Index addIndex(Session ses, String s, int i, IndexColumn[] idxCols, IndexType idxType,
        boolean b, String s1) {
        throw DbException.getUnsupportedException("addIndex");
    }

    /** {@inheritDoc} */
    @Override public void removeRow(Session ses, Row row) {
        throw DbException.getUnsupportedException("removeRow");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public void addRow(Session ses, Row row) {
        throw DbException.getUnsupportedException("addRow");
    }

    /** {@inheritDoc} */
    @Override public void checkSupportAlter() {
        throw DbException.getUnsupportedException("alter");
    }

    /** {@inheritDoc} */
    @Override public String getTableType() {
        return EXTERNAL_TABLE_ENGINE;
    }

    /** {@inheritDoc} */
    @Override public Index getScanIndex(Session ses) {
        return getIndexes().get(0); // Scan must be always first index.
    }

    /** {@inheritDoc} */
    @Override public Index getUniqueIndex() {
        return getIndexes().get(1); // PK index is always second.
    }

    /** {@inheritDoc} */
    @Override public ArrayList<Index> getIndexes() {
        return idxs;
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedExclusively() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedExclusivelyBy(Session ses) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long getMaxDataModificationId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isDeterministic() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean canDrop() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(@Nullable Session ses) {
        return getUniqueIndex().getRowCount(ses);
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return getUniqueIndex().getRowCountApproximation();
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /**
     * Creates index column for table.
     *
     * @param col Column index.
     * @param sorting Sorting order {@link SortOrder}
     * @return Created index column.
     */
    public IndexColumn indexColumn(int col, int sorting) {
        IndexColumn res = new IndexColumn();

        res.column = getColumn(col);
        res.columnName = res.column.getName();
        res.sortType = sorting;

        return res;
    }

    /**
     * H2 Table engine.
     */
    @SuppressWarnings({"PublicInnerClass", "FieldAccessedSynchronizedAndUnsynchronized"})
    public static class Engine implements TableEngine {
        /** */
        private static GridH2RowDescriptor rowDesc;

        /** */
        private static IndexesFactory idxsFactory;

        /** */
        private static GridH2Table resTbl;

        /** */
        private static String spaceName;

        /** {@inheritDoc} */
        @Override public TableBase createTable(CreateTableData createTblData) {
            resTbl = new GridH2Table(createTblData, rowDesc, idxsFactory, spaceName);

            return resTbl;
        }

        /**
         * Creates table using given connection, DDL clause for given type descriptor and list of indexes.
         *
         * @param conn Connection.
         * @param sql DDL clause.
         * @param desc Row descriptor.
         * @param factory Indexes factory.
         * @param space Space name.
         * @throws SQLException If failed.
         * @return Created table.
         */
        public static synchronized GridH2Table createTable(Connection conn, String sql,
            @Nullable GridH2RowDescriptor desc, IndexesFactory factory, String space)
            throws SQLException {
            rowDesc = desc;
            idxsFactory = factory;
            spaceName = space;

            try {
                try (Statement s = conn.createStatement()) {
                    s.execute(sql + " engine \"" + Engine.class.getName() + "\"");
                }

                return resTbl;
            }
            finally {
                resTbl = null;
                idxsFactory = null;
                rowDesc = null;
            }
        }
    }

    /**
     * Type which can create indexes list for given table.
     */
    @SuppressWarnings({"PackageVisibleInnerClass", "PublicInnerClass"})
    public static interface IndexesFactory {
        /**
         * Create list of indexes. First must be primary key, after that all unique indexes and
         * only then non-unique indexes.
         * All indexes must be subtypes of {@link GridH2TreeIndex}.
         *
         * @param tbl Table to create indexes for.
         * @return List of indexes.
         */
        ArrayList<Index> createIndexes(GridH2Table tbl);
    }

    /**
     * Wrapper type for primary key.
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class ScanIndex implements Index {
        /** */
        static final String SCAN_INDEX_NAME_SUFFIX = "__SCAN_";

        /** */
        private static final IndexType TYPE = IndexType.createScan(false);

        /** */
        private final GridH2IndexBase delegate;

        /**
         * Constructor.
         *
         * @param delegate Index delegate to.
         */
        private ScanIndex(GridH2IndexBase delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public long getDiskSpaceUsed() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void add(Session ses, Row row) {
            delegate.add(ses, row);
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
            return delegate.canScan();
        }

        /** {@inheritDoc} */
        @Override public void close(Session ses) {
            delegate.close(ses);
        }

        /** {@inheritDoc} */
        @Override public void commit(int operation, Row row) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int compareRows(SearchRow rowData, SearchRow compare) {
            return delegate.compareRows(rowData, compare);
        }

        /** {@inheritDoc} */
        @Override public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
            return find(filter.getSession(), first, last);
        }

        /** {@inheritDoc} */
        @Override public Cursor find(Session ses, SearchRow first, SearchRow last) {
            return delegate.find(ses, null, null);
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
            return delegate.getColumns();
        }

        /** {@inheritDoc} */
        @Override public double getCost(Session ses, int[] masks, TableFilter tblFilter, SortOrder sortOrder) {
            return getRowCountApproximation() + Constants.COST_ROW_OFFSET;
        }

        /** {@inheritDoc} */
        @Override public IndexColumn[] getIndexColumns() {
            return delegate.getIndexColumns();
        }

        /** {@inheritDoc} */
        @Override public IndexType getIndexType() {
            return TYPE;
        }

        /** {@inheritDoc} */
        @Override public String getPlanSQL() {
            return delegate.getTable().getSQL() + "." + SCAN_INDEX_NAME_SUFFIX;
        }

        /** {@inheritDoc} */
        @Override public Row getRow(Session ses, long key) {
            return delegate.getRow(ses, key);
        }

        /** {@inheritDoc} */
        @Override public long getRowCount(Session ses) {
            return delegate.getRowCount(ses);
        }

        /** {@inheritDoc} */
        @Override public long getRowCountApproximation() {
            return delegate.getRowCountApproximation();
        }

        /** {@inheritDoc} */
        @Override public Table getTable() {
            return delegate.getTable();
        }

        /** {@inheritDoc} */
        @Override public boolean isRowIdIndex() {
            return delegate.isRowIdIndex();
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
        @Override public void truncate(Session ses) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Schema getSchema() {
            return delegate.getSchema();
        }

        /** {@inheritDoc} */
        @Override public boolean isHidden() {
            return delegate.isHidden();
        }

        /** {@inheritDoc} */
        @Override public void checkRename() {
            throw DbException.getUnsupportedException("rename");
        }

        /** {@inheritDoc} */
        @Override public ArrayList<DbObject> getChildren() {
            return delegate.getChildren();
        }

        /** {@inheritDoc} */
        @Override public String getComment() {
            return delegate.getComment();
        }

        /** {@inheritDoc} */
        @Override public String getCreateSQL() {
            return null; // Scan should return null.
        }

        /** {@inheritDoc} */
        @Override public String getCreateSQLForCopy(Table tbl, String quotedName) {
            return delegate.getCreateSQLForCopy(tbl, quotedName);
        }

        /** {@inheritDoc} */
        @Override public Database getDatabase() {
            return delegate.getDatabase();
        }

        /** {@inheritDoc} */
        @Override public String getDropSQL() {
            return delegate.getDropSQL();
        }

        /** {@inheritDoc} */
        @Override public int getId() {
            return delegate.getId();
        }

        /** {@inheritDoc} */
        @Override public String getName() {
            return delegate.getName() + SCAN_INDEX_NAME_SUFFIX;
        }

        /** {@inheritDoc} */
        @Override public String getSQL() {
            return delegate.getSQL();
        }

        /** {@inheritDoc} */
        @Override public int getType() {
            return delegate.getType();
        }

        /** {@inheritDoc} */
        @Override public boolean isTemporary() {
            return delegate.isTemporary();
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
    }
}