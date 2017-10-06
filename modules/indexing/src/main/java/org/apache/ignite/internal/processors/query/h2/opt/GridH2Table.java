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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.h2.H2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.database.H2RowFactory;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.SpatialIndex;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.SchemaObject;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableBase;
import org.h2.table.TableType;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.LongAdder8;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.KEY_COL;

/**
 * H2 Table implementation.
 */
public class GridH2Table extends TableBase {
    /** Cache context. */
    private final GridCacheContext cctx;

    /** */
    private final H2RowDescriptor desc;

    /** */
    private volatile ArrayList<Index> idxs;

    /** */
    private final int pkIndexPos;

    /** Total number of system indexes. */
    private final int sysIdxsCnt;

    /** */
    private final Map<String, GridH2IndexBase> tmpIdxs = new HashMap<>();

    /** */
    private final ReadWriteLock lock;

    /** */
    private boolean destroyed;

    /** */
    private final ConcurrentMap<Session, Boolean> sessions = new ConcurrentHashMap8<>();

    /** */
    private IndexColumn affKeyCol;

    /** */
    private final LongAdder8 size = new LongAdder8();

    /** */
    private final H2RowFactory rowFactory;

    /** */
    private volatile boolean rebuildFromHashInProgress;

    /** Identifier. */
    private final QueryTable identifier;

    /** Identifier as string. */
    private final String identifierStr;

    /** Flag remove index or not when table will be destroyed. */
    private volatile boolean rmIndex;

    /**
     * Creates table.
     *
     * @param createTblData Table description.
     * @param desc Row descriptor.
     * @param rowFactory Row factory.
     * @param idxsFactory Indexes factory.
     * @param cctx Cache context.
     */
    public GridH2Table(CreateTableData createTblData, H2RowDescriptor desc, H2RowFactory rowFactory,
        GridH2SystemIndexFactory idxsFactory, GridCacheContext cctx) {
        super(createTblData);

        assert idxsFactory != null;

        this.desc = desc;
        this.cctx = cctx;

        if (desc.context() != null && !desc.context().customAffinityMapper()) {
            boolean affinityColExists = true;

            String affKey = desc.type().affinityKey();

            int affKeyColId = -1;

            if (affKey != null) {
                if (doesColumnExist(affKey))
                    affKeyColId = getColumn(affKey).getColumnId();
                else
                    affinityColExists = false;
            }
            else
                affKeyColId = KEY_COL;

            if (affinityColExists) {
                affKeyCol = indexColumn(affKeyColId, SortOrder.ASCENDING);

                assert affKeyCol != null;
            }
        }

        this.rowFactory = rowFactory;

        identifier = new QueryTable(getSchema().getName(), getName());

        identifierStr = identifier.schema() + "." + identifier.table();

        // Indexes must be created in the end when everything is ready.
        idxs = idxsFactory.createSystemIndexes(this);

        assert idxs != null;

        List<Index> clones = new ArrayList<>(idxs.size());
        for (Index index : idxs) {
            Index clone = createDuplicateIndexIfNeeded(index);
            if (clone != null)
                clones.add(clone);
        }
        idxs.addAll(clones);

        boolean hasHashIndex = idxs.size() >= 2 && index(0).getIndexType().isHash();

        // Add scan index at 0 which is required by H2.
        if (hasHashIndex)
            idxs.add(0, new GridH2PrimaryScanIndex(this, index(1), index(0)));
        else
            idxs.add(0, new GridH2PrimaryScanIndex(this, index(0), null));

        pkIndexPos = hasHashIndex ? 2 : 1;

        sysIdxsCnt = idxs.size();

        lock = new ReentrantReadWriteLock();
    }

    /**
     * @return {@code true} If this is a partitioned table.
     */
    public boolean isPartitioned() {
        return desc != null && desc.context().config().getCacheMode() == PARTITIONED;
    }

    /**
     * @return Affinity key column or {@code null} if not available.
     */
    @Nullable public IndexColumn getAffinityKeyColumn() {
        return affKeyCol;
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
     * @return Cache name.
     */
    public String cacheName() {
        return cctx.name();
    }

    /**
     * @return Cache context.
     */
    public GridCacheContext cache() {
        return cctx;
    }

    /** {@inheritDoc} */
    @Override public boolean lock(Session ses, boolean exclusive, boolean force) {
        Boolean putRes = sessions.putIfAbsent(ses, exclusive);

        // In accordance with base method semantics, we'll return true if we were already exclusively locked
        if (putRes != null)
            return putRes;

        ses.addLock(this);

        lock(exclusive);

        if (destroyed) {
            unlock(exclusive);

            throw new IllegalStateException("Table " + identifierString() + " already destroyed.");
        }

        return false;
    }

    /**
     * @return Table identifier.
     */
    public QueryTable identifier() {
        return identifier;
    }

    /**
     * @return Table identifier as string.
     */
    public String identifierString() {
        return identifierStr;
    }

    /**
     * Acquire table lock.
     *
     * @param exclusive Exclusive flag.
     */
    private void lock(boolean exclusive) {
        Lock l = exclusive ? lock.writeLock() : lock.readLock();

        try {
            l.lockInterruptibly();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedException("Thread got interrupted while trying to acquire table lock.", e);
        }
    }

    /**
     * Release table lock.
     *
     * @param exclusive Exclusive flag.
     */
    private void unlock(boolean exclusive) {
        Lock l = exclusive ? lock.writeLock() : lock.readLock();

        l.unlock();
    }

    /**
     * Check if table is not destroyed.
     */
    private void ensureNotDestroyed() {
        if (destroyed)
            throw new IllegalStateException("Table " + identifierString() + " already destroyed.");
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    @Override public void removeChildrenAndResources(Session ses) {
        lock(true);

        try {
            super.removeChildrenAndResources(ses);

            // Clear all user indexes registered in schema.
            while (idxs.size() > sysIdxsCnt) {
                Index idx = idxs.get(sysIdxsCnt);

                if (idx.getName() != null && idx.getSchema().findIndex(ses, idx.getName()) == idx) {
                    // This call implicitly removes both idx and its proxy, if any, from idxs.
                    database.removeSchemaObject(ses, idx);

                    // We have to call destroy here if we are who has removed this index from the table.
                    if (idx instanceof GridH2IndexBase)
                        ((GridH2IndexBase)idx).destroy(rmIndex);
                }
            }

            if (SysProperties.CHECK) {
                for (SchemaObject obj : database.getAllSchemaObjects(DbObject.INDEX)) {
                    Index idx = (Index) obj;
                    if (idx.getTable() == this)
                        DbException.throwInternalError("index not dropped: " + idx.getName());
                }
            }

            database.removeMeta(ses, getId());
            invalidate();

        }
        finally {
            unlock(true);
        }
    }

    /**
     * Destroy the table.
     */
    public void destroy() {
        lock(true);

        try {
            ensureNotDestroyed();

            assert sessions.isEmpty() : sessions;

            destroyed = true;

            for (int i = 1, len = idxs.size(); i < len; i++)
                if (idxs.get(i) instanceof GridH2IndexBase)
                    index(i).destroy(rmIndex);
        }
        finally {
            unlock(true);
        }
    }

    /**
     * If flag {@code True}, index will be destroyed when table {@link #destroy()}.
     *
     * @param rmIndex Flag indicate remove index on destroy or not.
     */
    public void setRemoveIndexOnDestroy(boolean rmIndex){
        this.rmIndex = rmIndex;
    }

    /** {@inheritDoc} */
    @Override public void unlock(Session ses) {
        Boolean exclusive = sessions.remove(ses);

        if (exclusive == null)
            return;

        unlock(exclusive);
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
    public boolean update(KeyCacheObject key,
        int partId,
        CacheObject val,
        GridCacheVersion ver,
        long expirationTime,
        boolean rmv,
        long link)
        throws IgniteCheckedException {
        assert desc != null;

        GridH2Row row = desc.createRow(key, partId, val, ver, expirationTime);

        row.link = link;

        if (!rmv)
            ((GridH2KeyValueRowOnheap)row).valuesCache(new Value[getColumns().length]);

        try {
            return doUpdate(row, rmv);
        }
        finally {
            if (!rmv)
                ((GridH2KeyValueRowOnheap)row).valuesCache(null);
        }
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
    private GridH2IndexBase pk() {
        return (GridH2IndexBase)idxs.get(2);
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
        lock(false);

        try {
            ensureNotDestroyed();

            GridH2IndexBase pk = pk();

            if (!del) {
                assert rowFactory == null || row.link != 0 : row;

                GridH2Row old = pk.put(row); // Put to PK.

                if (old == null)
                    size.increment();

                int len = idxs.size();

                int i = pkIndexPos;

                // Put row if absent to all indexes sequentially.
                // Start from 3 because 0 - Scan (don't need to update), 1 - PK hash (already updated), 2 - PK (already updated).
                while (++i < len) {
                    if (!(idxs.get(i) instanceof GridH2IndexBase))
                        continue;
                    GridH2IndexBase idx = index(i);

                    addToIndex(idx, pk, row, old, false);
                }

                for (GridH2IndexBase idx : tmpIdxs.values())
                    addToIndex(idx, pk, row, old, true);
            }
            else {
                //  index(1) is PK, get full row from there (search row here contains only key but no other columns).
                GridH2Row old = pk.remove(row);

                if (old != null) {
                    // Remove row from all indexes.
                    // Start from 3 because 0 - Scan (don't need to update), 1 - PK hash (already updated), 2 - PK (already updated).
                    for (int i = pkIndexPos + 1, len = idxs.size(); i < len; i++) {
                        if (!(idxs.get(i) instanceof GridH2IndexBase))
                            continue;
                        Row res = index(i).remove(old);

                        assert eq(pk, res, old) : "\n" + old + "\n" + res + "\n" + i + " -> " + index(i).getName();
                    }

                    for (GridH2IndexBase idx : tmpIdxs.values())
                        idx.remove(old);

                    size.decrement();
                }
                else
                    return false;
            }

            return true;
        }
        finally {
            unlock(false);
        }
    }

    /**
     * Add row to index.
     *
     * @param idx Index to add row to.
     * @param pk Primary key index.
     * @param row Row to add to index.
     * @param old Previous row state, if any.
     * @param tmp {@code True} if this is proposed index which may be not consistent yet.
     */
    private void addToIndex(GridH2IndexBase idx, Index pk, GridH2Row row, GridH2Row old, boolean tmp) {
        assert !idx.getIndexType().isUnique() : "Unique indexes are not supported: " + idx;

        GridH2Row old2 = idx.put(row);

        if (old2 != null) { // Row was replaced in index.
            if (!tmp) {
                if (!eq(pk, old2, old))
                    throw new IllegalStateException("Row conflict should never happen, unique indexes are " +
                        "not supported [idx=" + idx + ", old=" + old + ", old2=" + old2 + ']');
            }
        }
        else if (old != null) // Row was not replaced, need to remove manually.
            idx.removex(old);
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
        ArrayList<GridH2IndexBase> res = new ArrayList<>(idxs.size() - 2);

        for (int i = pkIndexPos, len = idxs.size(); i < len; i++) {
            if (idxs.get(i) instanceof GridH2IndexBase)
                res.add(index(i));
        }

        return res;
    }

    /**
     *
     */
    public void markRebuildFromHashInProgress(boolean value) {
        assert !value || (idxs.size() >= 2 && index(1).getIndexType().isHash()) : "Table has no hash index.";

        rebuildFromHashInProgress = value;
    }

    /**
     *
     */
    public boolean rebuildFromHashInProgress() {
        return rebuildFromHashInProgress;
    }

    /** {@inheritDoc} */
    @Override public Index addIndex(Session ses, String idxName, int idxId, IndexColumn[] cols, IndexType idxType,
        boolean create, String idxComment) {
        return commitUserIndex(ses, idxName);
    }

    /**
     * Add index that is in an intermediate state and is still being built, thus is not used in queries until it is
     * promoted.
     *
     * @param idx Index to add.
     * @throws IgniteCheckedException If failed.
     */
    public void proposeUserIndex(Index idx) throws IgniteCheckedException {
        assert idx instanceof GridH2IndexBase;

        lock(true);

        try {
            ensureNotDestroyed();

            for (Index oldIdx : idxs) {
                if (F.eq(oldIdx.getName(), idx.getName()))
                    throw new IgniteCheckedException("Index already exists: " + idx.getName());
            }

            Index oldTmpIdx = tmpIdxs.put(idx.getName(), (GridH2IndexBase)idx);

            assert oldTmpIdx == null;
        }
        finally {
            unlock(true);
        }
    }

    /**
     * Promote temporary index to make it usable in queries.
     *
     * @param ses H2 session.
     * @param idxName Index name.
     * @return Temporary index with given name.
     */
    private Index commitUserIndex(Session ses, String idxName) {
        lock(true);

        try {
            ensureNotDestroyed();

            Index idx = tmpIdxs.remove(idxName);

            assert idx != null;

            Index cloneIdx = createDuplicateIndexIfNeeded(idx);

            ArrayList<Index> newIdxs = new ArrayList<>(
                idxs.size() + ((cloneIdx == null) ? 1 : 2));

            newIdxs.addAll(idxs);

            newIdxs.add(idx);

            if (cloneIdx != null)
                newIdxs.add(cloneIdx);

            idxs = newIdxs;

            database.addSchemaObject(ses, idx);

            if (cloneIdx != null)
                database.addSchemaObject(ses, cloneIdx);

            setModified();

            return idx;
        }
        finally {
            unlock(true);
        }
    }

    /**
     * Remove user index without promoting it.
     *
     * @param idxName Index name.
     */
    public void rollbackUserIndex(String idxName) {
        lock(true);

        try {
            ensureNotDestroyed();

            GridH2IndexBase rmvIdx = tmpIdxs.remove(idxName);

            assert rmvIdx != null;
        }
        finally {
            unlock(true);
        }
    }

    /**
     * Check whether user index with provided name exists.
     *
     * @param idxName Index name.
     * @return {@code True} if exists.
     */
    public boolean containsUserIndex(String idxName) {
        for (int i = 2; i < idxs.size(); i++) {
            Index idx = idxs.get(i);

            if (idx.getName().equalsIgnoreCase(idxName))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void removeIndex(Index h2Idx) {
        throw DbException.getUnsupportedException("must use removeIndex(session, idx)");
    }

    /**
     * Remove the given index from the list.
     *
     * @param h2Idx the index to remove
     */
    public void removeIndex(Session session, Index h2Idx) {
        lock(true);

        try {
            ArrayList<Index> idxs = new ArrayList<>(this.idxs);

            Index targetIdx = (h2Idx instanceof GridH2ProxyIndex) ?
                ((GridH2ProxyIndex)h2Idx).underlyingIndex() : h2Idx;

            for (int i = pkIndexPos; i < idxs.size();) {
                Index idx = idxs.get(i);

                if (idx == targetIdx || (idx instanceof GridH2ProxyIndex &&
                    ((GridH2ProxyIndex)idx).underlyingIndex() == targetIdx)) {

                    idxs.remove(i);

                    if (idx instanceof GridH2ProxyIndex &&
                        idx.getSchema().findIndex(session, idx.getName()) != null)
                        database.removeSchemaObject(session, idx);

                    continue;
                }

                i++;
            }

            this.idxs = idxs;
        }
        finally {
            unlock(true);
        }
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
    @Override public TableType getTableType() {
        return TableType.EXTERNAL_TABLE_ENGINE;
    }

    /** {@inheritDoc} */
    @Override public Index getScanIndex(Session ses) {
        return getIndexes().get(0); // Scan must be always first index.
    }

    /** {@inheritDoc} */
    @Override public Index getUniqueIndex() {
        if (rebuildFromHashInProgress)
            return index(1);
        else
            return index(2);
    }

    /** {@inheritDoc} */
    @Override public ArrayList<Index> getIndexes() {
        if (!rebuildFromHashInProgress)
            return idxs;

        ArrayList<Index> idxs = new ArrayList<>(2);

        idxs.add(this.idxs.get(0));
        idxs.add(this.idxs.get(1));

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
        return size.longValue();
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
     * @return Data store.
     */
    public H2RowFactory rowFactory() {
        return rowFactory;
    }

    /**
     * Creates proxy index for given target index.
     * Proxy index refers to alternative key and val columns.
     *
     * @param target Index to clone.
     * @return Proxy index.
     */
    public Index createDuplicateIndexIfNeeded(Index target) {
        if (!(target instanceof H2TreeIndex) && !(target instanceof SpatialIndex))
            return null;

        IndexColumn[] cols = target.getIndexColumns();

        List<IndexColumn> proxyCols = new ArrayList<>(cols.length);

        boolean modified = false;

        for (IndexColumn col : cols) {
            IndexColumn proxyCol = new IndexColumn();

            proxyCol.columnName = col.columnName;
            proxyCol.column = col.column;
            proxyCol.sortType = col.sortType;

            int altColId = desc.getAlternativeColumnId(proxyCol.column.getColumnId());

            if (altColId != proxyCol.column.getColumnId()) {
                proxyCol.column = getColumn(altColId);

                proxyCol.columnName = proxyCol.column.getName();

                modified = true;
            }

            proxyCols.add(proxyCol);
        }

        if (modified) {
            String proxyName = target.getName() + "_proxy";

            if (target.getIndexType().isSpatial())
                return new GridH2ProxySpatialIndex(this, proxyName, proxyCols, target);

            return new GridH2ProxyIndex(this, proxyName, proxyCols, target);
        }

        return null;
    }

    /**
     * Add new columns to this table.
     *
     * @param cols Columns to add.
     * @param ifNotExists Ignore this command if {@code cols} has size of 1 and column with given name already exists.
     */
    public void addColumns(List<QueryField> cols, boolean ifNotExists) {
        assert !ifNotExists || cols.size() == 1;

        lock(true);

        try {
            int pos = columns.length;

            Column[] newCols = new Column[columns.length + cols.size()];

            // First, let's copy existing columns to new array
            System.arraycopy(columns, 0, newCols, 0, columns.length);

            // And now, let's add new columns
            for (QueryField col : cols) {
                if (doesColumnExist(col.name())) {
                    if (ifNotExists && cols.size() == 1)
                        return;
                    else
                        throw new IgniteSQLException("Column already exists [tblName=" + getName() +
                            ", colName=" + col.name() + ']');
                }

                try {
                    Column c = new Column(col.name(), DataType.getTypeFromClass(Class.forName(col.typeName())));

                    c.setNullable(col.isNullable());

                    newCols[pos++] = c;
                }
                catch (ClassNotFoundException e) {
                    throw new IgniteSQLException("H2 data type not found for class: " + col.typeName(), e);
                }
            }

            setColumns(newCols);

            desc.refreshMetadataFromTypeDescriptor();

            setModified();
        }
        finally {
            unlock(true);
        }
    }
}