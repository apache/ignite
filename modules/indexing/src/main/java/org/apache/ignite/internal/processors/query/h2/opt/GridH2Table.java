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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.h2.database.H2RowFactory;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.command.ddl.CreateTableData;
import org.h2.command.dml.Insert;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.SpatialIndex;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SortOrder;
import org.h2.schema.SchemaObject;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableBase;
import org.h2.table.TableType;
import org.h2.value.DataType;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.DEFAULT_COLUMNS_COUNT;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.KEY_COL;

/**
 * H2 Table implementation.
 */
public class GridH2Table extends TableBase {
    /** Insert hack flag. */
    private static final ThreadLocal<Boolean> INSERT_HACK = new ThreadLocal<>();

    /** Cache context. */
    private final GridCacheContext cctx;

    /** */
    private final GridH2RowDescriptor desc;

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
    private final ConcurrentMap<Session, Boolean> sessions = new ConcurrentHashMap<>();

    /** */
    private IndexColumn affKeyCol;

    /** */
    private final LongAdder size = new LongAdder();

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
    public GridH2Table(CreateTableData createTblData, GridH2RowDescriptor desc, H2RowFactory rowFactory,
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
                if (doesColumnExist(affKey)) {
                    affKeyColId = getColumn(affKey).getColumnId();

                    if (desc.isKeyColumn(affKeyColId))
                        affKeyColId = KEY_COL;
                }
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
     * @return Cache ID.
     */
    public int cacheId() {
        return cctx.cacheId();
    }

    /**
     * @return Cache context.
     */
    public GridCacheContext cache() {
        return cctx;
    }

    /** {@inheritDoc} */
    @Override public boolean lock(Session ses, boolean exclusive, boolean force) {
        // In accordance with base method semantics, we'll return true if we were already exclusively locked.
        Boolean res = sessions.get(ses);

        if (res != null)
            return res;

        // Acquire the lock.
        lock(exclusive);

        if (destroyed) {
            unlock(exclusive);

            throw new IllegalStateException("Table " + identifierString() + " already destroyed.");
        }

        // Mutate state.
        sessions.put(ses, exclusive);

        ses.addLock(this);

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
            if (!exclusive || !GridMapQueryExecutor.FORCE_LAZY)
                l.lockInterruptibly();
            else {
                for (;;) {
                    if (l.tryLock(200, TimeUnit.MILLISECONDS))
                        break;
                    else
                        Thread.yield();
                }
            }
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
     * Updates table for given key. If value is null then row with given key will be removed from table,
     * otherwise value and expiration time will be updated or new row will be added.
     *
     * @param row Row to be updated.
     * @param prevRow Previous row.
     * @param prevRowAvailable Whether previous row is available.
     * @throws IgniteCheckedException If failed.
     */
    public void update(CacheDataRow row, @Nullable CacheDataRow prevRow, boolean prevRowAvailable) throws IgniteCheckedException {
        assert desc != null;

        GridH2KeyValueRowOnheap row0 = (GridH2KeyValueRowOnheap)desc.createRow(row);
        GridH2KeyValueRowOnheap prevRow0 = prevRow != null ? (GridH2KeyValueRowOnheap)desc.createRow(prevRow) :
            null;

        row0.prepareValuesCache();

        if (prevRow0 != null)
            prevRow0.prepareValuesCache();

        try {
            lock(false);

            try {
                ensureNotDestroyed();

                boolean replaced;

                if (prevRowAvailable)
                    replaced = pk().putx(row0);
                else {
                    prevRow0 = (GridH2KeyValueRowOnheap)pk().put(row0);

                    replaced = prevRow0 != null;
                }

                if (!replaced)
                    size.increment();

                for (int i = pkIndexPos + 1, len = idxs.size(); i < len; i++) {
                    Index idx = idxs.get(i);

                    if (idx instanceof GridH2IndexBase)
                        addToIndex((GridH2IndexBase)idx, row0, prevRow0);
                }

                if (!tmpIdxs.isEmpty()) {
                    for (GridH2IndexBase idx : tmpIdxs.values())
                        addToIndex(idx, row0, prevRow0);
                }
            }
            finally {
                unlock(false);
            }
        }
        finally {
            row0.clearValuesCache();

            if (prevRow0 != null)
                prevRow0.clearValuesCache();
        }
    }

    /**
     * Remove row.
     *
     * @param row Row.
     * @return {@code True} if was removed.
     * @throws IgniteCheckedException If failed.
     */
    public boolean remove(CacheDataRow row) throws IgniteCheckedException {
        GridH2Row row0 = desc.createRow(row);

        lock(false);

        try {
            ensureNotDestroyed();

            boolean rmv = pk().removex(row0);

            if (rmv) {
                for (int i = pkIndexPos + 1, len = idxs.size(); i < len; i++) {
                    Index idx = idxs.get(i);

                    if (idx instanceof GridH2IndexBase)
                        ((GridH2IndexBase)idx).removex(row0);
                }

                if (!tmpIdxs.isEmpty()) {
                    for (GridH2IndexBase idx : tmpIdxs.values())
                        idx.removex(row0);
                }

                size.decrement();
            }

            return rmv;
        }
        finally {
            unlock(false);
        }
    }

    /**
     * Add row to index.
     * @param idx Index to add row to.
     * @param row Row to add to index.
     * @param prevRow Previous row state, if any.
     */
    private void addToIndex(GridH2IndexBase idx, GridH2Row row, GridH2Row prevRow) {
        boolean replaced = idx.putx(row);

        // Row was not replaced, need to remove manually.
        if (!replaced && prevRow != null)
            idx.removex(prevRow);
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
    private Index createDuplicateIndexIfNeeded(Index target) {
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

    /**
     *
     * @param cols
     * @param ifExists
     */
    public void dropColumns(List<String> cols, boolean ifExists) {
        assert !ifExists || cols.size() == 1;

        lock(true);

        try {
            int size = columns.length;

            for (String name : cols) {
                if (!doesColumnExist(name)) {
                    if (ifExists && cols.size() == 1)
                        return;
                    else
                        throw new IgniteSQLException("Column does not exist [tblName=" + getName() +
                            ", colName=" + name + ']');
                }

                size --;
            }

            assert size > DEFAULT_COLUMNS_COUNT;

            Column[] newCols = new Column[size];

            int dst = 0;

            for (int i = 0; i < columns.length; i++) {
                Column column = columns[i];

                for (String name : cols) {
                    if (F.eq(name, column.getName())) {
                        column = null;

                        break;
                    }
                }

                if (column != null)
                    newCols[dst++] = column;
            }

            setColumns(newCols);

            desc.refreshMetadataFromTypeDescriptor();

            for (Index idx : getIndexes()) {
                if (idx instanceof GridH2IndexBase)
                    ((GridH2IndexBase)idx).refreshColumnIds();
            }

            setModified();
        }
        finally {
            unlock(true);
        }
    }

    /** {@inheritDoc} */
    @Override public Column[] getColumns() {
        Boolean insertHack = INSERT_HACK.get();

        if (insertHack != null && insertHack) {
            StackTraceElement[] elems = Thread.currentThread().getStackTrace();

            StackTraceElement elem = elems[2];

            if (F.eq(elem.getClassName(), Insert.class.getName()) && F.eq(elem.getMethodName(), "prepare")) {
                Column[] columns0 = new Column[columns.length - 3];

                System.arraycopy(columns, 3, columns0, 0, columns0.length);

                return columns0;
            }
        }

        return columns;
    }

    /**
     * Set insert hack flag.
     *
     * @param val Value.
     */
    public static void insertHack(boolean val) {
        INSERT_HACK.set(val);
    }

    /**
     * Check whether insert hack is required. This is true in case statement contains "INSERT INTO ... VALUES".
     *
     * @param sql SQL statement.
     * @return {@code True} if target combination is found.
     */
    @SuppressWarnings("RedundantIfStatement")
    public static boolean insertHackRequired(String sql) {
        if (F.isEmpty(sql))
            return false;

        sql = sql.toLowerCase();

        int idxInsert = sql.indexOf("insert");

        if (idxInsert < 0)
            return false;

        int idxInto = sql.indexOf("into", idxInsert);

        if (idxInto < 0)
            return false;

        return true;
    }
}