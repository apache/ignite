/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.twostep;

import org.h2.api.*;
import org.h2.command.ddl.*;
import org.h2.engine.*;
import org.h2.index.*;
import org.h2.message.*;
import org.h2.result.*;
import org.h2.table.*;

import java.util.*;

/**
 * Merge table for distributed queries.
 */
public class GridMergeTable extends TableBase {
    /** */
    private final ArrayList<Index> idxs = new ArrayList<>(1);

    /** */
    private final GridMergeIndex idx;

    /**
     * @param data Data.
     */
    public GridMergeTable(CreateTableData data) {
        super(data);

        idx = new GridMergeIndexUnsorted(this, "merge_scan");

        idxs.add(idx);
    }

    /** {@inheritDoc} */
    @Override public void lock(Session session, boolean exclusive, boolean force) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        idx.close(ses);
    }

    /** {@inheritDoc} */
    @Override public void unlock(Session s) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols,
        IndexType indexType, boolean create, String indexComment) {
        throw DbException.getUnsupportedException("addIndex");
    }

    /** {@inheritDoc} */
    @Override public void removeRow(Session session, Row row) {
        throw DbException.getUnsupportedException("removeRow");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session session) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public void addRow(Session session, Row row) {
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
    @Override public GridMergeIndex getScanIndex(Session session) {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public Index getUniqueIndex() {
        return null; // We don't have a PK.
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
    @Override public long getRowCount(Session ses) {
        return idx.getRowCount(ses);
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return idx.getRowCountApproximation();
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
     * Engine.
     */
    public static class Engine implements TableEngine {
        /** */
        private static ThreadLocal<GridMergeTable> createdTbl = new ThreadLocal<>();

        /**
         * @return Created table.
         */
        public static GridMergeTable getCreated() {
            GridMergeTable tbl = createdTbl.get();

            assert tbl != null;

            createdTbl.remove();

            return tbl;
        }

        /** {@inheritDoc} */
        @Override public Table createTable(CreateTableData data) {
            GridMergeTable tbl = new GridMergeTable(data);

            createdTbl.set(tbl);

            return tbl;
        }
    }
}
