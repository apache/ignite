/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2.opt;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.h2.engine.*;
import org.h2.index.*;
import org.h2.message.*;
import org.h2.result.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Index base.
 */
public abstract class GridH2IndexBase extends BaseIndex {
    /** */
    protected static final ThreadLocal<GridIndexingQueryFilter> filters = new ThreadLocal<>();

    /** */
    protected final int keyCol;

    /** */
    protected final int valCol;

    /**
     * @param keyCol Key column.
     * @param valCol Value column.
     */
    protected GridH2IndexBase(int keyCol, int valCol) {
        this.keyCol = keyCol;
        this.valCol = valCol;
    }

    /**
     * Sets key filters for current thread.
     *
     * @param fs Filters.
     */
    public static void setFiltersForThread(GridIndexingQueryFilter fs) {
        filters.set(fs);
    }

    /**
     * If the index supports rebuilding it has to creates its own copy.
     *
     * @return Rebuilt copy.
     * @throws InterruptedException If interrupted.
     */
    public GridH2IndexBase rebuild() throws InterruptedException {
        return this;
    }

    /**
     * Put row if absent.
     *
     * @param row Row.
     * @return Existing row or {@code null}.
     */
    public abstract GridH2Row put(GridH2Row row);

    /**
     * Remove row from index.
     *
     * @param row Row.
     * @return Removed row.
     */
    public abstract GridH2Row remove(SearchRow row);

    /**
     * Takes or sets existing snapshot to be used in current thread.
     *
     * @param s Optional existing snapshot to use.
     * @return Snapshot.
     */
    public Object takeSnapshot(@Nullable Object s) {
        return s;
    }

    /**
     * Releases snapshot for current thread.
     */
    public void releaseSnapshot() {
        // No-op.
    }

    /**
     * Filters rows from expired ones and using predicate.
     *
     * @param iter Iterator over rows.
     * @return Filtered iterator.
     */
    protected Iterator<GridH2Row> filter(Iterator<GridH2Row> iter) {
        GridBiPredicate<Object, Object> p = null;

        GridIndexingQueryFilter f = filters.get();

        if (f != null) {
            String spaceName = ((GridH2Table)getTable()).spaceName();

            try {
                p = f.forSpace(spaceName);
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }

        return new FilteringIterator(iter, U.currentTimeMillis(), p);
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public void add(Session ses, Row row) {
        throw DbException.getUnsupportedException("add");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses, Row row) {
        throw DbException.getUnsupportedException("remove row");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses) {
        throw DbException.getUnsupportedException("remove index");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /**
     * Iterator which filters by expiration time and predicate.
     */
    protected class FilteringIterator extends GridFilteredIterator<GridH2Row> {
        /** */
        private final GridBiPredicate<Object, Object> fltr;

        /** */
        private final long time;

        /**
         * @param iter Iterator.
         * @param time Time for expired rows filtering.
         */
        protected FilteringIterator(Iterator<GridH2Row> iter, long time,
            GridBiPredicate<Object, Object> fltr) {
            super(iter);

            this.time = time;
            this.fltr = fltr;
        }

        /**
         * @param row Row.
         * @return If this row was accepted.
         */
        @SuppressWarnings("unchecked")
        @Override protected boolean accept(GridH2Row row) {
            if (row instanceof GridH2AbstractKeyValueRow) {
                if (((GridH2AbstractKeyValueRow) row).expirationTime() <= time)
                    return false;
            }

            if (fltr == null)
                return true;

            Object key = row.getValue(keyCol).getObject();
            Object val = row.getValue(valCol).getObject();

            assert key != null;
            assert val != null;

            return fltr.apply(key, val);
        }
    }
}
