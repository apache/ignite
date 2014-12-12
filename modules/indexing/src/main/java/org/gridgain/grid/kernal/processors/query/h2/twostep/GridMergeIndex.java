/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.twostep;

import org.gridgain.grid.*;
import org.h2.engine.*;
import org.h2.index.*;
import org.h2.message.*;
import org.h2.result.*;
import org.h2.table.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Merge index.
 */
public class GridMergeIndex extends BaseIndex {
    /** */
    private static final int MAX_CAPACITY = 100_000;

    /** */
    private static final Collection<Row> END = new ArrayList<>(0);

    /** */
    private Collection<Collection<Row>> fetchedRows = new LinkedBlockingQueue<>();

    /** */
    private BlockingQueue<Collection<Row>> cursorRows = new LinkedBlockingQueue<>();

    /** */
    private int fetchedCnt;

    /** */
    private final AtomicInteger cnt = new AtomicInteger(0);

    /** {@inheritDoc} */
    @Override public long getRowCount(Session session) {
        return cnt.get();
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return getRowCount(null);
    }

    /**
     * @param cnt Count.
     */
    public void addCount(int cnt) {
        this.cnt.addAndGet(cnt);
    }

    /**
     * @param rows0 Rows.
     */
    public void addRows(Collection<Row> rows0) {
        assert !rows0.isEmpty();


    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public void close(Session session) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void add(Session session, Row row) {
        throw DbException.getUnsupportedException("add");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session session, Row row) {
        throw DbException.getUnsupportedException("remove row");
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session session, SearchRow first, SearchRow last) {
        if (fetchedRows == null)
            throw new GridRuntimeException("Rows were dropped out of result set.");

        return new Cursor0();
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session session, int[] masks, TableFilter filter, SortOrder sortOrder) {
        return getRowCountApproximation() + Constants.COST_ROW_OFFSET;
    }

    /** {@inheritDoc} */
    @Override public void remove(Session session) {
        throw DbException.getUnsupportedException("remove index");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session session) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("findFirstOrLast");
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    private class Cursor0 implements Cursor {
        /** */
        private Row cur;

        /** */
        private Iterator<Row> curIter;

        /** {@inheritDoc} */
        @Override public Row get() {
            return cur;
        }

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            return get();
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }
}
