/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.twostep;

import org.apache.ignite.*;
import org.h2.engine.*;
import org.h2.index.*;
import org.h2.message.*;
import org.h2.result.*;
import org.h2.table.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Merge index.
 */
public abstract class GridMergeIndex extends BaseIndex {
    /** */
    private static final int MAX_FETCH_SIZE = 100000;

    /** */
    private final AtomicInteger cnt = new AtomicInteger(0);

    /**
     * Will be r/w from query execution thread only, does not need to be threadsafe.
     */
    private ArrayList<Row> fetched = new ArrayList<>();

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
     * @param page Page.
     */
    public abstract void addPage(GridResultPage<?> page);

    /** {@inheritDoc} */
    @Override public Cursor find(Session session, SearchRow first, SearchRow last) {
        if (fetched == null)
            throw new IgniteException("Fetched result set was too large.");

        if (fetched.size() == cnt.get())  // We've fetched all the rows.
            return findAllFetched(fetched, first, last);

        return findInStream(first, last);
    }

    /**
     * @param first First row.
     * @param last Last row.
     * @return Cursor. Usually it must be {@link FetchingCursor} instance.
     */
    protected abstract Cursor findInStream(@Nullable SearchRow first, @Nullable SearchRow last);

    /**
     * @param fetched Fetched rows.
     * @param first First row.
     * @param last Last row.
     * @return Cursor.
     */
    protected Cursor findAllFetched(List<Row> fetched, @Nullable SearchRow first, @Nullable SearchRow last) {
        return new IteratorCursor(fetched.iterator());
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

    /**
     * Cursor over iterator.
     */
    protected class IteratorCursor implements Cursor {
        /** */
        private Iterator<Row> iter;

        /** */
        protected Row cur;

        /**
         * @param iter Iterator.
         */
        public IteratorCursor(Iterator<Row> iter) {
            assert iter != null;

            this.iter = iter;
        }

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
            cur = iter.hasNext() ? iter.next() : null;

            return cur != null;
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }

    /**
     * Fetching cursor.
     */
    protected abstract class FetchingCursor extends IteratorCursor {
        /** */
        private boolean canFetch = true;

        /**
         */
        public FetchingCursor() {
            super(fetched == null ? Collections.<Row>emptyIterator() : fetched.iterator());
        }

        /**
         * @return Next row or {@code null} if none available.
         */
        @Nullable protected abstract Row fetchNext();

        /** {@inheritDoc} */
        @Override public boolean next() {
            if (super.next())
                return true;

            if (!canFetch)
                return false;

            cur = fetchNext();

            if (cur == null) { // No more results to fetch.
                assert fetched == null || fetched.size() == cnt.get() : fetched.size() + " <> " + cnt.get();

                canFetch = false;

                return false;
            }

            if (fetched != null) { // Try to reuse fetched result.
                fetched.add(cur);

                if (fetched.size() == MAX_FETCH_SIZE)
                    fetched = null; // Throw away fetched result if it is too large.
            }

            return true;
        }
    }
}
