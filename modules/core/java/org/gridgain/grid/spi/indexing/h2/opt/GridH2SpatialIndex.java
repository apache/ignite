/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2.opt;

import org.h2.engine.*;
import org.h2.index.*;
import org.h2.message.*;
import org.h2.result.*;
import org.h2.table.*;

import java.util.*;
import java.util.concurrent.locks.*;

/**
 * Spatial index.
 */
public class GridH2SpatialIndex extends BaseIndex implements SpatialIndex {
    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private final SpatialIndex idx;

    /**
     * @param idx Spatial index.
     */
    public GridH2SpatialIndex(SpatialIndex idx) {
        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        Lock l = lock.writeLock();

        l.lock();

        try {
            idx.close(ses);
        }
        finally {
            l.unlock();
        }
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
    @Override public Cursor find(Session session, SearchRow first, SearchRow last) {
        Lock l = lock.readLock();

        l.lock();

        try {
            // TODO
        }
        finally {
            l.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session session, int[] masks, TableFilter filter, SortOrder sortOrder) {
        Lock l = lock.readLock();

        l.lock();

        try {
            // TODO
        }
        finally {
            l.unlock();
        }
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
    @Override public boolean canGetFirstOrLast() {
        return idx.canGetFirstOrLast();
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session session, boolean first) {
        Lock l = lock.readLock();

        l.lock();

        try {
            // TODO
        }
        finally {
            l.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        Lock l = lock.readLock();

        l.lock();

        try {
            // TODO
        }
        finally {
            l.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        Lock l = lock.readLock();

        l.lock();

        try {
            // TODO
        }
        finally {
            l.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public Cursor findByGeometry(TableFilter filter, SearchRow intersection) {
        Lock l = lock.readLock();

        l.lock();

        try {
            // TODO
        }
        finally {
            l.unlock();
        }
    }

    /**
     * Cursor copying result before returning.
     */
    private static class CopyCursor implements Cursor {
        /** */
        private final List<Row> list;

        /** */
        private int row = -1;

        /**
         * @param c Cursor.
         */
        private CopyCursor(Cursor c) {
            if (c.next()) {
                list = new ArrayList<>();

                list.add(c.get());

                while (c.next())
                    list.add(c.get());
            }
            else
                list = Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            return list.get(row);
        }

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            return list.;
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            return false;
        }
    }
}
