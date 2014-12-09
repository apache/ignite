/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2;

import org.gridgain.grid.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.sql.*;
import java.util.*;


/**
 * Iterator over result set.
 */
abstract class GridH2ResultSetIterator<T> extends GridCloseableIteratorAdapter<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final ResultSet data;

    /** */
    protected final Object[] row;

    /** */
    private boolean hasRow;

    /**
     * @param data Data array.
     * @throws GridException If failed.
     */
    protected GridH2ResultSetIterator(ResultSet data) throws GridException {
        this.data = data;

        if (data != null) {
            try {
                row = new Object[data.getMetaData().getColumnCount()];
            }
            catch (SQLException e) {
                throw new GridException(e);
            }
        }
        else
            row = null;
    }

    /**
     * @return {@code true} If next row was fetched successfully.
     */
    private boolean fetchNext() {
        if (data == null)
            return false;

        try {
            if (!data.next())
                return false;

            for (int c = 0; c < row.length; c++)
                row[c] = data.getObject(c + 1);

            return true;
        }
        catch (SQLException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onHasNext() {
        return hasRow || (hasRow = fetchNext());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("IteratorNextCanNotThrowNoSuchElementException")
    @Override public T onNext() {
        if (!hasNext())
            throw new NoSuchElementException();

        hasRow = false;

        return createRow();
    }

    /**
     * @return Row.
     */
    protected abstract T createRow();

    /** {@inheritDoc} */
    @Override public void onRemove() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void onClose() throws GridException {
        if (data == null)
            // Nothing to close.
            return;

        try {
            U.closeQuiet(data.getStatement());
        }
        catch (SQLException e) {
            throw new GridException(e);
        }

        U.closeQuiet(data);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString((Class<GridH2ResultSetIterator>)getClass(), this);
    }
}
