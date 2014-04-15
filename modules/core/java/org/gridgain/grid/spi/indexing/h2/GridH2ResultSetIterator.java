/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Iterator over result set.
 */
abstract class GridH2ResultSetIterator<T> extends GridCloseableIteratorAdapterEx<T> {
    private static final long serialVersionUID = 2631327063398590665L;
    /** */
    protected ResultSet rs;

    /** */
    private Statement stmt;

    /** */
    private T next;

    /** */
    private boolean init;

    /**
     * @param rs Result set.
     * @param stmt Statement to close at the end (if provided).
     */
    @SuppressWarnings({"AbstractMethodCallInConstructor", "OverriddenMethodCallDuringObjectConstruction"})
    protected GridH2ResultSetIterator(ResultSet rs, Statement stmt) {
        this.rs = rs;
        this.stmt = stmt;
    }

    /**
     * Loads first row.
     *
     * @throws GridException In case of error.
     */
    private void init() throws GridException {
        if (init)
            return;

        init = true;

        try {
            if (rs != null && rs.next())
                next = loadRow();
        }
        catch (Exception e) {
            if (e instanceof SQLException)
                onSqlException((SQLException)e);

            throw new GridException("Failed to load row.", e);
        }
    }

    /**
     * Handles SQL exception.
     *
     * @param e Exception.
     */
    protected abstract void onSqlException(SQLException e);

    /**
     * Loads row from result set.
     *
     * @return Object associated with row of the result set.
     * @throws SQLException In case of SQL error.
     * @throws GridSpiException In case of SPI error.
     * @throws IOException In case of I/O error.
     */
    protected abstract T loadRow() throws SQLException, GridSpiException, IOException;

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() throws GridException {
        // Initialize if needed;
        init();

        return next != null;
    }

    /** {@inheritDoc} */
    @Override protected T onNext() throws GridException {
        // Initialize if needed;
        init();

        T res = next;

        try {
            next = rs != null && !rs.isClosed() && rs.next() ? loadRow() : null;
        }
        catch (Exception e) {
            if (e instanceof SQLException)
                onSqlException((SQLException)e);

            throw new GridException("Failed to load row.", e);
        }

        if (res == null)
            throw new NoSuchElementException();

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void onClose() {
        U.close(rs, null);
        U.close(stmt, null);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString((Class<GridH2ResultSetIterator>)getClass(), this);
    }
}
