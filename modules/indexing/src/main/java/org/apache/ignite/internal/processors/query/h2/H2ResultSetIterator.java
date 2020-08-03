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

package org.apache.ignite.internal.processors.query.h2;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcResultSet;
import org.h2.result.ResultInterface;
import org.h2.value.Value;

/**
 * Iterator over result set.
 */
public abstract class H2ResultSetIterator<T> extends GridIteratorAdapter<T> implements GridCloseableIterator<T> {
    /** */
    private static final Field RESULT_FIELD;

    /*
     * Initialize.
     */
    static {
        try {
            RESULT_FIELD = JdbcResultSet.class.getDeclaredField("result");

            RESULT_FIELD.setAccessible(true);
        }
        catch (NoSuchFieldException e) {
            throw new IllegalStateException("Check H2 version in classpath.", e);
        }
    }

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private ResultInterface res;

    /** */
    private ResultSet data;

    /** */
    protected Object[] row;

    /** */
    private List<Object[]> page;

    /** */
    private boolean hasRow;

    /** Page size. */
    private final int pageSize;

    /** Column count. */
    private final int colCnt;

    /** Page row iterator. */
    private Iterator<Object[]> rowIter;

    /** Current H2 session. */
    private final Session ses;

    /** Closed. */
    private boolean closed;

    /** Canceled. */
    private boolean canceled;

    /** Fetch size interceptor. */
    final H2QueryFetchSizeInterceptor fetchSizeInterceptor;

    /**
     * @param data Data array.
     * @param log Logger.
     * @param h2 Indexing H2.
     * @param pageSize Page size.
     * @throws IgniteCheckedException If failed.
     */
    protected H2ResultSetIterator(
        ResultSet data,
        int pageSize,
        IgniteLogger log,
        IgniteH2Indexing h2,
        H2QueryInfo qryInfo
    )
        throws IgniteCheckedException {
        this.pageSize = pageSize;
        this.data = data;

        try {
            res = (ResultInterface)RESULT_FIELD.get(data);
        }
        catch (IllegalAccessException e) {
            throw new IllegalStateException(e); // Must not happen.
        }

        if (data != null) {
            try {
                colCnt = data.getMetaData().getColumnCount();

                ses = H2Utils.session(data.getStatement().getConnection());

                page = new ArrayList<>(pageSize);
            }
            catch (SQLException e) {
                throw new IgniteCheckedException(e);
            }
        }
        else {
            colCnt = 0;
            page = null;
            row = null;
            ses = null;
        }

        assert log != null;
        assert h2 != null;
        assert qryInfo != null;

        fetchSizeInterceptor = new H2QueryFetchSizeInterceptor(h2, qryInfo, log);
    }

    /**
     * @return {@code true} if the next page is available.
     * @throws IgniteCheckedException On cancel.
     */
    private boolean fetchPage() throws IgniteCheckedException {
        lockTables();

        try {
            GridH2Table.checkTablesVersions(ses);

            page.clear();

            try {
                if (data.isClosed())
                    return false;
            }
            catch (SQLException e) {
                if (e.getErrorCode() == ErrorCode.STATEMENT_WAS_CANCELED)
                    throw new QueryCancelledException();

                throw new IgniteSQLException(e);
            }

            for (int i = 0; i < pageSize; ++i) {
                try {
                    if (!data.next())
                        break;

                    row = new Object[colCnt];

                    readRow();

                    page.add(row);
                }
                catch (SQLException e) {
                    close();

                    if (e.getCause() instanceof IgniteSQLException)
                        throw (IgniteSQLException)e.getCause();

                    if (e.getErrorCode() == ErrorCode.STATEMENT_WAS_CANCELED)
                        throw new QueryCancelledException();

                    throw new IgniteSQLException(e);
                }
            }

            if (F.isEmpty(page)) {
                rowIter = null;

                return false;
            }
            else {
                rowIter = page.iterator();

                return true;
            }
        }
        finally {
            unlockTables();
        }
    }

    /**
     * @throws SQLException On error.
     */
    private void readRow() throws SQLException {
        if (res != null) {
            Value[] values = res.currentRow();

            for (int c = 0; c < row.length; c++) {
                Value val = values[c];

                if (val instanceof GridH2ValueCacheObject) {
                    GridH2ValueCacheObject valCacheObj = (GridH2ValueCacheObject)values[c];

                    row[c] = valCacheObj.getObject(true);
                }
                else
                    row[c] = val.getObject();
            }
        }
        else {
            for (int c = 0; c < row.length; c++)
                row[c] = data.getObject(c + 1);
        }
    }

    /** */
    public void lockTables() {
        if (!isClosed() && ses.isLazyQueryExecution())
            GridH2Table.readLockTables(ses);
    }

    /** */
    public void unlockTables() {
        if (ses.isLazyQueryExecution())
            GridH2Table.unlockTables(ses);
    }

    /**
     * @return {@code true} If next row was fetched successfully.
     * @throws IgniteCheckedException On error.
     */
    private synchronized boolean fetchNext() throws IgniteCheckedException {
        if (canceled)
            throw new QueryCancelledException();

        if (rowIter != null && rowIter.hasNext()) {
            row = rowIter.next();

            fetchSizeInterceptor.checkOnFetchNext();

            return true;
        }

        if (!fetchPage()) {
            closeInternal();

            return false;
        }

        if (rowIter != null && rowIter.hasNext()) {
            row = rowIter.next();

            fetchSizeInterceptor.checkOnFetchNext();

            return true;
        }
        else
            return false;
    }

    /**
     * @return Row.
     */
    protected abstract T createRow();

    /**
     * @throws IgniteCheckedException On error.
     */
    public void onClose() throws IgniteCheckedException {
        if (data == null)
            // Nothing to close.
            return;

        lockTables();

        try {
            fetchSizeInterceptor.checkOnClose();

            data.close();
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }
        finally {
            res = null;
            data = null;
            page = null;

            unlockTables();
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() throws IgniteCheckedException {
        if (closed)
            return;

        canceled = true;

        closeInternal();
    }

    /**
     * @throws IgniteCheckedException On error.
     */
    private synchronized void closeInternal() throws IgniteCheckedException {
        if (closed)
            return;

        closed = true;

        onClose();
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean hasNextX() throws IgniteCheckedException {
        if (canceled)
            throw new QueryCancelledException();

        if (closed)
            return false;

        return hasRow || (hasRow = fetchNext());
    }

    /** {@inheritDoc} */
    @Override public T nextX() throws IgniteCheckedException {
        if (!hasNextX())
            throw new NoSuchElementException();

        hasRow = false;

        return createRow();
    }

    /** {@inheritDoc} */
    @Override public void removeX() throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2ResultSetIterator.class, this);
    }
}
