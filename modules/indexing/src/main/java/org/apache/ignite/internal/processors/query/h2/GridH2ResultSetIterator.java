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
import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.jdbc.JdbcResultSet;
import org.h2.result.ResultInterface;
import org.h2.value.Value;

/**
 * Iterator over result set.
 */
public abstract class GridH2ResultSetIterator<T> extends GridCloseableIteratorAdapter<T> {
    /** */
    private static final Field RESULT_FIELD;

    /**
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
    private final ResultInterface res;

    /** */
    private final ResultSet data;

    /** */
    protected final Object[] row;

    /** */
    private final boolean closeStmt;

    /** */
    private boolean hasRow;

    /**
     * @param data Data array.
     * @param closeStmt If {@code true} closes result set statement when iterator is closed.
     * @param needCpy {@code True} if need copy cache object's value.
     * @throws IgniteCheckedException If failed.
     */
    protected GridH2ResultSetIterator(ResultSet data, boolean closeStmt, boolean needCpy) throws IgniteCheckedException {
        this.data = data;
        this.closeStmt = closeStmt;

        try {
            res = needCpy ? (ResultInterface)RESULT_FIELD.get(data) : null;
        }
        catch (IllegalAccessException e) {
            throw new IllegalStateException(e); // Must not happen.
        }

        if (data != null) {
            try {
                row = new Object[data.getMetaData().getColumnCount()];
            }
            catch (SQLException e) {
                throw new IgniteCheckedException(e);
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

            if (res != null) {
                Value[] values = res.currentRow();

                for (int c = 0; c < row.length; c++) {
                    Value val = values[c];

                    if (val instanceof GridH2ValueCacheObject) {
                        GridH2ValueCacheObject valCacheObj = (GridH2ValueCacheObject)values[c];

                        GridCacheContext cctx = valCacheObj.getCacheContext();

                        row[c] = valCacheObj.getObject(cctx != null && cctx.needValueCopy());
                    }
                    else
                        row[c] = val.getObject();
                }
            }
            else {
                for (int c = 0; c < row.length; c++)
                    row[c] = data.getObject(c + 1);
            }

            return true;
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
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
    @Override public void onClose() throws IgniteCheckedException {
        if (data == null)
            // Nothing to close.
            return;

        if (closeStmt) {
            try {
                U.closeQuiet(data.getStatement());
            }
            catch (SQLException e) {
                throw new IgniteCheckedException(e);
            }
        }

        U.closeQuiet(data);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2ResultSetIterator.class, this);
    }
}
