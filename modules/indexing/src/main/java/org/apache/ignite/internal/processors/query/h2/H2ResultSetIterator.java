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

import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.value.Value;

/**
 * Iterator over result set.
 */
public abstract class H2ResultSetIterator<T> extends GridCloseableIteratorAdapter<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final H2ResultSet data;

    /** */
    protected final Object[] row;

    /** */
    private boolean hasRow;

    /**
     * @param data Data array.
     * @throws IgniteCheckedException If failed.
     */
    protected H2ResultSetIterator(H2ResultSet data) throws IgniteCheckedException {
        this.data = data;

        if (data != null)
            row = new Object[data.getColumnsCount()];
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

            Value[] values = data.currentRow();

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

            return true;
        }
        catch (Exception e) {
            throw new IgniteSQLException("Failed to execute query.", e);
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
        U.closeQuiet(data);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2ResultSetIterator.class, this);
    }
}
