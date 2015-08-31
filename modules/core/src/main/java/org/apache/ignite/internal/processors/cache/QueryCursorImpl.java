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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;

/**
 * Query cursor implementation.
 */
public class QueryCursorImpl<T> implements QueryCursorEx<T> {
    /** Query executor. */
    private Iterable<T> iterExec;

    /** */
    private Iterator<T> iter;

    /** */
    private boolean iterTaken;

    /** */
    private List<GridQueryFieldMetadata> fieldsMeta;

    /**
     * @param iterExec Query executor.
     */
    public QueryCursorImpl(Iterable<T> iterExec) {
        this.iterExec = iterExec;
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        if (iter == null && iterTaken)
            throw new IgniteException("Cursor is closed.");

        if (iterTaken)
            throw new IgniteException("Iterator is already taken from this cursor.");

        iterTaken = true;

        iter = iterExec.iterator();

        assert iter != null;

        return iter;
    }

    /** {@inheritDoc} */
    @Override public List<T> getAll() {
        List<T> all = new ArrayList<>();

        try {
            for (T t : this) // Implicitly calls iterator() to do all checks.
                all.add(t);
        }
        finally {
            close();
        }

        return all;
    }

    /** {@inheritDoc} */
    @Override public void getAll(QueryCursorEx.Consumer<T> clo) throws IgniteCheckedException {
        try {
            for (T t : this)
                clo.consume(t);
        }
        finally {
            close();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        Iterator<T> i;

        if ((i = iter) != null) {
            iter = null;

            if (i instanceof AutoCloseable) {
                try {
                    ((AutoCloseable)i).close();
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
            }
        }
    }

    /**
     * @param fieldsMeta SQL Fields query result metadata.
     */
    public void fieldsMeta(List<GridQueryFieldMetadata> fieldsMeta) {
        this.fieldsMeta = fieldsMeta;
    }

    /**
     * @return SQL Fields query result metadata.
     */
    @Override public List<GridQueryFieldMetadata> fieldsMeta() {
        return fieldsMeta;
    }
}