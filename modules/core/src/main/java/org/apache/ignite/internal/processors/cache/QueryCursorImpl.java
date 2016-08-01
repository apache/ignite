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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

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

    /** */
    private final AtomicReference<GridAbsClosure> mapQrysCancel;

    /** */
    private final AtomicReference<GridAbsClosure> rdcQryCancel;

    /**
     * @param iterExec Query executor.
     * @param rdcQryCancel Used to hold cancellation closure.
     */
    public QueryCursorImpl(Iterable<T> iterExec,
        @Nullable AtomicReference<GridAbsClosure> mapQrysCancel, @Nullable AtomicReference<GridAbsClosure> rdcQryCancel) {
        this.iterExec = iterExec;
        this.mapQrysCancel = mapQrysCancel;
        this.rdcQryCancel = rdcQryCancel;
    }

    /**
     * @param iterExec Query executor.
     */
    public QueryCursorImpl(Iterable<T> iterExec) {
        this(iterExec, null, null);
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
            doClose(false);
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
            doClose(false);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        doClose(true);
    }

    /**
     * @param cancellable Cancellable flag.
     */
    private void doClose(boolean cancellable) {
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
        } else if (cancellable) {
            GridAbsClosure clo;
            if (mapQrysCancel != null) {
                // Make sure we use correct closure.
                while(true) {
                    clo = mapQrysCancel.get();

                    if (mapQrysCancel.compareAndSet(clo, F.noop()))
                        break;
                }

                if (clo != null)
                    clo.apply();
            }

            if (rdcQryCancel != null) {
                clo = rdcQryCancel.get();

                if (clo != null && rdcQryCancel.compareAndSet(clo, F.noop()))
                    clo.apply();
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