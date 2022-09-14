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

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;

import static org.apache.ignite.internal.processors.cache.QueryCursorImpl.State.CLOSED;
import static org.apache.ignite.internal.processors.cache.QueryCursorImpl.State.COMPLETED;
import static org.apache.ignite.internal.processors.cache.QueryCursorImpl.State.EXECUTING;
import static org.apache.ignite.internal.processors.cache.QueryCursorImpl.State.IDLE;

/**
 * Query cursor implementation.
 */
public class QueryCursorImpl<T> implements QueryCursorEx<T>, FieldsQueryCursor<T> {
    /** */
    private static final AtomicReferenceFieldUpdater<QueryCursorImpl, State> STATE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(QueryCursorImpl.class, State.class, "state");

    /** Query executor. */
    private final Iterable<T> iterExec;

    /** Result type flag - result set or update counter. */
    private final boolean isQry;

    /** */
    private Iterator<T> iter;

    /** */
    private volatile State state = IDLE;

    /** */
    private List<GridQueryFieldMetadata> fieldsMeta;

    /** */
    private final GridQueryCancel cancel;

    /** */
    private final boolean lazy;

    /** Partition result. */
    private PartitionResult partRes;

    /**
     * @param iterExec Query executor.
     */
    public QueryCursorImpl(Iterable<T> iterExec) {
        this(iterExec, null, true, false);
    }

    /**
     * @param iterExec Query executor.
     * @param isQry Result type flag - {@code true} for query, {@code false} for update operation.
     */
    public QueryCursorImpl(Iterable<T> iterExec, GridQueryCancel cancel, boolean isQry, boolean lazy) {
        this.iterExec = iterExec;
        this.cancel = cancel;
        this.isQry = isQry;
        this.lazy = lazy;
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        return new AutoClosableCursorIterator<>(this, iter());
    }

    /**
     * @return An simple iterator.
     */
    protected Iterator<T> iter() {
        if (!STATE_UPDATER.compareAndSet(this, IDLE, EXECUTING))
            throw new IgniteException("Iterator is already fetched or query was cancelled.");

        iter = iterExec.iterator();

        if (!lazy && !STATE_UPDATER.compareAndSet(this, EXECUTING, COMPLETED)) {
            // Handle race with cancel and make sure the iterator resources are freed correctly.
            closeIter();

            throw new CacheException(new QueryCancelledException());
        }

        assert iter != null;

        if (lazy)
            iter = new LazyIterator<>(iter);

        int expSize = fieldsMeta.size();

        return new Iterator<T>() {
            @Override public boolean hasNext() {
                return iter.hasNext();
            }

            @Override public T next() {
                T next = iter.next();

                if (next instanceof List) {
                    if (fieldsMeta != null && ((List<?>)next).size() != expSize) {
                        throw new RuntimeException("MY TEST size=" + expSize +
                            " next=" + ((List<?>)next).size() + " f=" + fieldsMeta + " n=" + next);
                    }
                }

                return next;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public List<T> getAll() {
        List<T> all = new ArrayList<>();

        try {
            Iterator<T> iter = iter(); // Implicitly calls iterator() to do all checks.

            while (iter.hasNext())
                all.add(iter.next());
        }
        finally {
            close();
        }

        return all;
    }

    /** {@inheritDoc} */
    @Override public void getAll(QueryCursorEx.Consumer<T> clo) throws IgniteCheckedException {
        try {
            Iterator<T> iter = iter(); // Implicitly calls iterator() to do all checks.

            while (iter.hasNext())
                clo.consume(iter.next());
        }
        finally {
            close();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        while (state != CLOSED) {
            if (STATE_UPDATER.compareAndSet(this, COMPLETED, CLOSED)) {
                closeIter();

                return;
            }

            if (STATE_UPDATER.compareAndSet(this, EXECUTING, CLOSED)) {
                if (cancel != null)
                    cancel.cancel();

                closeIter();

                return;
            }

            if (STATE_UPDATER.compareAndSet(this, IDLE, CLOSED))
                return;
        }
    }

    /**
     * Closes iterator.
     */
    private void closeIter() {
        if (iter instanceof AutoCloseable) {
            try {
                ((AutoCloseable)iter).close();
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * @return {@code true} if this cursor corresponds to a {@link ResultSet} as a result of query,
     * {@code false} if query was modifying operation like INSERT, UPDATE, or DELETE.
     */
    @Override public boolean isQuery() {
        return isQry;
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

    /** {@inheritDoc} */
    @Override public String getFieldName(int idx) {
        assert this.fieldsMeta != null;

        GridQueryFieldMetadata metadata = fieldsMeta.get(idx);

        return metadata.fieldName();
    }

    /** {@inheritDoc} */
    @Override public int getColumnsCount() {
        assert this.fieldsMeta != null;

        return fieldsMeta.size();
    }

    /** Query cursor state */
    protected enum State {
        /** Idle. */
        IDLE,

        /** Executing. */
        EXECUTING,

        /** Execution completed. */
        COMPLETED,

        /** Closed. */
        CLOSED,
    }

    /**
     * @return Partition result.
     */
    public PartitionResult partitionResult() {
        return partRes;
    }

    /**
     * @return Lazy mode flag.
     */
    protected boolean lazy() {
        return lazy;
    }

    /**
     * @param partRes New partition result.
     */
    public void partitionResult(PartitionResult partRes) {
        this.partRes = partRes;
    }

    /**
     * Iterator wrapper for lazy results. Updates cursor state when all rows are read,
     * otherwise just delegates invocation.
     */
    public class LazyIterator<Type> implements Iterator<Type>, AutoCloseable {
        /** */
        private final Iterator<Type> delegate;

        /**
         * @param delegate Iterator.
         */
        public LazyIterator(Iterator<Type> delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            if (delegate.hasNext())
                return true;

            STATE_UPDATER.compareAndSet(QueryCursorImpl.this, EXECUTING, COMPLETED);

            return false;
        }

        /** {@inheritDoc} */
        @Override public Type next() {
            return delegate.next();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            delegate.remove();
        }

        /** {@inheritDoc} */
        @Override public void forEachRemaining(java.util.function.Consumer<? super Type> action) {
            delegate.forEachRemaining(action);
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            if (delegate instanceof AutoCloseable)
                ((AutoCloseable)delegate).close();
        }
    }
}
