/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

import static org.apache.ignite.internal.processors.cache.QueryCursorImpl.State.CLOSED;
import static org.apache.ignite.internal.processors.cache.QueryCursorImpl.State.EXECUTION;
import static org.apache.ignite.internal.processors.cache.QueryCursorImpl.State.IDLE;
import static org.apache.ignite.internal.processors.cache.QueryCursorImpl.State.RESULT_READY;

/**
 * Query cursor implementation.
 */
public class QueryCursorImpl<T> implements QueryCursorEx<T>, FieldsQueryCursor<T> {
    /** */
    private final static AtomicReferenceFieldUpdater<QueryCursorImpl, State> STATE_UPDATER =
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

    /**
     * @param iterExec Query executor.
     * @param cancel Cancellation closure.
     */
    public QueryCursorImpl(Iterable<T> iterExec, GridQueryCancel cancel) {
        this(iterExec, cancel, true);
    }

    /**
     * @param iterExec Query executor.
     */
    public QueryCursorImpl(Iterable<T> iterExec) {
        this(iterExec, null);
    }

    /**
     * @param iterExec Query executor.
     * @param isQry Result type flag - {@code true} for query, {@code false} for update operation.
     */
    public QueryCursorImpl(Iterable<T> iterExec, GridQueryCancel cancel, boolean isQry) {
        this.iterExec = iterExec;
        this.cancel = cancel;
        this.isQry = isQry;
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        if (!STATE_UPDATER.compareAndSet(this, IDLE, EXECUTION))
            throw new IgniteException("Iterator is already fetched or query was cancelled.");

        iter = iterExec.iterator();

        if (!STATE_UPDATER.compareAndSet(this, EXECUTION, RESULT_READY)) {
            // Handle race with cancel and make sure the iterator resources are freed correctly.
            closeIter();

            throw new CacheException(new QueryCancelledException());
        }

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
        while(state != CLOSED) {
            if (STATE_UPDATER.compareAndSet(this, RESULT_READY, CLOSED)) {
                closeIter();

                return;
            }

            if (STATE_UPDATER.compareAndSet(this, EXECUTION, CLOSED)) {
                if (cancel != null)
                    cancel.cancel();

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
    public boolean isQuery() {
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
        /** Idle. */IDLE,
        /** Executing. */EXECUTION,
        /** Result ready. */RESULT_READY,
        /** Closed. */CLOSED,
    }
}
