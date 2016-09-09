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

import static org.apache.ignite.internal.processors.cache.QueryCursorImpl.State.*;

/**
 * Query cursor implementation.
 */
public class QueryCursorImpl<T> implements QueryCursorEx<T> {
    /** Query cursor state */
    enum State {
        /** Idle. */IDLE,
        /** Executing. */EXECUTION,
        /** Result ready. */RESULT_READY,
        /** Closed. */CLOSED,
    }

    /** Query executor. */
    private Iterable<T> iterExec;

    /** */
    private Iterator<T> iter;

    /** */
    private final AtomicReference<State> state = new AtomicReference<>(IDLE);

    /** */
    private List<GridQueryFieldMetadata> fieldsMeta;

    /** */
    private final GridAbsClosure cancel;

    /**
     * @param iterExec Query executor.
     * @param cancel Cancellation closure.
     */
    public QueryCursorImpl(Iterable<T> iterExec, GridAbsClosure cancel) {
        this.iterExec = iterExec;
        this.cancel = cancel;
    }

    /**
     * @param iterExec Query executor.
     */
    public QueryCursorImpl(Iterable<T> iterExec) {
        this(iterExec, null);
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        State state0 = state.get();

        if (state0 != IDLE || !state.compareAndSet(state0, EXECUTION))
            throw new IllegalStateException("Illegal query cursor state: " + state0);

        iter = iterExec.iterator();

        state.set(RESULT_READY);

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
        State state0 = this.state.get();

        switch (state0) {
            case EXECUTION:
                if (!state.compareAndSet(state0, CLOSED))
                    fail(state0);

                cancel.apply();

                break;
            case RESULT_READY:
                if (!state.compareAndSet(state0, CLOSED))
                    fail(state0);

                if (iter instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable)iter).close();
                    }
                    catch (Exception e) {
                        throw new IgniteException(e);
                    }
                }

                break;
            default:
                fail(state0);
        }
    }

    /**
     * @param state State.
     */
    private void fail(State state) {
        throw new IllegalStateException("Illegal query cursor state: " + state);
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