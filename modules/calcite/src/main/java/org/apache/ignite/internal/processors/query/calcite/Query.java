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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryState;
import org.apache.ignite.internal.processors.query.RunningQuery;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionCancelledException;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
public class Query<RowT> implements RunningQuery {
    /** Completable futures empty array. */
    private static final CompletableFuture<?>[] COMPLETABLE_FUTURES_EMPTY_ARRAY = new CompletableFuture<?>[0];

    /** */
    private final UUID initNodeId;

    /** */
    private final UUID id;

    /** */
    protected final Object mux = new Object();

    /** */
    protected final Set<RunningFragment<RowT>> fragments;

    /** */
    protected final GridQueryCancel cancel;

    /** */
    protected final Consumer<Query<RowT>> unregister;

    /** */
    protected volatile QueryState state = QueryState.INITED;

    /** */
    protected final ExchangeService exch;

    /** Logger. */
    protected final IgniteLogger log;

    /** */
    public Query(
        UUID id,
        UUID initNodeId,
        GridQueryCancel cancel,
        ExchangeService exch,
        Consumer<Query<RowT>> unregister,
        IgniteLogger log
    ) {
        this.id = id;
        this.unregister = unregister;
        this.initNodeId = initNodeId;
        this.exch = exch;
        this.log = log;

        this.cancel = cancel != null ? cancel : new GridQueryCancel();

        fragments = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    /** */
    @Override public UUID id() {
        return id;
    }

    /** */
    @Override public QueryState state() {
        return state;
    }

    /** */
    public UUID initiatorNodeId() {
        return initNodeId;
    }

    /** */
    protected void tryClose() {
        List<RunningFragment<RowT>> fragments = new ArrayList<>(this.fragments);

        AtomicInteger cntDown = new AtomicInteger(fragments.size());

        for (RunningFragment<RowT> frag : fragments) {
            frag.context().execute(() -> {
                frag.root().close();
                frag.context().cancel();

                if (cntDown.decrementAndGet() == 0)
                    unregister.accept(this);

            }, frag.root()::onError);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        synchronized (mux) {
            if (state == QueryState.CLOSED)
                return;
            
            if (state == QueryState.INITED) {
                state = QueryState.CLOSING;

                try {
                    exch.closeQuery(initNodeId, id);

                    return;
                }
                catch (IgniteCheckedException e) {
                    log.warning("Cannot send cancel request to query initiator", e);
                }
            }

            if (state == QueryState.EXECUTING || state == QueryState.CLOSING)
                state = QueryState.CLOSED;
        }

        for (RunningFragment<RowT> frag : fragments)
            frag.context().execute(() -> frag.root().onError(new ExecutionCancelledException()), frag.root()::onError);

        tryClose();
    }

    /** */
    public void addFragment(RunningFragment<RowT> f) {
        synchronized (mux) {
            if (state == QueryState.INITED)
                state = QueryState.EXECUTING;

            if (state == QueryState.CLOSING || state == QueryState.CLOSED) {
                throw new IgniteSQLException(
                    "The query was cancelled",
                    IgniteQueryErrorCode.QUERY_CANCELED,
                    new ExecutionCancelledException()
                );
            }
        }

        fragments.add(f);
    }

    /** */
    public boolean isCancelled() {
        return cancel.isCanceled();
    }

    /** */
    public void onNodeLeft(UUID nodeId) {
        if (initNodeId.equals(nodeId))
            cancel();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Query.class, this, "state", state, "fragments", fragments);
    }
}
