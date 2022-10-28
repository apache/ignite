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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.MemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.NoOpMemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.QueryMemoryTracker;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
public class Query<RowT> implements RunningQuery {
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

    /** */
    protected final int totalFragmentsCnt;

    /** */
    protected final AtomicInteger finishedFragmentsCnt = new AtomicInteger();

    /** */
    protected final Set<Long> initNodeStartedExchanges = new HashSet<>();

    /** Logger. */
    protected final IgniteLogger log;

    /** */
    private MemoryTracker memoryTracker;

    /** */
    public Query(
        UUID id,
        UUID initNodeId,
        GridQueryCancel cancel,
        ExchangeService exch,
        Consumer<Query<RowT>> unregister,
        IgniteLogger log,
        int totalFragmentsCnt
    ) {
        this.id = id;
        this.unregister = unregister;
        this.initNodeId = initNodeId;
        this.exch = exch;
        this.log = log;

        this.cancel = cancel != null ? cancel : new GridQueryCancel();

        fragments = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.totalFragmentsCnt = totalFragmentsCnt;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
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

        synchronized (mux) {
            if (memoryTracker != null)
                memoryTracker.reset();
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

            fragments.add(f);
        }
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

    /**
     * Callback after the first batch of the query fragment from the node is received.
     */
    public void onInboundExchangeStarted(UUID nodeId, long exchangeId) {
        // No-op.
    }

    /**
     * Callback after the last batch of the query fragment from the node is processed.
     */
    public void onInboundExchangeFinished(UUID nodeId, long exchangeId) {
        // No-op.
    }

    /**
     * Callback after the first batch of the query fragment from the node is sent.
     */
    public void onOutboundExchangeStarted(UUID nodeId, long exchangeId) {
        if (initNodeId.equals(nodeId))
            initNodeStartedExchanges.add(exchangeId);
    }

    /**
     * Callback after the last batch of the query fragment is sent to all nodes.
     */
    public void onOutboundExchangeFinished(long exchangeId) {
        if (finishedFragmentsCnt.incrementAndGet() == totalFragmentsCnt) {
            QueryState state0;

            synchronized (mux) {
                state0 = state;

                if (state0 == QueryState.EXECUTING)
                    state = QueryState.CLOSED;
            }

            if (state0 == QueryState.EXECUTING)
                tryClose();
        }
    }

    /** */
    public boolean isExchangeWithInitNodeStarted(long fragmentId) {
        // On remote node exchange ID is the same as fragment ID.
        return initNodeStartedExchanges.contains(fragmentId);
    }

    /** */
    public MemoryTracker createMemoryTracker(MemoryTracker globalMemoryTracker, long quota) {
        synchronized (mux) {
            // Query can have multiple fragments, each fragment requests memory tracker, but there should be only
            // one memory tracker per query on each node, store it inside Query instance.
            if (memoryTracker == null) {
                memoryTracker = quota > 0 || globalMemoryTracker != NoOpMemoryTracker.INSTANCE ?
                    new QueryMemoryTracker(globalMemoryTracker, quota) : NoOpMemoryTracker.INSTANCE;
            }

            return memoryTracker;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Query.class, this, "state", state, "fragments", fragments);
    }
}
