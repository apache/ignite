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
import java.util.function.Consumer;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryState;
import org.apache.ignite.internal.processors.query.RunningQuery;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionCancelledException;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;

/** */
public class Query<RowT> implements RunningQuery {
    /** Completable futures empty array. */
    private static final CompletableFuture<?>[] COMPLETABLE_FUTURES_EMPTY_ARRAY = new CompletableFuture<?>[0];

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
    public Query(UUID id, GridQueryCancel cancel, Consumer<Query<RowT>> unregister) {
        this.id = id;
        this.unregister = unregister;

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
    public static BaseQueryContext createQueryContext(QueryContext ctx, SchemaPlus schema, IgniteLogger log) {
        return BaseQueryContext.builder()
            .parentContext(Commons.convert(ctx))
            .frameworkConfig(
                Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(schema)
                    .build()
            )
            .logger(log)
            .build();
    }

    /** */
    protected void tryClose() {
        List<CompletableFuture<?>> futs = new ArrayList<>();
        for (RunningFragment<RowT> frag : fragments) {
            CompletableFuture<?> f = frag.context().submit(frag.root()::close, frag.root()::onError);

            CompletableFuture<?> fCancel = f.thenApply((u) -> frag.context().submit(frag.context()::cancel, frag.root()::onError));
            futs.add(fCancel);
        }

        CompletableFuture.allOf(futs.toArray(COMPLETABLE_FUTURES_EMPTY_ARRAY))
            .thenAccept((u) -> unregister.accept(this));
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        synchronized (mux) {
            if (state == QueryState.CLOSED)
                return;

            if (state == QueryState.EXECUTING)
                state = QueryState.CLOSED;
        }

        for (RunningFragment<RowT> frag : fragments)
            frag.context().execute(() -> frag.root().onError(new ExecutionCancelledException()), frag.root()::onError);

        tryClose();
    }

    /** */
    public void addFragment(RunningFragment<RowT> f) {
        if (state == QueryState.INITED) {
            synchronized (mux) {
                if (state == QueryState.INITED)
                    state = QueryState.EXECUTING;
            }
        }

        fragments.add(f);
    }

    /** */
    public boolean isCancelled() {
        return cancel.isCanceled();
    }
}
