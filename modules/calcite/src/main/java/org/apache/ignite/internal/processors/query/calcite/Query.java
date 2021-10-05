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

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryState;
import org.apache.ignite.internal.processors.query.RunningQuery;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;

/** */
public class Query<Row> implements RunningQuery {
    /** */
    private final UUID id;

    /** */
    protected final Set<RunningFragment<Row>> fragments;

    /** */
    protected final GridQueryCancel cancel;

    /** */
    protected final Consumer<Query> unregister;

    /** */
    protected volatile QueryState state = QueryState.INIT;

    /** */
    public Query(UUID id, GridQueryCancel cancel, Consumer<Query> unregister) {
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

    /** {@inheritDoc} */
    @Override public void cancel() {
        for (RunningFragment<Row> frag : fragments)
            frag.context().execute(frag.root()::close, frag.root()::onError);

        for (RunningFragment<Row> frag : fragments)
            frag.context().execute(frag.context()::cancel, frag.root()::onError);

        unregister.accept(this);
    }

    /** */
    public void addFragment(RunningFragment f) {
        fragments.add(f);
    }

    /** */
    public boolean isCancelled() {
        return cancel.isCanceled();
    }
}
