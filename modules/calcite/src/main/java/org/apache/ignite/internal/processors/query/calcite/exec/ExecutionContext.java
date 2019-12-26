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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;

/**
 * Runtime context allowing access to the tables in a database.
 */
public class ExecutionContext implements DataContext {
    /** */
    private final UUID queryId;

    /** */
    private final PlannerContext ctx;

    /** */
    private final Map<String, Object> params;

    /**
     * @param queryId Query ID.
     * @param ctx Query context.
     * @param params Parameters.
     */
    public ExecutionContext(UUID queryId, PlannerContext ctx, Map<String, Object> params) {
        this.queryId = queryId;
        this.params = params;
        this.ctx = ctx;
    }

    /**
     * @return Query ID.
     */
    public UUID queryId() {
        return queryId;
    }

    /**
     * @return Planner context.
     */
    public PlannerContext plannerContext() {
        return ctx;
    }

    /**
     * Executes a query task.
     *
     * @param task Query task.
     * @return Task future.
     */
    public Future<Void> execute(Runnable task) {
        return ctx.execute(queryId, task);
    }

    /** {@inheritDoc} */
    @Override public SchemaPlus getRootSchema() {
        return ctx.schema();
    }

    /** {@inheritDoc} */
    @Override public JavaTypeFactory getTypeFactory() {
        return ctx.typeFactory();
    }

    /** {@inheritDoc} */
    @Override public QueryProvider getQueryProvider() {
        return ctx.queryProvider();
    }

    /** {@inheritDoc} */
    @Override public Object get(String name) {
        return params.get(name);
    }
}
