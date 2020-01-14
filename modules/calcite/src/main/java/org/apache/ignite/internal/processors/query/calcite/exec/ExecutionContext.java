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
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;

/**
 * Runtime context allowing access to the tables in a database.
 */
public class ExecutionContext implements DataContext {
    /** */
    private final UUID queryId;

    /** */
    private final long fragmentId;

    /** */
    private final IgniteCalciteContext ctx;

    /** */
    private final int[] parts;

    /** */
    private final Map<String, Object> params;

    /** */
    private volatile boolean cancelled;

    /**
     * @param ctx Parent context.
     * @param queryId Query ID.
     * @param fragmentId Fragment ID.
     * @param parts Partitions.
     * @param params Parameters.
     */
    public ExecutionContext(IgniteCalciteContext ctx, UUID queryId, long fragmentId, int[] parts, Map<String, Object> params) {
        this.queryId = queryId;
        this.fragmentId = fragmentId;
        this.parts = parts;
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
     * @return Fragment ID.
     */
    public long fragmentId() {
        return fragmentId;
    }

    /**
     * @return Interested partitions.
     */
    public int[] partitions() {
        return parts;
    }

    /**
     * @return Parent context.
     */
    public IgniteCalciteContext parent() {
        return ctx;
    }

    public boolean cancelled() {
        return cancelled;
    }

    public void setCancelled() {
        cancelled = true;
    }

    /**
     * Executes a query task.
     *
     * @param task Query task.
     * @return Task future.
     */
    public Future<Void> execute(Runnable task) {
        return ctx.execute(queryId, fragmentId, task);
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

    /**
     * @return Exchange processor.
     */
    public ExchangeService exchange() {
        return ctx.exchangeService();
    }

    /**
     * @return Mailbox registry.
     */
    public MailboxRegistry mailboxRegistry() {
        return ctx.mailboxRegistry();
    }

    /** {@inheritDoc} */
    @Override public Object get(String name) {
        return params.get(name);
    }
}
