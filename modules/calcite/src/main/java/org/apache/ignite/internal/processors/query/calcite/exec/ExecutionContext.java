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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/**
 * Runtime context allowing access to the tables in a database.
 */
public class ExecutionContext<Row> implements DataContext {
    /** */
    private final UUID qryId;

    /** */
    private final PlanningContext ctx;

    /** */
    private final FragmentDescription fragmentDesc;

    /** */
    private final Map<String, Object> params;

    /** */
    private final QueryTaskExecutor executor;

    /** */
    private final RowHandler<Row> handler;

    /** */
    private final ExpressionFactory<Row> expressionFactory;

    /** */
    private volatile boolean cancelled;

    /**
     * @param ctx Parent context.
     * @param qryId Query ID.
     * @param fragmentDesc Partitions information.
     * @param params Parameters.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ExecutionContext(
        QueryTaskExecutor executor,
        PlanningContext ctx,
        UUID qryId,
        FragmentDescription fragmentDesc,
        RowHandler<Row> handler,
        Map<String, Object> params
    ) {
        this.executor = executor;
        this.ctx = ctx;
        this.qryId = qryId;
        this.fragmentDesc = fragmentDesc;
        this.handler = handler;
        this.params = params;

        expressionFactory = new ExpressionFactoryImpl<>(this, ctx.typeFactory(), ctx.conformance());
    }

    /**
     * @return Parent context.
     */
    public PlanningContext planningContext() {
        return ctx;
    }

    /**
     * @return Query ID.
     */
    public UUID queryId() {
        return qryId;
    }

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return fragmentDesc.fragmentId();
    }

    /**
     * @return Interested partitions.
     */
    public int[] partitions() {
        return fragmentDesc.partitions();
    }

    /** */
    public int partitionsCount() {
        return fragmentDesc.partitionsCount();
    }

    /**
     * @return Target mapping.
     */
    public NodesMapping targetMapping() {
        return fragmentDesc.targetMapping();
    }

    /** */
    public List<UUID> remoteSources(long exchangeId) {
        return fragmentDesc.remoteSources().get(exchangeId);
    }

    /** */
    public FragmentDescription fragmentDescription() {
        return fragmentDesc;
    }

    /**
     * @return Keep binary flag.
     */
    public boolean keepBinary() {
        return true; // TODO
    }

    /**
     * @return MVCC snapshot.
     */
    public MvccSnapshot mvccSnapshot() {
        return null; // TODO
    }

    /**
     * @return Cancelled flag.
     */
    public boolean cancelled() {
        return cancelled;
    }

    /**
     * @return Handler to access row fields.
     */
    public RowHandler<Row> rowHandler() {
        return handler;
    }

    /**
     * @return Expression factory.
     */
    public ExpressionFactory<Row> expressionFactory() {
        return expressionFactory;
    }

    /**
     * @return Originating node ID.
     */
    public UUID originatingNodeId() {
        return planningContext().originatingNodeId();
    }

    /** {@inheritDoc} */
    @Override public SchemaPlus getRootSchema() {
        return ctx.schema();
    }

    /** {@inheritDoc} */
    @Override public IgniteTypeFactory getTypeFactory() {
        return ctx.typeFactory();
    }

    /** {@inheritDoc} */
    @Override public QueryProvider getQueryProvider() {
        return null; // TODO
    }

    /** {@inheritDoc} */
    @Override public Object get(String name) {
        return params.get(name);
    }

    /**
     * Sets cancelled flag.
     */
    public void markCancelled() {
        if (!cancelled)
            cancelled = true;
    }

    /**
     * Executes a query task.
     *
     * @param task Query task.
     */
    public void execute(Runnable task) {
        executor.execute(qryId, fragmentId(), task);
    }
}
