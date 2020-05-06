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
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/**
 * Runtime context allowing access to the tables in a database.
 */
public class ExecutionContext implements DataContext {
    /** */
    private final UUID queryId;

    /** */
    private final PlanningContext ctx;

    /** */
    private final FragmentDescription fragmentDescription;

    /** */
    private final Map<String, Object> params;

    /** */
    private final QueryTaskExecutor executor;

    /** */
    private volatile boolean cancelled;

    /**
     * @param ctx Parent context.
     * @param queryId Query ID.
     * @param fragmentDescription Partitions information.
     * @param params Parameters.
     */
    public ExecutionContext(QueryTaskExecutor executor, PlanningContext ctx, UUID queryId,
        FragmentDescription fragmentDescription, Map<String, Object> params) {
        this.executor = executor;
        this.ctx = ctx;
        this.queryId = queryId;
        this.fragmentDescription = fragmentDescription;
        this.params = params;
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
        return queryId;
    }

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return fragmentDescription.fragmentId();
    }

    /**
     * @return Interested partitions.
     */
    public int[] partitions() {
        return fragmentDescription.partitions();
    }

    /** */
    public int partitionsCount () {
        return fragmentDescription.partitionsCount();
    }

    /**
     * @return Target mapping.
     */
    public NodesMapping targetMapping() {
        return fragmentDescription.targetMapping();
    }

    /** */
    public List<UUID> remoteSources(long exchangeId) {
        return fragmentDescription.remoteSources().get(exchangeId);
    }

    /** */
    public FragmentDescription fragmentDescription() {
        return fragmentDescription;
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
        executor.execute(queryId, fragmentId(), task);
    }
}
