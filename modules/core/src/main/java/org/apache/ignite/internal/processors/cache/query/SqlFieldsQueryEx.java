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

package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.NestedTxMode;
import org.apache.ignite.internal.util.typedef.F;

/**
 * {@link SqlFieldsQuery} with experimental and internal features.
 */
public final class SqlFieldsQueryEx extends SqlFieldsQuery {
    /** */
    private static final long serialVersionUID = 0L;

    /** Flag to enforce checks for correct operation type. */
    private final Boolean isQry;

    /** Whether server side DML should be enabled. */
    private boolean skipReducerOnUpdate;

    /** Auto commit flag. */
    private boolean autoCommit = true;

    /** Nested transactions handling mode. */
    private NestedTxMode nestedTxMode = NestedTxMode.DEFAULT;

    /** Batched arguments list. */
    private List<Object[]> batchedArgs;

    /**
     * @param sql SQL query.
     * @param isQry Flag indicating whether this object denotes a query or an update operation.
     */
    public SqlFieldsQueryEx(String sql, Boolean isQry) {
        super(sql);

        this.isQry = isQry;
    }

    /**
     * @param qry SQL query.
     */
    private SqlFieldsQueryEx(SqlFieldsQueryEx qry) {
        super(qry);

        this.isQry = qry.isQry;
        this.skipReducerOnUpdate = qry.skipReducerOnUpdate;
        this.autoCommit = qry.autoCommit;
        this.nestedTxMode = qry.nestedTxMode;
        this.batchedArgs = qry.batchedArgs;
    }

    /**
     * @return Flag indicating whether this object denotes a query or an update operation.
     */
    public Boolean isQuery() {
        return isQry;
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQueryEx setSql(String sql) {
        super.setSql(sql);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQueryEx setArgs(Object... args) {
        super.setArgs(args);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQueryEx setTimeout(int timeout, TimeUnit timeUnit) {
        super.setTimeout(timeout, timeUnit);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQueryEx setCollocated(boolean collocated) {
        super.setCollocated(collocated);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQueryEx setEnforceJoinOrder(boolean enforceJoinOrder) {
        super.setEnforceJoinOrder(enforceJoinOrder);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQueryEx setDistributedJoins(boolean distributedJoins) {
        super.setDistributedJoins(distributedJoins);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQueryEx setPageSize(int pageSize) {
        super.setPageSize(pageSize);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQueryEx setLocal(boolean loc) {
        super.setLocal(loc);

        return this;
    }

    /**
     * Sets server side update flag.
     * <p>
     * By default, when processing DML command, Ignite first fetches all affected intermediate rows for analysis to the
     * node which initiated the query and only then forms batches of updated values to be sent to remote nodes.
     * For simple DML commands (that however affect great deal of rows) such approach may be an overkill in terms of
     * network delays and memory usage on initiating node. Use this flag as hint for Ignite to do all intermediate rows
     * analysis and updates in place on corresponding remote data nodes.
     * <p>
     * There are limitations to what DML command can be optimized this way. The command containing LIMIT, OFFSET,
     * DISTINCT, ORDER BY, GROUP BY, sub-query or UNION will be processed the usual way despite this flag setting.
     * <p>
     * Defaults to {@code false}, meaning that intermediate results will be fetched to initiating node first.
     * Only affects DML commands. Ignored when {@link #isLocal()} is {@code true}.
     * Note that when set to {@code true}, the query may fail in the case of even single node failure.
     *
     * @param skipReducerOnUpdate Server side update flag.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setSkipReducerOnUpdate(boolean skipReducerOnUpdate) {
        this.skipReducerOnUpdate = skipReducerOnUpdate;

        return this;
    }

    /**
     * Gets server side update flag.
     * <p>
     * See {@link #setSkipReducerOnUpdate(boolean)} for more information.
     *
     * @return Server side update flag.
     */
    public boolean isSkipReducerOnUpdate() {
        return skipReducerOnUpdate;
    }

    /**
     * @return Nested transactions handling mode - behavior when the user attempts to open a transaction in scope of
     * another transaction.
     */
    public NestedTxMode getNestedTxMode() {
        return nestedTxMode;
    }

    /**
     * @param nestedTxMode Nested transactions handling mode - behavior when the user attempts to open a transaction
     * in scope of another transaction.
     */
    public void setNestedTxMode(NestedTxMode nestedTxMode) {
        this.nestedTxMode = nestedTxMode;
    }

    /**
     * @return Auto commit flag.
     */
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * @param autoCommit Auto commit flag.
     */
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQuery copy() {
        return new SqlFieldsQueryEx(this);
    }

    /**
     * Adds batched arguments.
     *
     * @param args Batched arguments.
     */
    public void addBatchedArgs(Object[] args) {
        if (this.batchedArgs == null)
            this.batchedArgs = new ArrayList<>();

        this.batchedArgs.add(args);
    }

    /**
     * Clears batched arguments.
     */
    public void clearBatchedArgs() {
        this.batchedArgs = null;
    }

    /**
     * Returns batched arguments.
     *
     * @return Batched arguments.
     */
    public List<Object[]> batchedArguments() {
        return this.batchedArgs;
    }

    /**
     * Checks if query is batched.
     *
     * @return {@code True} if batched.
     */
    public boolean isBatched() {
        return !F.isEmpty(batchedArgs);
    }
}
