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

package org.apache.ignite.internal.sql;

import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.ComputeMXBeanImpl;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.QueryMXBeanImpl;
import org.apache.ignite.internal.ServiceMXBeanImpl;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.internal.processors.query.schema.management.SchemaManager;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.sql.command.SqlAlterTableCommand;
import org.apache.ignite.internal.sql.command.SqlAlterUserCommand;
import org.apache.ignite.internal.sql.command.SqlAnalyzeCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlCreateUserCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropStatisticsCommand;
import org.apache.ignite.internal.sql.command.SqlDropUserCommand;
import org.apache.ignite.internal.sql.command.SqlIndexColumn;
import org.apache.ignite.internal.sql.command.SqlKillClientCommand;
import org.apache.ignite.internal.sql.command.SqlKillComputeTaskCommand;
import org.apache.ignite.internal.sql.command.SqlKillContinuousQueryCommand;
import org.apache.ignite.internal.sql.command.SqlKillQueryCommand;
import org.apache.ignite.internal.sql.command.SqlKillScanQueryCommand;
import org.apache.ignite.internal.sql.command.SqlKillServiceCommand;
import org.apache.ignite.internal.sql.command.SqlKillTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlRefreshStatitsicsCommand;
import org.apache.ignite.internal.sql.command.SqlStatisticsCommands;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.QueryUtils.convert;
import static org.apache.ignite.internal.processors.query.QueryUtils.isDdlOnSchemaSupported;

/**
 * Processor responsible for execution of native Ignite commands.
 */
public class SqlCommandProcessor {
    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Logger. */
    protected final IgniteLogger log;

    /** Schema manager. */
    protected final SchemaManager schemaMgr;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public SqlCommandProcessor(GridKernalContext ctx) {
        this.ctx = ctx;
        this.schemaMgr = ctx.query().schemaManager();
        log = ctx.log(getClass());
    }

    /**
     * Execute command.
     *
     * @param cmdNative Native command.
     * @return Result.
     */
    @Nullable public FieldsQueryCursor<List<?>> runCommand(SqlCommand cmdNative) {
        assert cmdNative != null;

        if (isDdl(cmdNative))
            runCommandNativeDdl(cmdNative);
        else if (cmdNative instanceof SqlKillClientCommand)
            processKillClientCommand((SqlKillClientCommand)cmdNative);
        else if (cmdNative instanceof SqlKillComputeTaskCommand)
            processKillComputeTaskCommand((SqlKillComputeTaskCommand)cmdNative);
        else if (cmdNative instanceof SqlKillTransactionCommand)
            processKillTxCommand((SqlKillTransactionCommand)cmdNative);
        else if (cmdNative instanceof SqlKillServiceCommand)
            processKillServiceTaskCommand((SqlKillServiceCommand)cmdNative);
        else if (cmdNative instanceof SqlKillScanQueryCommand)
            processKillScanQueryCommand((SqlKillScanQueryCommand)cmdNative);
        else if (cmdNative instanceof SqlKillContinuousQueryCommand)
            processKillContinuousQueryCommand((SqlKillContinuousQueryCommand)cmdNative);
        else if (cmdNative instanceof SqlKillQueryCommand)
            processKillQueryCommand((SqlKillQueryCommand)cmdNative);
        else if (cmdNative instanceof SqlAnalyzeCommand)
            processAnalyzeCommand((SqlAnalyzeCommand)cmdNative);
        else if (cmdNative instanceof SqlRefreshStatitsicsCommand)
            processRefreshStatisticsCommand((SqlRefreshStatitsicsCommand)cmdNative);
        else if (cmdNative instanceof SqlDropStatisticsCommand)
            processDropStatisticsCommand((SqlDropStatisticsCommand)cmdNative);

        return null;
    }

    /**
     * @return {@code True} if command is supported by this command processor.
     */
    public boolean isCommandSupported(SqlCommand cmd) {
        return cmd instanceof SqlCreateIndexCommand
            || cmd instanceof SqlDropIndexCommand
            || cmd instanceof SqlAlterTableCommand
            || cmd instanceof SqlCreateUserCommand
            || cmd instanceof SqlAlterUserCommand
            || cmd instanceof SqlDropUserCommand
            || cmd instanceof SqlKillClientCommand
            || cmd instanceof SqlKillComputeTaskCommand
            || cmd instanceof SqlKillServiceCommand
            || cmd instanceof SqlKillTransactionCommand
            || cmd instanceof SqlKillScanQueryCommand
            || cmd instanceof SqlKillContinuousQueryCommand
            || cmd instanceof SqlKillQueryCommand
            || cmd instanceof SqlStatisticsCommands;
    }

    /**
     * @param cmd Command.
     * @return {@code True} if this is supported DDL command.
     */
    private static boolean isDdl(SqlCommand cmd) {
        return cmd instanceof SqlCreateIndexCommand
            || cmd instanceof SqlDropIndexCommand
            || cmd instanceof SqlAlterTableCommand
            || cmd instanceof SqlCreateUserCommand
            || cmd instanceof SqlAlterUserCommand
            || cmd instanceof SqlDropUserCommand;
    }

    /**
     * Process kill query command
     *
     * @param cmd Command.
     */
    private void processKillQueryCommand(SqlKillQueryCommand cmd) {
        ctx.query().runningQueryManager().cancelQuery(cmd.nodeQueryId(), cmd.nodeId(), cmd.async());
    }

    /**
     * Process kill scan query cmd.
     *
     * @param cmd Command.
     */
    private void processKillScanQueryCommand(SqlKillScanQueryCommand cmd) {
        new QueryMXBeanImpl(ctx)
            .cancelScan(cmd.getOriginNodeId(), cmd.getCacheName(), cmd.getQryId());
    }

    /**
     * Process kill client command.
     *
     * @param cmd Command.
     */
    private void processKillClientCommand(SqlKillClientCommand cmd) {
        if (cmd.connectionId() == null)
            ctx.sqlListener().mxBean().dropAllConnections();
        else
            ctx.sqlListener().mxBean().dropConnection(cmd.connectionId());
    }

    /**
     * Process kill compute task command.
     *
     * @param cmd Command.
     */
    private void processKillComputeTaskCommand(SqlKillComputeTaskCommand cmd) {
        new ComputeMXBeanImpl(ctx).cancel(cmd.getSessionId());
    }

    /**
     * Process kill transaction cmd.
     *
     * @param cmd Command.
     */
    private void processKillTxCommand(SqlKillTransactionCommand cmd) {
        new TransactionsMXBeanImpl(ctx).cancel(cmd.getXid());
    }

    /**
     * Process kill service command.
     *
     * @param cmd Command.
     */
    private void processKillServiceTaskCommand(SqlKillServiceCommand cmd) {
        new ServiceMXBeanImpl(ctx).cancel(cmd.getName());
    }

    /**
     * Process kill continuous query cmd.
     *
     * @param cmd Command.
     */
    private void processKillContinuousQueryCommand(SqlKillContinuousQueryCommand cmd) {
        new QueryMXBeanImpl(ctx).cancelContinuous(cmd.getOriginNodeId(), cmd.getRoutineId());
    }

    /**
     * Process analyze command.
     *
     * @param cmd Sql analyze command.
     */
    private void processAnalyzeCommand(SqlAnalyzeCommand cmd) {
        ctx.security().authorize(SecurityPermission.CHANGE_STATISTICS);

        StatisticsObjectConfiguration objCfgs[] = cmd.configurations().stream()
            .map(t -> {
                if (t.key().schema() == null) {
                    StatisticsKey key = new StatisticsKey(cmd.schemaName(), t.key().obj());

                    return new StatisticsObjectConfiguration(key, t.columns().values(),
                        t.maxPartitionObsolescencePercent());
                }
                else
                    return t;
            }).toArray(StatisticsObjectConfiguration[]::new);

        try {
            ctx.query().statsManager().collectStatistics(objCfgs);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSQLException(e.getMessage(), e);
        }
    }

    /**
     * Process refresh statistics command.
     *
     * @param cmd Refresh statistics command.
     */
    private void processRefreshStatisticsCommand(SqlRefreshStatitsicsCommand cmd) {
        ctx.security().authorize(SecurityPermission.REFRESH_STATISTICS);

        StatisticsTarget[] targets = cmd.targets().stream()
            .map(t -> (t.schema() == null) ? new StatisticsTarget(cmd.schemaName(), t.obj(), t.columns()) : t)
            .toArray(StatisticsTarget[]::new);

        try {
            ctx.query().statsManager().refreshStatistics(targets);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSQLException(e.getMessage(), e);
        }
    }

    /**
     * Process drop statistics command.
     *
     * @param cmd Drop statistics command.
     */
    private void processDropStatisticsCommand(SqlDropStatisticsCommand cmd) {
        ctx.security().authorize(SecurityPermission.CHANGE_STATISTICS);

        StatisticsTarget[] targets = cmd.targets().stream()
            .map(t -> (t.schema() == null) ? new StatisticsTarget(cmd.schemaName(), t.obj(), t.columns()) : t)
            .toArray(StatisticsTarget[]::new);

        try {
            ctx.query().statsManager().dropStatistics(targets);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSQLException(e.getMessage(), e);
        }
    }

    /**
     * Run DDL statement.
     *
     * @param cmd Command.
     */
    private void runCommandNativeDdl(SqlCommand cmd) {
        IgniteInternalFuture<?> fut = null;

        try {
            isDdlOnSchemaSupported(cmd.schemaName());

            finishActiveTxIfNecessary();

            if (cmd instanceof SqlCreateIndexCommand) {
                SqlCreateIndexCommand cmd0 = (SqlCreateIndexCommand)cmd;

                TableDescriptor tbl = schemaMgr.table(cmd0.schemaName(), cmd0.tableName());

                if (tbl == null)
                    throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, cmd0.tableName());

                GridQueryTypeDescriptor typeDesc = tbl.type();
                GridCacheContextInfo<?, ?> cacheInfo = tbl.cacheInfo();

                QueryIndex newIdx = new QueryIndex();

                newIdx.setName(cmd0.indexName());

                newIdx.setIndexType(cmd0.spatial() ? QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);

                LinkedHashMap<String, Boolean> flds = new LinkedHashMap<>();

                for (SqlIndexColumn col : cmd0.columns()) {
                    GridQueryProperty prop = typeDesc.property(col.name());

                    if (prop == null)
                        throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_NOT_FOUND, col.name());

                    flds.put(prop.name(), !col.descending());
                }

                newIdx.setFields(flds);
                newIdx.setInlineSize(cmd0.inlineSize());

                fut = ctx.query().dynamicIndexCreate(cacheInfo.name(), cmd.schemaName(), typeDesc.tableName(),
                    newIdx, cmd0.ifNotExists(), cmd0.parallel());
            }
            else if (cmd instanceof SqlDropIndexCommand) {
                SqlDropIndexCommand cmd0 = (SqlDropIndexCommand)cmd;

                IndexDescriptor idxDesc = schemaMgr.index(cmd0.schemaName(), cmd0.indexName());

                // Do not allow to drop system indexes.
                if (idxDesc != null && !idxDesc.isPk() && !idxDesc.isAffinity() && !idxDesc.isProxy()) {
                    GridCacheContextInfo<?, ?> cacheInfo = idxDesc.table().cacheInfo();

                    fut = ctx.query().dynamicIndexDrop(cacheInfo.name(), cmd0.schemaName(), cmd0.indexName(),
                        cmd0.ifExists());
                }
                else {
                    if (cmd0.ifExists())
                        fut = new GridFinishedFuture<>();
                    else
                        throw new SchemaOperationException(SchemaOperationException.CODE_INDEX_NOT_FOUND,
                            cmd0.indexName());
                }
            }
            else if (cmd instanceof SqlAlterTableCommand) {
                SqlAlterTableCommand cmd0 = (SqlAlterTableCommand)cmd;

                TableDescriptor tbl = schemaMgr.table(cmd0.schemaName(), cmd0.tableName());

                if (tbl == null) {
                    throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND,
                        cmd0.tableName());
                }

                GridCacheContextInfo<?, ?> cacheInfo = tbl.cacheInfo();

                Boolean logging = cmd0.logging();

                assert logging != null : "Only LOGGING/NOLOGGING are supported at the moment.";

                IgniteCluster cluster = ctx.grid().cluster();

                if (logging) {
                    boolean res = cluster.enableWal(cacheInfo.name());

                    if (!res)
                        throw new IgniteSQLException("Logging already enabled for table: " + cmd0.tableName());
                }
                else {
                    boolean res = cluster.disableWal(cacheInfo.name());

                    if (!res)
                        throw new IgniteSQLException("Logging already disabled for table: " + cmd0.tableName());
                }

                fut = new GridFinishedFuture<>();
            }
            else if (cmd instanceof SqlCreateUserCommand) {
                SqlCreateUserCommand addCmd = (SqlCreateUserCommand)cmd;

                ctx.security().createUser(addCmd.userName(), addCmd.password().toCharArray());
            }
            else if (cmd instanceof SqlAlterUserCommand) {
                SqlAlterUserCommand altCmd = (SqlAlterUserCommand)cmd;

                ctx.security().alterUser(altCmd.userName(), altCmd.password().toCharArray());
            }
            else if (cmd instanceof SqlDropUserCommand) {
                SqlDropUserCommand dropCmd = (SqlDropUserCommand)cmd;

                ctx.security().dropUser(dropCmd.userName());
            }
            else
                throw new IgniteSQLException("Unsupported DDL operation: " + cmd,
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

            if (fut != null)
                fut.get();
        }
        catch (SchemaOperationException e) {
            throw convert(e);
        }
        catch (IgniteSQLException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteSQLException(e.getMessage(), e);
        }
    }

    /**
     * Commits active transaction if exists.
     *
     * @throws IgniteCheckedException If failed.
     */
    protected void finishActiveTxIfNecessary() throws IgniteCheckedException {
        try (GridNearTxLocal tx = MvccUtils.tx(ctx)) {
            if (tx == null)
                return;

            if (!tx.isRollbackOnly())
                tx.commit();
            else
                tx.rollback();
        }
    }
}
