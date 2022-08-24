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

package org.apache.ignite.internal.processors.query.h2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.BulkLoadContextCursor;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.bulkload.BulkLoadAckClientParameters;
import org.apache.ignite.internal.processors.bulkload.BulkLoadCacheWriter;
import org.apache.ignite.internal.processors.bulkload.BulkLoadParser;
import org.apache.ignite.internal.processors.bulkload.BulkLoadProcessor;
import org.apache.ignite.internal.processors.bulkload.BulkLoadStreamerWriter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.NestedTxMode;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.processors.query.h2.dml.DmlBulkLoadDataConverter;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlterTableAddColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlterTableDropColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.sql.SqlCommandProcessor;
import org.apache.ignite.internal.sql.command.SqlBeginTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCommitTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.apache.ignite.internal.sql.command.SqlIndexColumn;
import org.apache.ignite.internal.sql.command.SqlRollbackTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlSetStreamingCommand;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.h2.command.Prepared;
import org.h2.command.ddl.AlterTableAlterColumn;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.CreateTable;
import org.h2.command.ddl.DropIndex;
import org.h2.command.ddl.DropTable;
import org.h2.command.dml.NoOperation;
import org.h2.table.Column;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.mvccEnabled;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.tx;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.txStart;
import static org.apache.ignite.internal.processors.query.QueryUtils.convert;
import static org.apache.ignite.internal.processors.query.QueryUtils.isDdlOnSchemaSupported;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser.PARAM_WRAP_VALUE;

/**
 * Processor responsible for execution of all non-SELECT and non-DML commands.
 */
public class CommandProcessor extends SqlCommandProcessor {
    /** Schema manager. */
    private final H2SchemaManager schemaMgr;

    /** H2 Indexing. */
    private final IgniteH2Indexing idx;

    /** Is backward compatible handling of UUID through DDL enabled. */
    private static final boolean handleUuidAsByte =
            IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SQL_UUID_DDL_BYTE_FORMAT, false);

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param schemaMgr Schema manager.
     */
    public CommandProcessor(GridKernalContext ctx, H2SchemaManager schemaMgr, IgniteH2Indexing idx) {
        super(ctx);

        this.schemaMgr = schemaMgr;
        this.idx = idx;
    }

    /**
     * Execute command.
     *
     * @param sql SQL.
     * @param cmdNative Native command (if any).
     * @param cmdH2 H2 command (if any).
     * @param params Parameters.
     * @param cliCtx Client context.
     * @param qryId Running query ID.
     * @return Result.
     */
    public CommandResult runCommand(String sql, SqlCommand cmdNative, GridSqlStatement cmdH2,
        QueryParameters params, @Nullable SqlClientContext cliCtx, long qryId) throws IgniteCheckedException {
        assert cmdNative != null || cmdH2 != null;

        // Do execute.
        FieldsQueryCursor<List<?>> res = H2Utils.zeroCursor();
        boolean unregister = true;

        if (cmdH2 != null) {
            assert cmdNative == null;

            // Some commands are duplicated.
            cmdNative = convertH2Command(cmdH2);
        }

        if (cmdNative != null) {
            if (isCommandSupported(cmdNative)) {
                FieldsQueryCursor<List<?>> resNative = runNativeCommand(sql, cmdNative, params, cliCtx, qryId);

                if (resNative != null) {
                    res = resNative;
                    unregister = !(cmdNative instanceof SqlBulkLoadCommand);
                }
            }
            else
                throw new UnsupportedOperationException("Unsupported command: " + cmdNative);
        }
        else {
            assert cmdH2 != null;

            runCommandH2(sql, cmdH2);
        }

        return new CommandResult(res, unregister);
    }

    /**
     * Execute native command.
     *
     * @param sql SQL.
     * @param cmdNative Native command.
     * @param params Parameters.
     * @param cliCtx Client context.
     * @param qryId Running query ID.
     * @return Result.
     */
    public FieldsQueryCursor<List<?>> runNativeCommand(String sql, SqlCommand cmdNative,
        QueryParameters params, @Nullable SqlClientContext cliCtx, Long qryId) throws IgniteCheckedException {
        if (super.isCommandSupported(cmdNative))
            return runCommand(cmdNative);

        if (cmdNative instanceof SqlBulkLoadCommand)
            return processBulkLoadCommand((SqlBulkLoadCommand)cmdNative, qryId);
        else if (cmdNative instanceof SqlSetStreamingCommand)
            processSetStreamingCommand((SqlSetStreamingCommand)cmdNative, cliCtx);
        else
            processTxCommand(cmdNative, params);

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isCommandSupported(SqlCommand cmd) {
        return super.isCommandSupported(cmd)
            || cmd instanceof SqlBeginTransactionCommand
            || cmd instanceof SqlCommitTransactionCommand
            || cmd instanceof SqlRollbackTransactionCommand
            || cmd instanceof SqlBulkLoadCommand
            || cmd instanceof SqlSetStreamingCommand;
    }

    /**
     * Converts H2 command to corresponding native command if possible.
     *
     * @param cmdH2 H2 command.
     * @return Native command.
     **/
    private @Nullable SqlCommand convertH2Command(GridSqlStatement cmdH2) {
        if (cmdH2 instanceof GridSqlCreateIndex) {
            GridSqlCreateIndex cmd = (GridSqlCreateIndex)cmdH2;

            return new SqlCreateIndexCommand(
                cmd.schemaName(),
                cmd.tableName(),
                cmd.index().getName(),
                cmd.ifNotExists(),
                cmd.index().getFields().entrySet().stream().map(e -> new SqlIndexColumn(e.getKey(), !e.getValue()))
                    .collect(Collectors.toList()),
                cmd.index().getIndexType() == QueryIndexType.GEOSPATIAL,
                0,
                -1
            );
        }
        else if (cmdH2 instanceof GridSqlDropIndex) {
            GridSqlDropIndex cmd = (GridSqlDropIndex)cmdH2;

            return new SqlDropIndexCommand(
                cmd.schemaName(),
                cmd.indexName(),
                cmd.ifExists()
            );
        }

        return null;
    }

    /**
     * Execute DDL statement.
     *
     * @param sql SQL.
     * @param cmdH2 Command.
     */
    private void runCommandH2(String sql, GridSqlStatement cmdH2) {
        IgniteInternalFuture fut = null;

        try {
            finishActiveTxIfNecessary();

            if (cmdH2 instanceof GridSqlCreateTable) {
                GridSqlCreateTable cmd = (GridSqlCreateTable)cmdH2;

                ctx.security().authorize(cmd.cacheName(), SecurityPermission.CACHE_CREATE);

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = schemaMgr.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl != null) {
                    if (!cmd.ifNotExists())
                        throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_EXISTS,
                            cmd.tableName());
                }
                else {
                    QueryEntity e = toQueryEntity(cmd);

                    CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(cmd.tableName());

                    ccfg.setQueryEntities(Collections.singleton(e));
                    ccfg.setSqlSchema(cmd.schemaName());

                    SchemaOperationException err =
                        QueryUtils.checkQueryEntityConflicts(ccfg, ctx.cache().cacheDescriptors().values());

                    if (err != null)
                        throw err;

                    if (!F.isEmpty(cmd.cacheName()) && ctx.cache().cacheDescriptor(cmd.cacheName()) != null) {
                        ctx.query().dynamicAddQueryEntity(
                                cmd.cacheName(),
                                cmd.schemaName(),
                                e,
                                cmd.parallelism(),
                                true
                        ).get();
                    }
                    else {
                        ctx.query().dynamicTableCreate(
                            cmd.schemaName(),
                            e,
                            cmd.templateName(),
                            cmd.cacheName(),
                            cmd.cacheGroup(),
                            cmd.dataRegionName(),
                            cmd.affinityKey(),
                            cmd.atomicityMode(),
                            cmd.writeSynchronizationMode(),
                            cmd.backups(),
                            cmd.ifNotExists(),
                            cmd.encrypted(),
                            cmd.parallelism()
                        );
                    }
                }
            }
            else if (cmdH2 instanceof GridSqlDropTable) {
                GridSqlDropTable cmd = (GridSqlDropTable)cmdH2;

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = schemaMgr.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl == null) {
                    if (!cmd.ifExists())
                        throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND,
                            cmd.tableName());
                }
                else {
                    ctx.security().authorize(tbl.cacheName(), SecurityPermission.CACHE_DESTROY);

                    ctx.query().dynamicTableDrop(tbl.cacheName(), cmd.tableName(), cmd.ifExists());
                }
            }
            else if (cmdH2 instanceof GridSqlAlterTableAddColumn) {
                GridSqlAlterTableAddColumn cmd = (GridSqlAlterTableAddColumn)cmdH2;

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = schemaMgr.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl == null) {
                    if (!cmd.ifTableExists())
                        throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND,
                            cmd.tableName());
                }
                else {
                    if (QueryUtils.isSqlType(tbl.rowDescriptor().type().valueClass()))
                        throw new SchemaOperationException("Cannot add column(s) because table was created " +
                            "with " + PARAM_WRAP_VALUE + "=false option.");

                    List<QueryField> cols = new ArrayList<>(cmd.columns().length);

                    boolean allFieldsNullable = true;

                    for (GridSqlColumn col : cmd.columns()) {
                        if (tbl.doesColumnExist(col.columnName())) {
                            if ((!cmd.ifNotExists() || cmd.columns().length != 1)) {
                                throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_EXISTS,
                                    col.columnName());
                            }
                            else {
                                cols = null;

                                break;
                            }
                        }

                        QueryField field = new QueryField(col.columnName(),
                            getTypeClassName(col),
                            col.column().isNullable(), col.defaultValue(),
                            col.precision(), col.scale());

                        cols.add(field);

                        allFieldsNullable &= field.isNullable();
                    }

                    if (cols != null) {
                        assert tbl.rowDescriptor() != null;

                        if (!allFieldsNullable)
                            QueryUtils.checkNotNullAllowed(tbl.cacheInfo().config());

                        fut = ctx.query().dynamicColumnAdd(tbl.cacheName(), cmd.schemaName(),
                            tbl.rowDescriptor().type().tableName(), cols, cmd.ifTableExists(), cmd.ifNotExists());
                    }
                }
            }
            else if (cmdH2 instanceof GridSqlAlterTableDropColumn) {
                GridSqlAlterTableDropColumn cmd = (GridSqlAlterTableDropColumn)cmdH2;

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = schemaMgr.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl == null) {
                    if (!cmd.ifTableExists())
                        throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND,
                            cmd.tableName());
                }
                else {
                    assert tbl.rowDescriptor() != null;

                    GridCacheContext cctx = tbl.cacheContext();

                    assert cctx != null;

                    if (cctx.mvccEnabled())
                        throw new IgniteSQLException("Cannot drop column(s) with enabled MVCC. " +
                            "Operation is unsupported at the moment.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

                    if (QueryUtils.isSqlType(tbl.rowDescriptor().type().valueClass()))
                        throw new SchemaOperationException("Cannot drop column(s) because table was created " +
                            "with " + PARAM_WRAP_VALUE + "=false option.");

                    List<String> cols = new ArrayList<>(cmd.columns().length);

                    GridQueryTypeDescriptor type = tbl.rowDescriptor().type();

                    for (String colName : cmd.columns()) {
                        if (!tbl.doesColumnExist(colName)) {
                            if ((!cmd.ifExists() || cmd.columns().length != 1)) {
                                throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_NOT_FOUND,
                                    colName);
                            }
                            else {
                                cols = null;

                                break;
                            }
                        }

                        SchemaOperationException err = QueryUtils.validateDropColumn(type, colName);

                        if (err != null)
                            throw err;

                        cols.add(colName);
                    }

                    if (cols != null) {
                        fut = ctx.query().dynamicColumnRemove(tbl.cacheName(), cmd.schemaName(),
                            type.tableName(), cols, cmd.ifTableExists(), cmd.ifExists());
                    }
                }
            }
            else
                throw new IgniteSQLException("Unsupported DDL operation: " + sql,
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

            if (fut != null)
                fut.get();
        }
        catch (SchemaOperationException e) {
            U.error(null, "DDL operation failure", e);
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
     * Convert this statement to query entity and do Ignite specific sanity checks on the way.
     * @return Query entity mimicking this SQL statement.
     */
    private static QueryEntity toQueryEntity(GridSqlCreateTable createTbl) {
        QueryEntityEx res = new QueryEntityEx();

        res.setTableName(createTbl.tableName());

        Set<String> notNullFields = null;

        HashMap<String, Object> dfltValues = new HashMap<>();

        Map<String, Integer> precision = new HashMap<>();
        Map<String, Integer> scale = new HashMap<>();

        for (Map.Entry<String, GridSqlColumn> e : createTbl.columns().entrySet()) {
            GridSqlColumn gridCol = e.getValue();

            Column col = gridCol.column();

            res.addQueryField(e.getKey(), getTypeClassName(gridCol), null);

            if (!col.isNullable()) {
                if (notNullFields == null)
                    notNullFields = new HashSet<>();

                notNullFields.add(e.getKey());
            }

            Object dfltVal = gridCol.defaultValue();

            if (dfltVal != null)
                dfltValues.put(e.getKey(), dfltVal);

            if (col.getType() == Value.DECIMAL) {
                if (col.getPrecision() < H2Utils.DECIMAL_DEFAULT_PRECISION)
                    precision.put(e.getKey(), (int)col.getPrecision());

                if (col.getScale() < H2Utils.DECIMAL_DEFAULT_SCALE)
                    scale.put(e.getKey(), col.getScale());
            }

            if (col.getType() == Value.STRING ||
                col.getType() == Value.STRING_FIXED ||
                col.getType() == Value.STRING_IGNORECASE ||
                col.getType() == Value.BYTES)
                if (col.getPrecision() < H2Utils.STRING_DEFAULT_PRECISION)
                    precision.put(e.getKey(), (int)col.getPrecision());
        }

        if (!F.isEmpty(dfltValues))
            res.setDefaultFieldValues(dfltValues);

        if (!F.isEmpty(precision))
            res.setFieldsPrecision(precision);

        if (!F.isEmpty(scale))
            res.setFieldsScale(scale);

        String valTypeName = QueryUtils.createTableValueTypeName(createTbl.schemaName(), createTbl.tableName());
        String keyTypeName = QueryUtils.createTableKeyTypeName(valTypeName);

        if (!F.isEmpty(createTbl.keyTypeName()))
            keyTypeName = createTbl.keyTypeName();

        if (!F.isEmpty(createTbl.valueTypeName()))
            valTypeName = createTbl.valueTypeName();

        assert createTbl.wrapKey() != null;
        assert createTbl.wrapValue() != null;

        if (!createTbl.wrapKey()) {
            GridSqlColumn pkCol = createTbl.columns().get(createTbl.primaryKeyColumns().iterator().next());

            keyTypeName = getTypeClassName(pkCol);

            res.setKeyFieldName(pkCol.columnName());
        }
        else {
            res.setKeyFields(createTbl.primaryKeyColumns());

            res.setPreserveKeysOrder(true);
        }

        if (!createTbl.wrapValue()) {
            GridSqlColumn valCol = null;

            for (Map.Entry<String, GridSqlColumn> e : createTbl.columns().entrySet()) {
                if (!createTbl.primaryKeyColumns().contains(e.getKey())) {
                    valCol = e.getValue();

                    break;
                }
            }

            assert valCol != null;

            valTypeName = getTypeClassName(valCol);

            res.setValueFieldName(valCol.columnName());
        }

        res.setValueType(valTypeName);
        res.setKeyType(keyTypeName);

        if (!F.isEmpty(notNullFields))
            res.setNotNullFields(notNullFields);

        // Fill key object with all fields for new tables.
        res.fillAbsentPKsWithDefaults(true);

        if (Objects.nonNull(createTbl.primaryKeyInlineSize()))
            res.setPrimaryKeyInlineSize(createTbl.primaryKeyInlineSize());

        if (Objects.nonNull(createTbl.affinityKeyInlineSize()))
            res.setAffinityKeyInlineSize(createTbl.affinityKeyInlineSize());

        return res;
    }

    /**
     * @param cmd Statement.
     * @return Whether {@code cmd} is a DDL statement we're able to handle.
     */
    public static boolean isCommand(Prepared cmd) {
        return cmd instanceof CreateIndex || cmd instanceof DropIndex || cmd instanceof CreateTable ||
            cmd instanceof DropTable || cmd instanceof AlterTableAlterColumn;
    }

    /**
     * @param cmd Statement.
     * @return Whether {@code cmd} is a no-op.
     */
    public static boolean isCommandNoOp(Prepared cmd) {
        return cmd instanceof NoOperation;
    }

    /**
     * Helper function for obtaining type class name for H2.
     *
     * @param col Column.
     * @return Type class name.
     */
    private static String getTypeClassName(GridSqlColumn col) {
        int type = col.column().getType();

        switch (type) {
            case Value.UUID :
                if (!handleUuidAsByte)
                    return UUID.class.getName();

            default:
                return DataType.getTypeClassName(type);
        }
    }

    /**
     * Process transactional command.
     * @param cmd Command.
     * @param params Parameters.
     * @throws IgniteCheckedException if failed.
     */
    private void processTxCommand(SqlCommand cmd, QueryParameters params)
        throws IgniteCheckedException {
        NestedTxMode nestedTxMode = params.nestedTxMode();

        GridNearTxLocal tx = tx(ctx);

        if (cmd instanceof SqlBeginTransactionCommand) {
            if (!mvccEnabled(ctx))
                throw new IgniteSQLException("MVCC must be enabled in order to start transaction.",
                    IgniteQueryErrorCode.MVCC_DISABLED);

            if (tx != null) {
                if (nestedTxMode == null)
                    nestedTxMode = NestedTxMode.DEFAULT;

                switch (nestedTxMode) {
                    case COMMIT:
                        doCommit(tx);

                        txStart(ctx, params.timeout());

                        break;

                    case IGNORE:
                        log.warning("Transaction has already been started, ignoring BEGIN command.");

                        break;

                    case ERROR:
                        throw new IgniteSQLException("Transaction has already been started.",
                            IgniteQueryErrorCode.TRANSACTION_EXISTS);

                    default:
                        throw new IgniteSQLException("Unexpected nested transaction handling mode: " +
                            nestedTxMode.name());
                }
            }
            else
                txStart(ctx, params.timeout());
        }
        else if (cmd instanceof SqlCommitTransactionCommand) {
            // Do nothing if there's no transaction.
            if (tx != null)
                doCommit(tx);
        }
        else {
            assert cmd instanceof SqlRollbackTransactionCommand;

            // Do nothing if there's no transaction.
            if (tx != null)
                doRollback(tx);
        }
    }

    /**
     * Commit and properly close transaction.
     * @param tx Transaction.
     * @throws IgniteCheckedException if failed.
     */
    private void doCommit(@NotNull GridNearTxLocal tx) throws IgniteCheckedException {
        try {
            tx.commit();
        }
        finally {
            closeTx(tx);
        }
    }

    /**
     * Rollback and properly close transaction.
     * @param tx Transaction.
     * @throws IgniteCheckedException if failed.
     */
    public void doRollback(@NotNull GridNearTxLocal tx) throws IgniteCheckedException {
        try {
            tx.rollback();
        }
        finally {
            closeTx(tx);
        }
    }

    /**
     * Properly close transaction.
     * @param tx Transaction.
     * @throws IgniteCheckedException if failed.
     */
    private void closeTx(@NotNull GridNearTxLocal tx) throws IgniteCheckedException {
        try {
            tx.close();
        }
        finally {
            ctx.cache().context().tm().resetContext();
        }
    }

    /**
     * Process SET STREAMING command.
     *
     * @param cmd Command.
     * @param cliCtx Client context.
     */
    private void processSetStreamingCommand(SqlSetStreamingCommand cmd,
        @Nullable SqlClientContext cliCtx) {
        if (cliCtx == null)
            throw new IgniteSQLException("SET STREAMING command can only be executed from JDBC or ODBC driver.");

        if (cmd.isTurnOn())
            cliCtx.enableStreaming(
                cmd.allowOverwrite(),
                cmd.flushFrequency(),
                cmd.perNodeBufferSize(),
                cmd.perNodeParallelOperations(),
                cmd.isOrdered()
            );
        else
            cliCtx.disableStreaming();
    }

    /**
     * Process bulk load COPY command.
     *
     * @param cmd The command.
     * @param qryId Query id.
     * @return The context (which is the result of the first request/response).
     * @throws IgniteCheckedException If something failed.
     */
    private FieldsQueryCursor<List<?>> processBulkLoadCommand(SqlBulkLoadCommand cmd, long qryId)
        throws IgniteCheckedException {
        if (cmd.packetSize() == null)
            cmd.packetSize(BulkLoadAckClientParameters.DFLT_PACKET_SIZE);

        GridH2Table tbl = schemaMgr.dataTable(cmd.schemaName(), cmd.tableName());

        if (tbl == null) {
            throw new IgniteSQLException("Table does not exist: " + cmd.tableName(),
                IgniteQueryErrorCode.TABLE_NOT_FOUND);
        }

        H2Utils.checkAndStartNotStartedCache(ctx, tbl);

        UpdatePlan plan = UpdatePlanBuilder.planForBulkLoad(cmd, tbl);

        IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter = new DmlBulkLoadDataConverter(plan);

        IgniteDataStreamer<Object, Object> streamer = ctx.grid().dataStreamer(tbl.cacheName());

        BulkLoadCacheWriter outputWriter = new BulkLoadStreamerWriter(streamer);

        BulkLoadParser inputParser = BulkLoadParser.createParser(cmd.inputFormat());

        BulkLoadProcessor processor = new BulkLoadProcessor(inputParser, dataConverter, outputWriter,
            idx.runningQueryManager(), qryId, ctx.tracing());

        BulkLoadAckClientParameters params = new BulkLoadAckClientParameters(cmd.localFileName(), cmd.packetSize());

        return new BulkLoadContextCursor(processor, params);
    }
}
