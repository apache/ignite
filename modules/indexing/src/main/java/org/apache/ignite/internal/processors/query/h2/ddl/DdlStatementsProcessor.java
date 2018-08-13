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

package org.apache.ignite.internal.processors.query.h2.ddl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlterTableAddColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlterTableDropColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.security.SecurityContextHolder;
import org.apache.ignite.internal.sql.command.SqlAlterTableCommand;
import org.apache.ignite.internal.sql.command.SqlAlterUserCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlCreateUserCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropUserCommand;
import org.apache.ignite.internal.sql.command.SqlIndexColumn;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
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
import org.h2.table.Column;
import org.h2.value.DataType;
import org.h2.value.Value;

import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.UPDATE_RESULT_META;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser.PARAM_WRAP_VALUE;

/**
 * DDL statements processor.<p>
 * Contains higher level logic to handle operations as a whole and communicate with the client.
 */
public class DdlStatementsProcessor {
    /** Kernal context. */
    GridKernalContext ctx;

    /** Indexing. */
    IgniteH2Indexing idx;

    /**
     * Initialize message handlers and this' fields needed for further operation.
     *
     * @param ctx Kernal context.
     * @param idx Indexing.
     */
    public void start(final GridKernalContext ctx, IgniteH2Indexing idx) {
        this.ctx = ctx;
        this.idx = idx;
    }

    /**
     * Run DDL statement.
     *
     * @param sql Original SQL.
     * @param cmd Command.
     * @return Result.
     * @throws IgniteCheckedException On error.
     */
    @SuppressWarnings("unchecked")
    public FieldsQueryCursor<List<?>> runDdlStatement(String sql, SqlCommand cmd) throws IgniteCheckedException {
        IgniteInternalFuture fut = null;

        try {
            isDdlOnSchemaSupported(cmd.schemaName());

            if (cmd instanceof SqlCreateIndexCommand) {
                SqlCreateIndexCommand cmd0 = (SqlCreateIndexCommand)cmd;

                GridH2Table tbl = idx.dataTable(cmd0.schemaName(), cmd0.tableName());

                if (tbl == null)
                    throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, cmd0.tableName());

                assert tbl.rowDescriptor() != null;

                isDdlSupported(tbl);

                QueryIndex newIdx = new QueryIndex();

                newIdx.setName(cmd0.indexName());

                newIdx.setIndexType(cmd0.spatial() ? QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);

                LinkedHashMap<String, Boolean> flds = new LinkedHashMap<>();

                // Let's replace H2's table and property names by those operated by GridQueryProcessor.
                GridQueryTypeDescriptor typeDesc = tbl.rowDescriptor().type();

                for (SqlIndexColumn col : cmd0.columns()) {
                    GridQueryProperty prop = typeDesc.property(col.name());

                    if (prop == null)
                        throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_NOT_FOUND, col.name());

                    flds.put(prop.name(), !col.descending());
                }

                newIdx.setFields(flds);
                newIdx.setInlineSize(cmd0.inlineSize());

                fut = ctx.query().dynamicIndexCreate(tbl.cacheName(), cmd.schemaName(), typeDesc.tableName(),
                    newIdx, cmd0.ifNotExists(), cmd0.parallel());
            }
            else if (cmd instanceof SqlDropIndexCommand) {
                SqlDropIndexCommand cmd0 = (SqlDropIndexCommand)cmd;

                GridH2Table tbl = idx.dataTableForIndex(cmd0.schemaName(), cmd0.indexName());

                if (tbl != null) {
                    isDdlSupported(tbl);

                    fut = ctx.query().dynamicIndexDrop(tbl.cacheName(), cmd0.schemaName(), cmd0.indexName(),
                        cmd0.ifExists());
                }
                else {
                    if (cmd0.ifExists())
                        fut = new GridFinishedFuture();
                    else
                        throw new SchemaOperationException(SchemaOperationException.CODE_INDEX_NOT_FOUND,
                            cmd0.indexName());
                }
            }
            else if (cmd instanceof SqlAlterTableCommand) {
                SqlAlterTableCommand cmd0 = (SqlAlterTableCommand)cmd;

                GridH2Table tbl = idx.dataTable(cmd0.schemaName(), cmd0.tableName());

                if (tbl == null) {
                    ctx.cache().createMissingQueryCaches();

                    tbl = idx.dataTable(cmd0.schemaName(), cmd0.tableName());
                }

                if (tbl == null) {
                    throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND,
                        cmd0.tableName());
                }

                Boolean logging = cmd0.logging();

                assert logging != null : "Only LOGGING/NOLOGGING are supported at the moment.";

                IgniteCluster cluster = ctx.grid().cluster();

                if (logging) {
                    boolean res = cluster.enableWal(tbl.cacheName());

                    if (!res)
                        throw new IgniteSQLException("Logging already enabled for table: " + cmd0.tableName());
                }
                else {
                    boolean res = cluster.disableWal(tbl.cacheName());

                    if (!res)
                        throw new IgniteSQLException("Logging already disabled for table: " + cmd0.tableName());
                }

                fut = new GridFinishedFuture();
            }
            else if (cmd instanceof SqlCreateUserCommand) {
                SqlCreateUserCommand addCmd = (SqlCreateUserCommand)cmd;

                ctx.authentication().addUser(addCmd.userName(), addCmd.password());
            }
            else if (cmd instanceof SqlAlterUserCommand) {
                SqlAlterUserCommand altCmd = (SqlAlterUserCommand)cmd;

                ctx.authentication().updateUser(altCmd.userName(), altCmd.password());
            }
            else if (cmd instanceof SqlDropUserCommand) {
                SqlDropUserCommand dropCmd = (SqlDropUserCommand)cmd;

                ctx.authentication().removeUser(dropCmd.userName());
            }
            else
                throw new IgniteSQLException("Unsupported DDL operation: " + sql,
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

            if (fut != null)
                fut.get();

            return H2Utils.zeroCursor();
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
     * Execute DDL statement.
     *
     * @param sql SQL.
     * @param prepared Prepared.
     * @return Cursor on query results.
     * @throws IgniteCheckedException On error.
     */
    @SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
    public FieldsQueryCursor<List<?>> runDdlStatement(String sql, Prepared prepared)
        throws IgniteCheckedException {
        IgniteInternalFuture fut = null;

        try {
            GridSqlStatement stmt0 = new GridSqlQueryParser(false).parse(prepared);

            if (stmt0 instanceof GridSqlCreateIndex) {
                GridSqlCreateIndex cmd = (GridSqlCreateIndex)stmt0;

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl == null)
                    throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, cmd.tableName());

                assert tbl.rowDescriptor() != null;

                isDdlSupported(tbl);

                QueryIndex newIdx = new QueryIndex();

                newIdx.setName(cmd.index().getName());

                newIdx.setIndexType(cmd.index().getIndexType());

                LinkedHashMap<String, Boolean> flds = new LinkedHashMap<>();

                // Let's replace H2's table and property names by those operated by GridQueryProcessor.
                GridQueryTypeDescriptor typeDesc = tbl.rowDescriptor().type();

                for (Map.Entry<String, Boolean> e : cmd.index().getFields().entrySet()) {
                    GridQueryProperty prop = typeDesc.property(e.getKey());

                    if (prop == null)
                        throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_NOT_FOUND, e.getKey());

                    flds.put(prop.name(), e.getValue());
                }

                newIdx.setFields(flds);

                fut = ctx.query().dynamicIndexCreate(tbl.cacheName(), cmd.schemaName(), typeDesc.tableName(),
                    newIdx, cmd.ifNotExists(), 0);
            }
            else if (stmt0 instanceof GridSqlDropIndex) {
                GridSqlDropIndex cmd = (GridSqlDropIndex) stmt0;

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = idx.dataTableForIndex(cmd.schemaName(), cmd.indexName());

                if (tbl != null) {
                    isDdlSupported(tbl);

                    fut = ctx.query().dynamicIndexDrop(tbl.cacheName(), cmd.schemaName(), cmd.indexName(),
                        cmd.ifExists());
                }
                else {
                    if (cmd.ifExists())
                        fut = new GridFinishedFuture();
                    else
                        throw new SchemaOperationException(SchemaOperationException.CODE_INDEX_NOT_FOUND,
                            cmd.indexName());
                }
            }
            else if (stmt0 instanceof GridSqlCreateTable) {
                ctx.security().authorize(null, SecurityPermission.CACHE_CREATE, SecurityContextHolder.get());

                GridSqlCreateTable cmd = (GridSqlCreateTable)stmt0;

                isDdlOnSchemaSupported(cmd.schemaName());

                if (!F.eq(QueryUtils.DFLT_SCHEMA, cmd.schemaName()))
                    throw new SchemaOperationException("CREATE TABLE can only be executed on " +
                        QueryUtils.DFLT_SCHEMA + " schema.");

                GridH2Table tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());

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

                    ctx.query().dynamicTableCreate(cmd.schemaName(), e, cmd.templateName(), cmd.cacheName(),
                        cmd.cacheGroup(), cmd.dataRegionName(), cmd.affinityKey(), cmd.atomicityMode(),
                        cmd.writeSynchronizationMode(), cmd.backups(), cmd.ifNotExists());
                }
            }
            else if (stmt0 instanceof GridSqlDropTable) {
                ctx.security().authorize(null, SecurityPermission.CACHE_DESTROY, SecurityContextHolder.get());

                GridSqlDropTable cmd = (GridSqlDropTable)stmt0;

                isDdlOnSchemaSupported(cmd.schemaName());

                if (!F.eq(QueryUtils.DFLT_SCHEMA, cmd.schemaName()))
                    throw new SchemaOperationException("DROP TABLE can only be executed on " +
                        QueryUtils.DFLT_SCHEMA + " schema.");

                GridH2Table tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl == null && cmd.ifExists()) {
                    ctx.cache().createMissingQueryCaches();

                    tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());
                }

                if (tbl == null) {
                    if (!cmd.ifExists())
                        throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND,
                            cmd.tableName());
                }
                else
                    ctx.query().dynamicTableDrop(tbl.cacheName(), cmd.tableName(), cmd.ifExists());
            }
            else if (stmt0 instanceof GridSqlAlterTableAddColumn) {
                GridSqlAlterTableAddColumn cmd = (GridSqlAlterTableAddColumn)stmt0;

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl == null && cmd.ifTableExists()) {
                    ctx.cache().createMissingQueryCaches();

                    tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());
                }

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
                            DataType.getTypeClassName(col.column().getType()),
                            col.column().isNullable(), col.defaultValue(),
                            col.precision(), col.scale());

                        cols.add(field);

                        allFieldsNullable &= field.isNullable();
                    }

                    if (cols != null) {
                        assert tbl.rowDescriptor() != null;

                        if (!allFieldsNullable)
                            QueryUtils.checkNotNullAllowed(tbl.cache().config());

                        fut = ctx.query().dynamicColumnAdd(tbl.cacheName(), cmd.schemaName(),
                            tbl.rowDescriptor().type().tableName(), cols, cmd.ifTableExists(), cmd.ifNotExists());
                    }
                }
            }
            else if (stmt0 instanceof GridSqlAlterTableDropColumn) {
                GridSqlAlterTableDropColumn cmd = (GridSqlAlterTableDropColumn)stmt0;

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl == null && cmd.ifTableExists()) {
                    ctx.cache().createMissingQueryCaches();

                    tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());
                }

                if (tbl == null) {
                    if (!cmd.ifTableExists())
                        throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND,
                            cmd.tableName());
                }
                else {
                    assert tbl.rowDescriptor() != null;

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

            QueryCursorImpl<List<?>> resCur = (QueryCursorImpl<List<?>>)new QueryCursorImpl(Collections.singletonList
                (Collections.singletonList(0L)), null, false);

            resCur.fieldsMeta(UPDATE_RESULT_META);

            return resCur;
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
     * Check if schema supports DDL statement.
     *
     * @param schemaName Schema name.
     */
    private static void isDdlOnSchemaSupported(String schemaName) {
        if (F.eq(QueryUtils.SCHEMA_SYS, schemaName))
            throw new IgniteSQLException("DDL statements are not supported on " + schemaName + " schema",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Check if table supports DDL statement.
     *
     * @param tbl Table.
     */
    private static void isDdlSupported(GridH2Table tbl) {
        GridCacheContext cctx = tbl.cache();

        assert cctx != null;

        if (cctx.isLocal())
            throw new IgniteSQLException("DDL statements are not supported on LOCAL caches",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * @return {@link IgniteSQLException} with the message same as of {@code this}'s and
     */
    private IgniteSQLException convert(SchemaOperationException e) {
        int sqlCode;

        switch (e.code()) {
            case SchemaOperationException.CODE_CACHE_NOT_FOUND:
                sqlCode = IgniteQueryErrorCode.CACHE_NOT_FOUND;

                break;

            case SchemaOperationException.CODE_TABLE_NOT_FOUND:
                sqlCode = IgniteQueryErrorCode.TABLE_NOT_FOUND;

                break;

            case SchemaOperationException.CODE_TABLE_EXISTS:
                sqlCode = IgniteQueryErrorCode.TABLE_ALREADY_EXISTS;

                break;

            case SchemaOperationException.CODE_COLUMN_NOT_FOUND:
                sqlCode = IgniteQueryErrorCode.COLUMN_NOT_FOUND;

                break;

            case SchemaOperationException.CODE_COLUMN_EXISTS:
                sqlCode = IgniteQueryErrorCode.COLUMN_ALREADY_EXISTS;

                break;

            case SchemaOperationException.CODE_INDEX_NOT_FOUND:
                sqlCode = IgniteQueryErrorCode.INDEX_NOT_FOUND;

                break;

            case SchemaOperationException.CODE_INDEX_EXISTS:
                sqlCode = IgniteQueryErrorCode.INDEX_ALREADY_EXISTS;

                break;

            default:
                sqlCode = IgniteQueryErrorCode.UNKNOWN;
        }

        return new IgniteSQLException(e.getMessage(), sqlCode);
    }

    /**
     * Convert this statement to query entity and do Ignite specific sanity checks on the way.
     * @return Query entity mimicking this SQL statement.
     */
    private static QueryEntity toQueryEntity(GridSqlCreateTable createTbl) {
        QueryEntity res = new QueryEntity();

        res.setTableName(createTbl.tableName());

        Set<String> notNullFields = null;

        HashMap<String, Object> dfltValues = new HashMap<>();

        Map<String, IgniteBiTuple<Integer, Integer>> decimalInfo = new HashMap<>();

        for (Map.Entry<String, GridSqlColumn> e : createTbl.columns().entrySet()) {
            GridSqlColumn gridCol = e.getValue();

            Column col = gridCol.column();

            res.addQueryField(e.getKey(), DataType.getTypeClassName(col.getType()), null);

            if (!col.isNullable()) {
                if (notNullFields == null)
                    notNullFields = new HashSet<>();

                notNullFields.add(e.getKey());
            }

            Object dfltVal = gridCol.defaultValue();

            if (dfltVal != null)
                dfltValues.put(e.getKey(), dfltVal);

            if (col.getType() == Value.DECIMAL)
                decimalInfo.put(e.getKey(), F.t((int)col.getPrecision(), col.getScale()));
        }

        if (!F.isEmpty(dfltValues))
            res.setDefaultFieldValues(dfltValues);

        if (!F.isEmpty(decimalInfo))
            res.setDecimalInfo(decimalInfo);

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

            keyTypeName = DataType.getTypeClassName(pkCol.column().getType());

            res.setKeyFieldName(pkCol.columnName());
        }
        else
            res.setKeyFields(createTbl.primaryKeyColumns());

        if (!createTbl.wrapValue()) {
            GridSqlColumn valCol = null;

            for (Map.Entry<String, GridSqlColumn> e : createTbl.columns().entrySet()) {
                if (!createTbl.primaryKeyColumns().contains(e.getKey())) {
                    valCol = e.getValue();

                    break;
                }
            }

            assert valCol != null;

            valTypeName = DataType.getTypeClassName(valCol.column().getType());

            res.setValueFieldName(valCol.columnName());
        }

        res.setValueType(valTypeName);
        res.setKeyType(keyTypeName);

        if (!F.isEmpty(notNullFields)) {
            QueryEntityEx res0 = new QueryEntityEx(res);

            res0.setNotNullFields(notNullFields);

            res = res0;
        }

        return res;
    }

    /**
     * @param cmd Statement.
     * @return Whether {@code cmd} is a DDL statement we're able to handle.
     */
    public static boolean isDdlStatement(Prepared cmd) {
        return cmd instanceof CreateIndex || cmd instanceof DropIndex || cmd instanceof CreateTable ||
            cmd instanceof DropTable || cmd instanceof AlterTableAlterColumn;
    }
}
