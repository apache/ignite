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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlterTableAddColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.sql.command.SqlColumn;
import org.apache.ignite.internal.sql.command.SqlColumnType;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlCreateTableCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropTableCommand;
import org.apache.ignite.internal.sql.command.SqlIndexColumn;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.command.Prepared;
import org.h2.command.ddl.AlterTableAlterColumn;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.CreateTable;
import org.h2.command.ddl.DropIndex;
import org.h2.command.ddl.DropTable;
import org.h2.table.Column;
import org.h2.value.DataType;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.UPDATE_RESULT_META;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser.PARAM_WRAP_VALUE;
import static org.apache.ignite.internal.sql.SqlKeyword.KEY_TYPE;
import static org.apache.ignite.internal.sql.SqlKeyword.VAL_TYPE;
import static org.apache.ignite.internal.sql.SqlKeyword.WRAP_KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.WRAP_VALUE;

/**
 * DDL statements processor.<p>
 * Contains higher level logic to handle operations as a whole and communicate with the client.
 */
public class DdlStatementsProcessor {
    /** Kernal context. */
    private GridKernalContext ctx;

    /** Indexing. */
    private IgniteH2Indexing idx;

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
    public FieldsQueryCursor<List<?>> runDdlStatement(String sql, SqlCommand cmd) throws IgniteCheckedException{
        IgniteInternalFuture fut = null;

        try {
            if (cmd instanceof SqlCreateIndexCommand)

                fut = makeFutureForCreateIndex((SqlCreateIndexCommand) cmd);

            else if (cmd instanceof SqlDropIndexCommand)

                fut = makeFutureForDropIndex((SqlDropIndexCommand) cmd);

            else if (cmd instanceof SqlCreateTableCommand)

                createTable((SqlCreateTableCommand) cmd);

            else if (cmd instanceof SqlDropTableCommand)

                dropTable((SqlDropTableCommand) cmd);

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
            throw convert(e);
        }
        catch (IgniteSQLException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteSQLException("Unexpected DDL operation failure: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a table taking parameters from {@link SqlCreateTableCommand}.
     * @param cmd Command to take parameters from.
     * @throws IgniteCheckedException If failed.
     */
    private void createTable(SqlCreateTableCommand cmd) throws IgniteCheckedException {

        if (!F.eq(QueryUtils.DFLT_SCHEMA, cmd.schemaName()) && cmd.schemaName() != null)
            throw new SchemaOperationException("CREATE TABLE can only be executed on " +
                QueryUtils.DFLT_SCHEMA + " schema.");

        GridH2Table tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());

        if (tbl != null) {
            if (!cmd.ifNotExists())
                throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_EXISTS,
                    cmd.tableName());
        }
        else {
            postProcessCreateTableCmd(cmd);

            QueryEntity qryEnt = toQueryEntity(cmd);

            CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(cmd.tableName());

            ccfg.setQueryEntities(Collections.singleton(qryEnt));
            ccfg.setSqlSchema(cmd.schemaName());

            SchemaOperationException err =
                QueryUtils.checkQueryEntityConflicts(ccfg, ctx.cache().cacheDescriptors().values());

            if (err != null)
                throw err;

            ctx.query().dynamicTableCreate(cmd.schemaName(), qryEnt, cmd.templateName(), cmd.cacheName(),
                cmd.cacheGroup(), cmd.dataRegionName(), cmd.affinityKey(), cmd.atomicityMode(),
                cmd.writeSynchronizationMode(), cmd.backups(), cmd.ifNotExists());
        }
    }

    /**
     * Post-processes the {@link SqlCreateTableCommand} by setting default values or combinations of parameters
     * to proper values.
     *
     * @param cmd The command to fix.
     */
    private void postProcessCreateTableCmd(SqlCreateTableCommand cmd) {

        if (F.isEmpty(cmd.templateName()))
            cmd.templateName(QueryUtils.TEMPLATE_PARTITIONED);

        Map<String, SqlColumn> cols = cmd.columns();

        if (cols.containsKey(QueryUtils.KEY_FIELD_NAME.toUpperCase()) ||
            cols.containsKey(QueryUtils.VAL_FIELD_NAME.toUpperCase()))
            throw new IgniteSQLException("Direct specification of _KEY and _VAL columns is forbidden",
                IgniteQueryErrorCode.PARSING);

        if (F.isEmpty(cmd.primaryKeyColumnNames()))
            throw new IgniteSQLException("No PRIMARY KEY columns specified");

        int keyColsNum = cmd.primaryKeyColumnNames().size();
        int valColsNum = cols.size() - keyColsNum;

        if (valColsNum == 0)
            throw new IgniteSQLException("Table must have at least one non PRIMARY KEY column.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        // Process key wrapping.

        Boolean wrapKey = cmd.wrapKey();

        if (wrapKey != null && !wrapKey) {
            if (keyColsNum > 1) {
                throw new IgniteSQLException(WRAP_KEY + " cannot be false when composite primary key exists.",
                    IgniteQueryErrorCode.PARSING);
            }

            if (!F.isEmpty(cmd.keyTypeName())) {
                throw new IgniteSQLException(WRAP_KEY + " cannot be false when " + KEY_TYPE + " is set.",
                    IgniteQueryErrorCode.PARSING);
            }
        }

        boolean wrapKey0 = (cmd.wrapKey() != null && cmd.wrapKey()) || !F.isEmpty(cmd.keyTypeName()) || keyColsNum > 1;

        cmd.wrapKey(wrapKey0);

        // Process value wrapping.
        Boolean wrapVal = cmd.wrapValue();

        if (wrapVal != null && !wrapVal) {
            if (valColsNum > 1) {
                throw new IgniteSQLException(WRAP_VALUE + " cannot be false when multiple non-primary key " +
                    "columns exist.", IgniteQueryErrorCode.PARSING);
            }

            if (!F.isEmpty(cmd.valueTypeName())) {
                throw new IgniteSQLException(WRAP_VALUE + " cannot be false when " + VAL_TYPE + " is set.",
                    IgniteQueryErrorCode.PARSING);
            }

            cmd.wrapValue(false);
        }
        else
            cmd.wrapValue(true); // By default value is always wrapped to allow for ALTER TABLE ADD COLUMN commands.

        if (!F.isEmpty(cmd.valueTypeName()) && F.eq(cmd.keyTypeName(), cmd.valueTypeName()))
            throw new IgniteSQLException("Key and value type names " +
                "should be different for CREATE TABLE: " + cmd.valueTypeName(), IgniteQueryErrorCode.PARSING);

        if (cmd.affinityKey() == null) {
            if (cmd.primaryKeyColumnNames().size() == 1 && cmd.wrapKey())
                cmd.affinityKey(cmd.primaryKeyColumnNames().iterator().next());
        }

        if (cmd.backups() == null)
            cmd.backups(0);
    }

    /**
     * Drops a table taking parameters from {@link SqlDropTableCommand}.
     *
     * @param cmd The command to take parameters from.
     * @throws IgniteCheckedException If failed.
     */
    private void dropTable(SqlDropTableCommand cmd) throws IgniteCheckedException {

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

    /**
     * Creates a future for {@link SqlCreateIndexCommand}.
     *
     * @param cmd The command to wrap.
     * @return A future that wraps the command.
     * @throws SchemaOperationException If a table or a particular column cannot be found.
     */
    private IgniteInternalFuture makeFutureForCreateIndex(SqlCreateIndexCommand cmd) throws SchemaOperationException {
        IgniteInternalFuture fut;

        GridH2Table tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());

        if (tbl == null)
            throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, cmd.tableName());

        assert tbl.rowDescriptor() != null;

        QueryIndex newIdx = new QueryIndex();

        newIdx.setName(cmd.indexName());

        newIdx.setIndexType(cmd.spatial() ? QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);

        LinkedHashMap<String, Boolean> flds = new LinkedHashMap<>();

        // Let's replace H2's table and property names by those operated by GridQueryProcessor.
        GridQueryTypeDescriptor typeDesc = tbl.rowDescriptor().type();

        for (SqlIndexColumn col : cmd.columns()) {
            GridQueryProperty prop = typeDesc.property(col.name());

            if (prop == null)
                throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_NOT_FOUND, col.name());

            flds.put(prop.name(), !col.descending());
        }

        newIdx.setFields(flds);
        newIdx.setInlineSize(cmd.inlineSize());

        fut = ctx.query().dynamicIndexCreate(tbl.cacheName(), cmd.schemaName(), typeDesc.tableName(),
            newIdx, cmd.ifNotExists(), cmd.parallel());

        return fut;
    }

    /**
     * Creates a future for {@link SqlDropIndexCommand}.
     *
     * @param cmd The command to wrap.
     * @return A future that wraps the command.
     * @throws SchemaOperationException If an index cannot be found.
     */
    private IgniteInternalFuture makeFutureForDropIndex(SqlDropIndexCommand cmd) throws SchemaOperationException {
        IgniteInternalFuture fut;

        GridH2Table tbl = idx.dataTableForIndex(cmd.schemaName(), cmd.indexName());

        if (tbl != null) {
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

        return fut;
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

                GridH2Table tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl == null)
                    throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, cmd.tableName());

                assert tbl.rowDescriptor() != null;

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

                GridH2Table tbl = idx.dataTableForIndex(cmd.schemaName(), cmd.indexName());

                if (tbl != null) {
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
                GridSqlCreateTable cmd = (GridSqlCreateTable)stmt0;

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
                GridSqlDropTable cmd = (GridSqlDropTable)stmt0;

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
                            col.column().isNullable());

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
            throw convert(e);
        }
        catch (IgniteSQLException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteSQLException("Unexpected DDL operation failure: " + e.getMessage(), e);
        }
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

        for (Map.Entry<String, GridSqlColumn> e : createTbl.columns().entrySet()) {
            GridSqlColumn gridCol = e.getValue();

            Column col = gridCol.column();

            res.addQueryField(e.getKey(), DataType.getTypeClassName(col.getType()), null);

            if (!col.isNullable()) {
                if (notNullFields == null)
                    notNullFields = new HashSet<>();

                notNullFields.add(e.getKey());
            }
        }

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
     * Convert this statement to query entity and do Ignite specific sanity checks on the way.
     * @return Query entity mimicking this SQL statement.
     */
    private static QueryEntity toQueryEntity(SqlCreateTableCommand cmd) {
        if (cmd.primaryKeyColumnNames().size() == 0)
            throw new IgniteSQLException("No primary key(s) defined for table " + cmd.tableName());

        QueryEntity res = new QueryEntity();

        res.setTableName(cmd.tableName());

        Set<String> notNullFields = null;

        for (SqlColumn col : cmd.columns().values()) {

            res.addQueryField(col.name(), getSupportedTypeName(col.type()), null);

            if (!col.isNullable()) {
                if (notNullFields == null)
                    notNullFields = new HashSet<>();

                notNullFields.add(col.name());
            }
        }

        String valTypeName = QueryUtils.createTableValueTypeName(cmd.schemaName(), cmd.tableName());
        String keyTypeName = QueryUtils.createTableKeyTypeName(valTypeName);

        if (!F.isEmpty(cmd.keyTypeName()))
            keyTypeName = cmd.keyTypeName();

        if (!F.isEmpty(cmd.valueTypeName()))
            valTypeName = cmd.valueTypeName();

        boolean wrapKey = cmd.wrapKey() != null ? cmd.wrapKey() : false;
        boolean wrapVal = cmd.wrapValue() != null ? cmd.wrapValue() : true;

        if (wrapKey)
            res.setKeyFields(cmd.primaryKeyColumnNames());
        else {
            SqlColumn pkCol = cmd.columns().get(cmd.primaryKeyColumnNames().iterator().next());

            keyTypeName = getSupportedTypeName(pkCol.type());

            res.setKeyFieldName(pkCol.name());
        }

        res.setKeyType(keyTypeName);

        if (!wrapVal) {
            SqlColumn valCol = null;

            for (Map.Entry<String, SqlColumn> e : cmd.columns().entrySet()) {

                if (!cmd.primaryKeyColumnNames().contains(e.getKey())) {

                    valCol = e.getValue();

                    break;
                }
            }

            assert valCol != null;

            valTypeName = getSupportedTypeName(valCol.type());

            res.setValueFieldName(valCol.name());
        }

        res.setValueType(valTypeName);

        if (!F.isEmpty(notNullFields)) {
            QueryEntityEx res0 = new QueryEntityEx(res);

            res0.setNotNullFields(notNullFields);

            res = res0;
        }

        return res;
    }

    /**
     * Returns Java class name for a supported {@link SqlColumnType}.
     *
     * @param colTyp The column type.
     * @return The Java type string.
     * @throws IgniteSQLException if this type has no Java representation.
     */
    private static @NotNull String getSupportedTypeName(@NotNull SqlColumnType colTyp) {
        Class<?> cls = SqlColumnType.classForType(colTyp);

        if (cls == null)
            throw new IgniteSQLException("Unsupported column type: " + colTyp);

        return cls.getName();
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
