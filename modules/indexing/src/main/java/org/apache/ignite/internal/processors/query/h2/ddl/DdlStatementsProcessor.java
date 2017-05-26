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

import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.h2.command.Prepared;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.CreateTable;
import org.h2.command.ddl.DropIndex;
import org.h2.command.ddl.DropTable;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.table.Column;
import org.h2.value.DataType;

import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.UPDATE_RESULT_META;

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
     * Execute DDL statement.
     *
     * @param sql SQL.
     * @param stmt H2 statement to parse and execute.
     */
    @SuppressWarnings("unchecked")
    public FieldsQueryCursor<List<?>> runDdlStatement(String sql, PreparedStatement stmt)
        throws IgniteCheckedException {
        assert stmt instanceof JdbcPreparedStatement;

        IgniteInternalFuture fut;

        try {
            GridSqlStatement gridStmt = new GridSqlQueryParser(false).parse(GridSqlQueryParser.prepared(stmt));

            if (gridStmt instanceof GridSqlCreateIndex) {
                GridSqlCreateIndex createIdx = (GridSqlCreateIndex)gridStmt;

                QueryIndex newIdx = new QueryIndex();

                newIdx.setName(createIdx.index().getName());

                newIdx.setIndexType(createIdx.index().getIndexType());

                LinkedHashMap<String, Boolean> flds = new LinkedHashMap<>();

                GridH2Table tbl = idx.dataTable(createIdx.schemaName(), createIdx.tableName());

                if (tbl == null)
                    throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND,
                        createIdx.tableName());

                assert tbl.rowDescriptor() != null;

                // Let's replace H2's table and property names by those operated by GridQueryProcessor.
                GridQueryTypeDescriptor typeDesc = tbl.rowDescriptor().type();

                for (Map.Entry<String, Boolean> e : createIdx.index().getFields().entrySet()) {
                    GridQueryProperty prop = typeDesc.property(e.getKey());

                    if (prop == null)
                        throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_NOT_FOUND, e.getKey());

                    flds.put(prop.name(), e.getValue());
                }

                newIdx.setFields(flds);

                fut = ctx.query().dynamicIndexCreate(tbl.cacheName(), createIdx.schemaName(), typeDesc.tableName(),
                    newIdx, createIdx.ifNotExists());
            }
            else if (gridStmt instanceof GridSqlDropIndex) {
                GridSqlDropIndex dropIdx = (GridSqlDropIndex)gridStmt;

                GridH2Table tbl = idx.dataTableForIndex(dropIdx.schemaName(), dropIdx.indexName());

                if (tbl != null)
                    fut = ctx.query().dynamicIndexDrop(tbl.cacheName(), dropIdx.schemaName(), dropIdx.indexName(),
                        dropIdx.ifExists());
                else {
                    if (dropIdx.ifExists())
                        fut = new GridFinishedFuture();
                    else
                        throw new SchemaOperationException(SchemaOperationException.CODE_INDEX_NOT_FOUND,
                            dropIdx.indexName());
                }
            }
            else if (gridStmt instanceof GridSqlCreateTable) {
                GridSqlCreateTable createTbl = (GridSqlCreateTable)gridStmt;

                ctx.query().dynamicTableCreate(createTbl.schemaName(), toQueryEntity(createTbl),
                    createTbl.templateCacheName(), createTbl.ifNotExists());

                fut = null;
            }
            else if (gridStmt instanceof GridSqlDropTable) {
                GridSqlDropTable dropTbl = (GridSqlDropTable)gridStmt;

                ctx.query().dynamicTableDrop(dropTbl.schemaName(), dropTbl.tableName(), dropTbl.ifExists());

                fut = null;
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
            throw new IgniteSQLException("Unexpected DLL operation failure: " + e.getMessage(), e);
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

        for (Map.Entry<String, GridSqlColumn> e : createTbl.columns().entrySet()) {
            GridSqlColumn gridCol = e.getValue();

            Column col = gridCol.column();

            res.addQueryField(e.getKey(), DataType.getTypeClassName(col.getType()), null);
        }

        res.setKeyType(createTbl.tableName() + "Key");

        res.setValueType(createTbl.tableName());

        res.setKeyFields(createTbl.primaryKeyColumns());

        return res;
    }

    /**
     * @param cmd Statement.
     * @return Whether {@code cmd} is a DDL statement we're able to handle.
     */
    public static boolean isDdlStatement(Prepared cmd) {
        return cmd instanceof CreateIndex || cmd instanceof DropIndex || cmd instanceof CreateTable ||
            cmd instanceof DropTable;
    }
}
