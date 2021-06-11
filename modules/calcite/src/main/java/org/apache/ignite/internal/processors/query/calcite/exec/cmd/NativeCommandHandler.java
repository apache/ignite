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

package org.apache.ignite.internal.processors.query.calcite.exec.cmd;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQuerySchemaManager;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateIndex;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlDropIndex;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlNativeCommand;
import org.apache.ignite.internal.sql.SqlCommandProcessor;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;

import static org.apache.ignite.internal.processors.query.calcite.util.PlanUtils.deriveObjectName;
import static org.apache.ignite.internal.processors.query.calcite.util.PlanUtils.deriveSchemaName;

/**
 * Handler for Ignite native (core module) commands.
 */
public class NativeCommandHandler {
    /** Context. */
    private final GridKernalContext ctx;

    /** Command processor. */
    private final CommandProcessor proc;

    /**
     * @param ctx Context.
     */
    public NativeCommandHandler(GridKernalContext ctx, SchemaHolder schemaHolder) {
        this.ctx = ctx;
        proc = new CommandProcessor(ctx, new SchemaManager(schemaHolder));
    }

    /**
     * @param qryId Query id.
     * @param cmd   Native command.
     * @param pctx  Planning context.
     */
    public FieldsQueryCursor<List<?>> handle(UUID qryId, NativeCommand cmd, PlanningContext pctx) {
        assert proc.isCommandSupported(cmd.command()) : cmd.command();

        FieldsQueryCursor<List<?>> res = proc.runCommand(pctx.query(), cmd.command(), pctx.unwrap(SqlClientContext.class));

        if (res == null) {
            // TODO
        }

        return res;
    }

    /**
     * Converts SqlNode to native command.
     */
    public NativeCommand convert(IgniteSqlNativeCommand sqlCmd, PlanningContext pctx) {
        return new NativeCommand(convertSqlCmd(sqlCmd, pctx));
    }

    /**
     * Converts SqlNode to SqlCommand.
     */
    private static SqlCommand convertSqlCmd(IgniteSqlNativeCommand cmd, PlanningContext pctx) {
        if (cmd instanceof IgniteSqlCreateIndex)
            return convertCreateIndex((IgniteSqlCreateIndex)cmd, pctx);
        else if (cmd instanceof IgniteSqlDropIndex)
            return convertDropIndex((IgniteSqlDropIndex)cmd, pctx);

        throw new IgniteSQLException("Unsupported native operation [" +
            "cmdName=" + (cmd == null ? null : cmd.getClass().getSimpleName()) + "; " +
            "querySql=\"" + pctx.query() + "\"]", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Converts CREATE INDEX command.
     */
    private static SqlCreateIndexCommand convertCreateIndex(IgniteSqlCreateIndex sqlCmd, PlanningContext ctx) {
/*
        CreateIndexCommand createIdxCmd = new CreateIndexCommand();

        createIdxCmd.schemaName(deriveSchemaName(createIdxNode.tableName(), ctx));
        createIdxCmd.tableName(deriveObjectName(createIdxNode.tableName(), ctx, "tableName"));
        createIdxCmd.indexName(createIdxNode.indexName().getSimple());
        createIdxCmd.ifNotExists(createIdxNode.ifNotExists());

        List<Boolean> asc = new ArrayList<>(createIdxNode.columnList().size());
        List<String> cols = new ArrayList<>(createIdxNode.columnList().size());

        for (SqlNode col : createIdxNode.columnList().getList()) {
            if (col.getKind() == SqlKind.DESCENDING) {
                col = ((SqlCall)col).getOperandList().get(0);

                asc.add(Boolean.FALSE);
            }
            else
                asc.add(Boolean.TRUE);

            cols.add(((SqlIdentifier)col).getSimple());
        }

        createIdxCmd.columns();

        //IgnitePlanner planner = ctx.planner();

        return createIdxCmd;
*/

        SqlCreateIndexCommand cmd = new SqlCreateIndexCommand();
        // TODO

        return cmd;
    }

    /**
     * Converts DROP INDEX command.
     */
    private static SqlDropIndexCommand convertDropIndex(IgniteSqlDropIndex sqlCmd, PlanningContext ctx) {
        String schemaName = deriveSchemaName(sqlCmd.indexName(), ctx);
        String idxName = deriveObjectName(sqlCmd.indexName(), ctx, "index name");

        return new SqlDropIndexCommand(schemaName, idxName, sqlCmd.ifExists());
    }

    /**
     * Schema manager.
     */
    private static class SchemaManager implements GridQuerySchemaManager {
        /** Schema holder. */
        private final SchemaHolder schemaHolder;

        /**
         * @param holder Schema holder.
         */
        private SchemaManager(SchemaHolder holder) {
            schemaHolder = holder;
        }

        /** {@inheritDoc} */
        @Override public GridQueryTypeDescriptor typeDescriptorForTable(String schemaName, String tableName) {
            IgniteTable tbl = (IgniteTable)schemaHolder.schema().getSubSchema(schemaName).getTable(tableName);
            // TODO not exists

            return tbl.descriptor().typeDescription();
        }

        /** {@inheritDoc} */
        @Override public GridQueryTypeDescriptor typeDescriptorForIndex(String schemaName, String idxName) {
            schemaHolder.schema().getSubSchema(schemaName).getTableNames();

            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> GridCacheContextInfo<K, V> cacheInfoForTable(String schemaName, String tableName) {
            IgniteTable tbl = (IgniteTable)schemaHolder.schema().getSubSchema(schemaName).getTable(tableName);
            // TODO not exists

            return (GridCacheContextInfo<K, V>)tbl.descriptor().cacheInfo();
        }
    }

    /**
     * Processor for executing Ignite native (core module) commands.
     */
    private static class CommandProcessor extends SqlCommandProcessor {
        /**
         * @param ctx       Context.
         * @param schemaMgr Schema manager.
         */
        public CommandProcessor(GridKernalContext ctx,
            GridQuerySchemaManager schemaMgr) {
            super(ctx, schemaMgr);
        }
    }
}
