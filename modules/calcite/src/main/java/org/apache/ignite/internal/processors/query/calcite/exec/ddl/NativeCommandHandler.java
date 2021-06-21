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

package org.apache.ignite.internal.processors.query.calcite.exec.ddl;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQuerySchemaManager;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.NativeCommandWrapper;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.sql.SqlCommandProcessor;

/**
 * Handler for Ignite native (core module) commands.
 */
public class NativeCommandHandler {
    /** Command processor. */
    private final SqlCommandProcessor proc;

    /**
     * @param ctx Context.
     */
    public NativeCommandHandler(GridKernalContext ctx, Supplier<SchemaPlus> schemaSupp) {
        proc = new SqlCommandProcessor(ctx, new SchemaManager(schemaSupp));
    }

    /**
     * @param qryId Query id.
     * @param cmd   Native command.
     * @param pctx  Planning context.
     */
    public FieldsQueryCursor<List<?>> handle(UUID qryId, NativeCommandWrapper cmd, PlanningContext pctx) {
        assert proc.isCommandSupported(cmd.command()) : cmd.command();

        return proc.runCommand(pctx.query(), cmd.command(), pctx.unwrap(SqlClientContext.class));
    }

    /**
     * Schema manager.
     */
    private static class SchemaManager implements GridQuerySchemaManager {
        /** Schema holder. */
        private final Supplier<SchemaPlus> schemaSupp;

        /**
         * @param schemaSupp Schema supplier.
         */
        private SchemaManager(Supplier<SchemaPlus> schemaSupp) {
            this.schemaSupp = schemaSupp;
        }

        /** {@inheritDoc} */
        @Override public GridQueryTypeDescriptor typeDescriptorForTable(String schemaName, String tableName) {
            SchemaPlus schema = schemaSupp.get().getSubSchema(schemaName);

            if (schema == null)
                return null;

            IgniteTable tbl = (IgniteTable)schema.getTable(tableName);

            return tbl == null ? null : tbl.descriptor().typeDescription();
        }

        /** {@inheritDoc} */
        @Override public GridQueryTypeDescriptor typeDescriptorForIndex(String schemaName, String idxName) {
            SchemaPlus schema = schemaSupp.get().getSubSchema(schemaName);

            if (schema == null)
                return null;

            for (String tableName : schema.getTableNames()) {
                Table tbl = schema.getTable(tableName);

                if (tbl instanceof IgniteTable && ((IgniteTable)tbl).getIndex(idxName) != null)
                    return ((IgniteTable)tbl).descriptor().typeDescription();
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> GridCacheContextInfo<K, V> cacheInfoForTable(String schemaName, String tableName) {
            SchemaPlus schema = schemaSupp.get().getSubSchema(schemaName);

            if (schema == null)
                return null;

            IgniteTable tbl = (IgniteTable)schema.getTable(tableName);

            return tbl == null ? null : (GridCacheContextInfo<K, V>)tbl.descriptor().cacheInfo();
        }
    }
}
