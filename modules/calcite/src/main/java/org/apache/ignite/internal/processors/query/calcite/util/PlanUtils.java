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

package org.apache.ignite.internal.processors.query.calcite.util;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;

/** */
public class PlanUtils {
    /** Derives a schema name from the compound identifier. */
    public static String deriveSchemaName(SqlIdentifier id, PlanningContext ctx) {
        String schemaName;
        if (id.isSimple())
            schemaName = ctx.schemaName();
        else {
            SqlIdentifier schemaId = id.skipLast(1);

            if (!schemaId.isSimple()) {
                throw new IgniteSQLException("Unexpected value of schemaName [" +
                    "expected a simple identifier, but was " + schemaId + "; " +
                    "querySql=\"" + ctx.query() + "\"]", IgniteQueryErrorCode.PARSING);
            }

            schemaName = schemaId.getSimple();
        }

        ensureSchemaExists(ctx.unwrap(BaseQueryContext.class), schemaName);

        return schemaName;
    }

    /** Derives an object(a table, an index, etc) name from the compound identifier. */
    public static String deriveObjectName(SqlIdentifier id, PlanningContext ctx, String objDesc) {
        if (id.isSimple())
            return id.getSimple();

        SqlIdentifier objId = id.getComponent(id.skipLast(1).names.size());

        if (!objId.isSimple()) {
            throw new IgniteSQLException("Unexpected value of " + objDesc + " [" +
                "expected a simple identifier, but was " + objId + "; " +
                "querySql=\"" + ctx.query() + "\"]", IgniteQueryErrorCode.PARSING);
        }

        return objId.getSimple();
    }

    /** */
    private static void ensureSchemaExists(BaseQueryContext ctx, String schemaName) {
        if (ctx.catalogReader().getRootSchema().getSubSchema(schemaName, true) == null)
            throw new IgniteSQLException("Schema with name " + schemaName + " not found",
                IgniteQueryErrorCode.SCHEMA_NOT_FOUND);
    }
}
