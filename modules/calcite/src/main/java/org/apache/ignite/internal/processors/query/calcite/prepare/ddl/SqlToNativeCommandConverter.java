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

package org.apache.ignite.internal.processors.query.calcite.prepare.ddl;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlAlterTable;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlAlterUser;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateIndex;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateUser;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlDropIndex;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlDropUser;
import org.apache.ignite.internal.sql.command.SqlAlterTableCommand;
import org.apache.ignite.internal.sql.command.SqlAlterUserCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlCreateUserCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropUserCommand;
import org.apache.ignite.internal.sql.command.SqlIndexColumn;

import static org.apache.ignite.internal.processors.query.calcite.util.PlanUtils.deriveObjectName;
import static org.apache.ignite.internal.processors.query.calcite.util.PlanUtils.deriveSchemaName;

/** */
public class SqlToNativeCommandConverter {
    /**
     * Is a given AST can be converted by this class.
     *
     * @param sqlCmd Root node of the given AST.
     */
    public static boolean isSupported(SqlNode sqlCmd) {
        return sqlCmd instanceof IgniteSqlCreateIndex
            || sqlCmd instanceof IgniteSqlDropIndex
            || sqlCmd instanceof IgniteSqlAlterTable
            || sqlCmd instanceof IgniteSqlCreateUser
            || sqlCmd instanceof IgniteSqlAlterUser
            || sqlCmd instanceof IgniteSqlDropUser;
    }

    /**
     * Converts a given AST to a native command.
     *
     * @param sqlCmd Root node of the given AST.
     * @param pctx Planning context.
     */
    public static NativeCommandWrapper convert(SqlDdl sqlCmd, PlanningContext pctx) {
        return new NativeCommandWrapper(convertSqlCmd(sqlCmd, pctx));
    }

    /**
     * Converts SqlNode to SqlCommand.
     */
    private static SqlCommand convertSqlCmd(SqlDdl cmd, PlanningContext pctx) {
        if (cmd instanceof IgniteSqlCreateIndex)
            return convertCreateIndex((IgniteSqlCreateIndex)cmd, pctx);
        else if (cmd instanceof IgniteSqlDropIndex)
            return convertDropIndex((IgniteSqlDropIndex)cmd, pctx);
        else if (cmd instanceof IgniteSqlAlterTable)
            return convertAlterTable((IgniteSqlAlterTable)cmd, pctx);
        else if (cmd instanceof IgniteSqlCreateUser)
            return convertCreateUser((IgniteSqlCreateUser)cmd, pctx);
        else if (cmd instanceof IgniteSqlAlterUser)
            return convertAlterUser((IgniteSqlAlterUser)cmd, pctx);
        else if (cmd instanceof IgniteSqlDropUser)
            return convertDropUser((IgniteSqlDropUser)cmd, pctx);

        throw new IgniteSQLException("Unsupported native operation [" +
            "cmdName=" + (cmd == null ? null : cmd.getClass().getSimpleName()) + "; " +
            "querySql=\"" + pctx.query() + "\"]", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Converts CREATE INDEX command.
     */
    private static SqlCreateIndexCommand convertCreateIndex(IgniteSqlCreateIndex sqlCmd, PlanningContext ctx) {
        String schemaName = deriveSchemaName(sqlCmd.tableName(), ctx);
        String tblName = deriveObjectName(sqlCmd.tableName(), ctx, "table name");
        String idxName = sqlCmd.indexName().getSimple();

        List<SqlIndexColumn> cols = new ArrayList<>(sqlCmd.columnList().size());

        for (SqlNode col : sqlCmd.columnList().getList()) {
            boolean desc = false;

            if (col.getKind() == SqlKind.DESCENDING) {
                col = ((SqlCall)col).getOperandList().get(0);

                desc = true;
            }

            cols.add(new SqlIndexColumn(((SqlIdentifier)col).getSimple(), desc));
        }

        int parallel = sqlCmd.parallel() == null ? 0 : sqlCmd.parallel().intValue(true);

        int inlineSize = sqlCmd.inlineSize() == null ? QueryIndex.DFLT_INLINE_SIZE :
            sqlCmd.inlineSize().intValue(true);

        return new SqlCreateIndexCommand(schemaName, tblName, idxName, sqlCmd.ifNotExists(), cols, false,
            parallel, inlineSize);
    }

    /**
     * Converts DROP INDEX command.
     */
    private static SqlDropIndexCommand convertDropIndex(IgniteSqlDropIndex sqlCmd, PlanningContext ctx) {
        String schemaName = deriveSchemaName(sqlCmd.name(), ctx);
        String idxName = deriveObjectName(sqlCmd.name(), ctx, "index name");

        return new SqlDropIndexCommand(schemaName, idxName, sqlCmd.ifExists());
    }

    /**
     * Converts ALTER TABLE ... LOGGING/NOLOGGING command.
     */
    private static SqlAlterTableCommand convertAlterTable(IgniteSqlAlterTable sqlCmd, PlanningContext ctx) {
        String schemaName = deriveSchemaName(sqlCmd.name(), ctx);
        String tblName = deriveObjectName(sqlCmd.name(), ctx, "table name");

        return new SqlAlterTableCommand(schemaName, tblName, sqlCmd.ifExists(), sqlCmd.logging());
    }

    /**
     * Converts CREATE USER ... command.
     */
    private static SqlCreateUserCommand convertCreateUser(IgniteSqlCreateUser sqlCmd, PlanningContext ctx) {
        return new SqlCreateUserCommand(sqlCmd.user().getSimple(), sqlCmd.password());
    }

    /**
     * Converts ALTER USER ... command.
     */
    private static SqlAlterUserCommand convertAlterUser(IgniteSqlAlterUser sqlCmd, PlanningContext ctx) {
        return new SqlAlterUserCommand(sqlCmd.user().getSimple(), sqlCmd.password());
    }

    /**
     * Converts DROP USER ... command.
     */
    private static SqlDropUserCommand convertDropUser(IgniteSqlDropUser sqlCmd, PlanningContext ctx) {
        return new SqlDropUserCommand(sqlCmd.user().getSimple());
    }
}
