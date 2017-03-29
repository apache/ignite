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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.h2.command.Prepared;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.DropIndex;
import org.h2.jdbc.JdbcPreparedStatement;

import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.List;

import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.UPDATE_RESULT_META;

/**
 * DDL statements processor.<p>
 * Contains higher level logic to handle operations as a whole and communicate with the client.
 */
public class DdlStatementsProcessor {
    /** Kernal context. */
    GridKernalContext ctx;

    /** Logger. */
    private IgniteLogger log;

    /**
     * Initialize message handlers and this' fields needed for further operation.
     *
     * @param ctx Kernal context.
     * @param idx Indexing.
     */
    public void start(final GridKernalContext ctx, IgniteH2Indexing idx) {
        this.ctx = ctx;

        log = ctx.log(DdlStatementsProcessor.class);
    }

    /**
     * Execute DDL statement.
     *
     * @param cacheName Cache name.
     * @param stmt H2 statement to parse and execute.
     */
    @SuppressWarnings("unchecked")
    public QueryCursor<List<?>> runDdlStatement(String cacheName, PreparedStatement stmt)
        throws IgniteCheckedException {
        assert stmt instanceof JdbcPreparedStatement;

        IgniteInternalFuture fut;

        try {
            GridSqlStatement gridStmt = new GridSqlQueryParser(false).parse(GridSqlQueryParser.prepared(stmt));

            if (gridStmt instanceof GridSqlCreateIndex) {
                GridSqlCreateIndex createIdx = (GridSqlCreateIndex) gridStmt;

                // TODO: How to handle schema name properly?
                fut = ctx.query().dynamicIndexCreate(
                    cacheName, createIdx.tableName(), createIdx.index(), createIdx.ifNotExists());
            }
            else if (gridStmt instanceof GridSqlDropIndex) {
                // TODO: Implement.
                throw new UnsupportedOperationException("DROP INDEX");
            }
            else
                throw new IgniteSQLException("Unexpected DDL operation [type=" + gridStmt.getClass() + ']',
                    IgniteQueryErrorCode.UNEXPECTED_OPERATION);

            fut.get();

            QueryCursorImpl<List<?>> resCur = (QueryCursorImpl<List<?>>)new QueryCursorImpl(Collections.singletonList
                (Collections.singletonList(0L)), null, false);

            resCur.fieldsMeta(UPDATE_RESULT_META);

            return resCur;
        }
        catch (Exception e) {
            // TODO: Proper error handling.
            throw new IgniteSQLException("DLL operation failed.", e);
        }
    }

    /**
     * @param cmd Statement.
     * @return Whether {@code cmd} is a DDL statement we're able to handle.
     */
    public static boolean isDdlStatement(Prepared cmd) {
        return cmd instanceof CreateIndex || cmd instanceof DropIndex;
    }
}
