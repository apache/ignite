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
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridDdlStatementsProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.DmlStatementsProcessor;
import org.apache.ignite.internal.processors.query.h2.sql.GridCreateIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridDropIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.resources.LoggerResource;
import org.h2.command.Prepared;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.DropIndex;
import org.h2.jdbc.JdbcPreparedStatement;

/**
 *
 */
public class DdlStatementsProcessor implements GridDdlStatementsProcessor {
    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Kernal context. */
    private GridKernalContext ctx;

    /** {@inheritDoc} */
    @Override public void start(GridKernalContext ctx) throws IgniteCheckedException {
        this.ctx = ctx;

        ctx.discovery().setCustomEventListener(DdlOperationInit.class, new CustomEventListener<DdlOperationInit>() {
            @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, DdlOperationInit msg) {

            }
        });
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> runDdlStatement(GridCacheContext<?, ?> cctx, PreparedStatement stmt)
        throws IgniteCheckedException {
        assert stmt instanceof JdbcPreparedStatement;

        GridSqlStatement gridStmt = new GridSqlQueryParser().parse(GridSqlQueryParser
            .prepared((JdbcPreparedStatement) stmt));

        if (gridStmt instanceof GridCreateIndex) {
            GridCreateIndex createIdx = (GridCreateIndex) gridStmt;

            CreateIndexArguments args = new CreateIndexArguments(createIdx.cacheName(), createIdx.index(),
                createIdx.ifNotExists());

            execute(DdlOperationType.CREATE_INDEX, args);

            return DmlStatementsProcessor.cursorForUpdateResult(0L);
        }
        else if (gridStmt instanceof GridDropIndex)
            throw new UnsupportedOperationException("DROP INDEX");
        else
            throw new IgniteSQLException("Unexpected DDL operation [type=" + gridStmt.getClass() + ']',
                IgniteQueryErrorCode.UNEXPECTED_OPERATION);
    }

    /**
     * Perform operation.
     *
     * @param opType Operation type.
     * @param args Operation arguments.
     * @throws IgniteCheckedException if failed.
     */
    private void execute(DdlOperationType opType, DdlOperationArguments args) throws IgniteCheckedException {
        DdlOperationInit initMsg = new DdlOperationInit();

        initMsg.setArguments(args);
        initMsg.setOperationType(opType);

        ctx.discovery().sendCustomEvent(initMsg);
    }

    /**
     * @param cmd Statement.
     * @return Whether {@code cmd} is a DDL statement.
     */
    public static boolean isDdlStatement(Prepared cmd) {
        return cmd instanceof CreateIndex || cmd instanceof DropIndex;
    }

}
