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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.LoggerResource;
import org.h2.command.Prepared;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.DropIndex;
import org.h2.jdbc.JdbcPreparedStatement;
import org.jsr166.ConcurrentHashMap8;

/**
 *
 */
public class DdlStatementsProcessor implements GridDdlStatementsProcessor {
    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Kernal context. */
    private GridKernalContext ctx;

    /** Running operations originating at this node as a client. */
    private Map<IgniteUuid, DdlOperation> operations = new ConcurrentHashMap8<>();

    /** Arguments of operations for which this node is a server. Are stored at {@code INIT} stage. */
    private Map<IgniteUuid, DdlOperationArguments> serverArgs = new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override public void start(final GridKernalContext ctx) throws IgniteCheckedException {
        this.ctx = ctx;

        ctx.discovery().setCustomEventListener(DdlOperationInit.class, new CustomEventListener<DdlOperationInit>() {
            /** {@inheritDoc} */
            @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
            @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, DdlOperationInit msg) {
                if (msg.getNodesState() == null) {
                    // Null state means we're at coordinator, so let's populate state with participating nodes
                    // in accordance with topology version.
                    Collection<ClusterNode> nodes = ctx.discovery().cacheNodes(msg.getArguments().cacheName, topVer);

                    Map<UUID, IgniteCheckedException> newNodesState = new HashMap<>();

                    for (ClusterNode node : nodes)
                        newNodesState.put(node.id(), null);

                    msg.setNodesState(newNodesState);
                }
                else if (!msg.getNodesState().containsKey(ctx.localNodeId()))
                    return;

                try {
                    handleInit(msg.getArguments());
                }
                catch (Exception e) {
                    msg.getNodesState().put(ctx.localNodeId(), new IgniteCheckedException(e));
                }
            }
        });
    }

    /**
     * Perform preliminary actions and checks for {@code INIT} stage of DDL statement execution.
     * @param args Operation arguments.
     */
    private void handleInit(DdlOperationArguments args) {
        switch (args.opType) {
            case CREATE_INDEX:
                handleInitCreateIndex((CreateIndexArguments) args);
                break;
            case DROP_INDEX:
                throw new UnsupportedOperationException(args.opType.name());
        }

        serverArgs.put(args.opId, args);
    }

    /**
     * Perform preliminary actions for CREATE INDEX.
     * @param args {@code CREATE INDEX} arguments.
     */
    private void handleInitCreateIndex(CreateIndexArguments args) {
        // No-op.
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

        IgniteUuid opId = IgniteUuid.randomUuid();

        if (gridStmt instanceof GridCreateIndex) {
            GridCreateIndex createIdx = (GridCreateIndex) gridStmt;

            CreateIndexArguments args = new CreateIndexArguments(opId, createIdx.cacheName(), createIdx.index(),
                createIdx.ifNotExists());

            execute(args);
        }
        else if (gridStmt instanceof GridDropIndex)
            throw new UnsupportedOperationException("DROP INDEX");
        else
            throw new IgniteSQLException("Unexpected DDL operation [type=" + gridStmt.getClass() + ']',
                IgniteQueryErrorCode.UNEXPECTED_OPERATION);

        return DmlStatementsProcessor.cursorForUpdateResult(0L);
    }

    /**
     * Perform operation.
     *
     * @param args Operation arguments.
     * @throws IgniteCheckedException if failed.
     */
    private void execute(DdlOperationArguments args) throws IgniteCheckedException {
        IgniteUuid opId = IgniteUuid.randomUuid();

        DdlOperation op = new DdlOperation(opId, ctx, args);

        operations.put(opId, op);

        op.init();

        op.get();
    }

    /**
     * @param cmd Statement.
     * @return Whether {@code cmd} is a DDL statement we're able to handle.
     */
    public static boolean isDdlStatement(Prepared cmd) {
        return cmd instanceof CreateIndex || cmd instanceof DropIndex;
    }
}
