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
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridDdlStatementsProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.DmlStatementsProcessor;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationAck;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationCancel;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationInit;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationNodeResult;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationResult;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.util.typedef.internal.U;
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
    private Map<IgniteUuid, DdlOperationFuture> operations = new ConcurrentHashMap8<>();

    /** Arguments of operations for which this node is a server. Are stored at {@code INIT} stage. */
    private Map<IgniteUuid, DdlOperationArguments> operationArgs = new ConcurrentHashMap8<>();

    /** Running operations <b>coordinated by</b> this node. */
    private Map<IgniteUuid, DdlOperationRunContext> operationRuns = new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override public void start(final GridKernalContext ctx) {
        this.ctx = ctx;

        ctx.discovery().setCustomEventListener(DdlOperationInit.class, new CustomEventListener<DdlOperationInit>() {
            /** {@inheritDoc} */
            @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "unchecked"})
            @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, DdlOperationInit msg) {
                onInit(topVer, msg);
            }
        });

        ctx.discovery().setCustomEventListener(DdlOperationAck.class, new CustomEventListener<DdlOperationAck>() {
            /** {@inheritDoc} */
            @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, DdlOperationAck msg) {
                onAck(snd, msg);
            }
        });

        ctx.discovery().setCustomEventListener(DdlOperationCancel.class, new CustomEventListener<DdlOperationCancel>() {
            /** {@inheritDoc} */
            @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "unchecked"})
            @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, DdlOperationCancel msg) {
                onCancel(msg);
            }
        });

        ctx.io().addMessageListener(GridTopic.TOPIC_DDL, new GridMessageListener() {
            /** {@inheritDoc} */
            @Override public void onMessage(UUID nodeId, Object msg) {
                if (msg instanceof DdlOperationResult) {
                    DdlOperationResult res = (DdlOperationResult) msg;

                    onResult(res.getOperationId(), bytesToException(res.getError()));
                }

                if (msg instanceof DdlOperationNodeResult) {
                    DdlOperationNodeResult res = (DdlOperationNodeResult) msg;

                    onNodeResult(res.getOperationId(), bytesToException(res.getError()));
                }
            }
        });
    }

    /**
     * Process local event of cancel. <b>Coordinator</b> and <b>client</b> take additional actions for cleanup.
     * Ring sent cancellation message will be processed once at coordinator to notify initial client of the whole operation
     *
     * @param msg Message.
     */
    @SuppressWarnings("unchecked")
    private void onCancel(DdlOperationCancel msg) {
        DdlOperationArguments args = operationArgs.remove(msg.getOperationId());

        // Node does not know anything about this operation, nothing to do.
        if (args == null)
            return;

        try {
            args.opType.command().cancel(args);
        }
        catch (Throwable e) {
            log.warning("Failed to process DDL CANCEL locally [opId=" + msg.getOperationId() +']', e);
        }

        DdlOperationRunContext runCtx = operationRuns.remove(msg.getOperationId());

        if (runCtx != null) { // We're on the coordinator, let's notify the client about cancellation and its reason
            IgniteCheckedException ex = msg.getError();

            if (ex == null)
                ex = new IgniteCheckedException("DDL operation has been cancelled (presumably by the user).");

            sendResult(args, ex);
        }
    }

    /**
     * Handle {@code ACK} message on a <b>peer node</b> - do local portion of actual DDL job and notify
     * <b>coordinator</b> about success or failure.
     *
     * @param snd Sender.
     * @param msg Message.
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown", "unchecked"})
    private void onAck(ClusterNode snd, DdlOperationAck msg) {
        IgniteCheckedException ex = null;

        DdlOperationArguments args = operationArgs.get(msg.getOperationId());

        if (args == null) {
            ex = new IgniteCheckedException("DDL operation not found by its id at its peer node [opId=" +
                 "nodeId=" + ctx.localNodeId() + ']');
        }
        else {
            try {
                args.opType.command().execute(args);
            }
            catch (IgniteCheckedException e) {
                ex = e;
            }
            catch (Throwable e) {
                ex = new IgniteCheckedException(e);
            }
        }

        try {
            DdlOperationNodeResult res = new DdlOperationNodeResult();

            res.setOperationId(msg.getOperationId());
            res.setError(exceptionToBytes(ex));

            ctx.io().send(snd, GridTopic.TOPIC_DDL, res, GridIoPolicy.IDX_POOL);
        }
        catch (Throwable e) {
            log.error("Failed to notify coordinator about local DLL operation completion [opId=" + msg.getOperationId() +
                ", sndNodeId=" + snd.id() + ']', e);
        }
    }

    /**
     * Handle local DDL operation result from <b>a peer node</b> on <b>the coordinator</b>.
     *
     * @param opId DDL operation ID.
     * @param err Exception that occurred on the <b>peer</b>, or null if the local operation has been successful.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void onNodeResult(IgniteUuid opId, IgniteCheckedException err) {
        DdlOperationArguments args = operationArgs.get(opId);

        DdlOperationRunContext runCtx = operationRuns.get(opId);

        // First error fails whole operation.
        if (args == null || runCtx == null || err != null) {
            cancel(opId, err);

            return;
        }

        // Return if we expect more messages like this
        if (runCtx.nodesCnt.decrementAndGet() > 0)
            return;

        sendResult(args, null);
    }

    /**
     * Initiate cancel of a DDL operation due to an irrecoverable error on the peer or on the coordinator,
     * or due to the operation's cancellation by the user.
     * May be called on the <b>client</b> or on the <b>coordinator</b>.
     *
     * @param opId Operation id.
     * @param err Error that has led to cancel, if any.
     */
    public void cancel(IgniteUuid opId, IgniteCheckedException err) {
        DdlOperationCancel cancel = new DdlOperationCancel();

        cancel.setOperationId(opId);
        cancel.setError(err);

        try {
            ctx.discovery().sendCustomEvent(cancel);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send CANCEL message over the ring [opId=" + opId + ']', e);
        }
    }

    /**
     * Notify client about result.
     *
     * @param args Operation arguments.
     * @param err Error, if any.
     */
    private void sendResult(DdlOperationArguments args, IgniteCheckedException err) {
        assert args != null;

        DdlOperationResult res = new DdlOperationResult();

        res.setError(exceptionToBytes(err));
        res.setOperationId(args.opId);

        try {
            ctx.io().send(args.sndNodeId, GridTopic.TOPIC_DDL, res, GridIoPolicy.IDX_POOL);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to notify client node about DDL operation failure " +
                "[opId=" + args.opId + ", sndNodeId=" + args.sndNodeId + ']', e);
        }
    }

    /**
     * Callback handling whole DDL operation result <b>on the client</b>.
     *
     * @param opId DDL operation ID.
     * @param err Error, if any.
     */
    @SuppressWarnings("unchecked")
    private void onResult(IgniteUuid opId, IgniteCheckedException err) {
        DdlOperationFuture fut = operations.get(opId);

        if (fut == null) {
            log.warning("DDL operation not found at its client [opId=" + opId + ", nodeId=" + ctx.localNodeId() + ']');

            return;
        }

        fut.onDone(null, err);
    }

    /**
     * Perform preliminary actions and checks for {@code INIT} stage of DDL statement execution <b>on a peer node</b>.
     *
     * @param topVer topology version.
     * @param msg {@code INIT} message.
     */
    @SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
    private void onInit(AffinityTopologyVersion topVer, DdlOperationInit msg) {
        DdlOperationArguments args = msg.getArguments();

        if (msg.getNodesState() == null) {
            // Null state means we're at coordinator, so let's populate state with participating nodes
            // in accordance with topology version.
            // We take cache nodes and not just affinity nodes as long as we must create index on client
            // nodes as well to build query plans correctly.
            Collection<ClusterNode> nodes = ctx.discovery().cacheNodes(args.cacheName,
                topVer);

            Map<UUID, IgniteCheckedException> newNodesState = new HashMap<>();

            for (ClusterNode node : nodes)
                newNodesState.put(node.id(), null);

            msg.setNodesState(newNodesState);

            operationRuns.put(args.opId, new DdlOperationRunContext(newNodesState.size()));
        }
        else if (!msg.getNodesState().containsKey(ctx.localNodeId()))
            return;

        try {
            args.opType.command().init(args);

            operationArgs.put(args.opId, args);
        }
        catch (Throwable e) {
            msg.getNodesState().put(ctx.localNodeId(), new IgniteCheckedException(e));
        }
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

        if (gridStmt instanceof GridSqlCreateIndex) {
            GridSqlCreateIndex createIdx = (GridSqlCreateIndex) gridStmt;

            CreateIndexArguments args = new CreateIndexArguments(ctx.localNodeId(), opId, createIdx.cacheName(),
                createIdx.index(), createIdx.ifNotExists());

            execute(args);
        }
        else if (gridStmt instanceof GridSqlDropIndex)
            throw new UnsupportedOperationException("DROP INDEX");
        else
            throw new IgniteSQLException("Unexpected DDL operation [type=" + gridStmt.getClass() + ']',
                IgniteQueryErrorCode.UNEXPECTED_OPERATION);

        return DmlStatementsProcessor.cursorForUpdateResult(0L);
    }

    /**
     * Start whole DDL operation and wait for its result.
     *
     * @param args Operation arguments.
     * @throws IgniteCheckedException if failed.
     */
    private void execute(DdlOperationArguments args) throws IgniteCheckedException {
        DdlOperationFuture op = new DdlOperationFuture(ctx, args);

        operations.put(args.opId, op);

        try {
            op.init();

            op.get();
        }
        finally {
            operations.remove(args.opId);
        }
    }

    /**
     * Serialize exception or at least its message to bytes.
     *
     * @param ex Exception.
     * @return Serialized exception.
     */
    private byte[] exceptionToBytes(IgniteCheckedException ex) {
        if (ex == null)
            return null;

        try {
            return U.marshal(ctx, ex);
        }
        catch (IgniteCheckedException e) {
            IgniteCheckedException resEx;

            // Let's try to serialize at least the message
            try {
                resEx = new IgniteCheckedException("Failed to serialize exception " +
                    "[msg=" + ex.getMessage() + ']');
            }
            catch (Throwable ignored) {
                resEx = new IgniteCheckedException("Failed to serialize exception");
            }

            try {
                return U.marshal(ctx, resEx);
            }
            catch (IgniteCheckedException exx) {
                // Why would it fail? We've sanitized it...
                throw new AssertionError(exx);
            }
        }
    }

    /**
     * Deserialize exception from bytes.
     *
     * @param ex Exception.
     * @return Serialized exception.
     */
    private IgniteCheckedException bytesToException(byte[] ex) {
        if (ex == null)
            return null;

        try {
            return U.unmarshal(ctx, ex, ctx.config().getClassLoader());
        }
        catch (Throwable e) {
            return new IgniteCheckedException("Failed to deserialize exception", e);
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
