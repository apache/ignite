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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.DmlStatementsProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
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
import org.h2.command.Prepared;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.DropIndex;
import org.h2.jdbc.JdbcPreparedStatement;
import org.jsr166.ConcurrentHashMap8;

/**
 * DDL statements processor.<p>
 * Contains higher level logic to handle operations as a whole and communicate with the client.
 */
public class DdlStatementsProcessor {
    /** Kernal context. */
    GridKernalContext ctx;

    /** Indexing engine. */
    private IgniteH2Indexing idx;

    private volatile boolean isStopped;

    /** Running operations originating at this node as a client. */
    private Map<IgniteUuid, DdlOperationFuture> operations = new ConcurrentHashMap8<>();

    /** Arguments of operations for which this node is a server. Are stored at {@code INIT} stage. */
    private Map<IgniteUuid, DdlCommandArguments> operationArgs = new ConcurrentHashMap8<>();

    /** Running operations <b>coordinated by</b> this node. */
    private Map<IgniteUuid, DdlOperationRunContext> operationRuns = new ConcurrentHashMap8<>();

    /**
     * Initialize message handlers and this' fields needed for further operation.
     *
     * @param ctx Kernal context.
     * @param idx Indexing.
     */
    public void start(final GridKernalContext ctx, IgniteH2Indexing idx) {
        this.ctx = ctx;
        this.idx = idx;

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
     * Process local event of cancel. <b>Coordinator</b> takes additional actions for cleanup and client notification.
     *
     * @param msg Message.
     */
    @SuppressWarnings("unchecked")
    private void onCancel(DdlOperationCancel msg) {
        DdlCommandArguments args = operationArgs.remove(msg.getOperationId());

        // Node does not know anything about this operation, nothing to do.
        if (args == null)
            return;

        try {
            args.getOperationArguments().opType.command().cancel(args);
        }
        catch (Throwable e) {
            idx.getLogger().warning("Failed to process DDL CANCEL locally [opId=" +
                args.getOperationArguments().opId +']', e);
        }

        DdlOperationRunContext runCtx = operationRuns.remove(msg.getOperationId());

        if (runCtx != null) { // We're on the coordinator, let's notify the client about cancellation and its reason
            IgniteCheckedException ex = new IgniteCheckedException("DDL operation has been cancelled at INIT stage",
                msg.getError());

            sendResult(args.getOperationArguments(), ex);
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

        DdlCommandArguments args = operationArgs.get(msg.getOperationId());

        if (args == null) {
            ex = new IgniteCheckedException("DDL operation not found by its id at its peer node [opId=" +
                 "nodeId=" + ctx.localNodeId() + ']');
        }
        else {
            try {
                doAck(args);
            }
            catch (Throwable e) {
                ex = wrapThrowableIfNeeded(e);
            }
        }

        try {
            DdlOperationNodeResult res = new DdlOperationNodeResult();

            res.setOperationId(msg.getOperationId());
            res.setError(exceptionToBytes(ex));

            ctx.io().send(snd, GridTopic.TOPIC_DDL, res, GridIoPolicy.IDX_POOL);
        }
        catch (Throwable e) {
            idx.getLogger().error("Failed to notify coordinator about local DLL operation completion [opId=" +
                msg.getOperationId() + ", sndNodeId=" + snd.id() + ']', e);
        }
    }

    /**
     * Perform local portion of DDL operation.
     * Exists as a separate method to allow overriding it in tests to check behavior in case of errors.
     *
     * @param args Operation arguments.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    void doAck(DdlCommandArguments args) throws IgniteCheckedException {
        args.getOperationArguments().opType.command().execute(args);
    }

    /**
     * Handle local DDL operation result from <b>a peer node</b> on <b>the coordinator</b>.
     *
     * @param opId DDL operation ID.
     * @param err Exception that occurred on the <b>peer</b>, or null if the local operation has been successful.
     */
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "SynchronizationOnLocalVariableOrMethodParameter"})
    private void onNodeResult(IgniteUuid opId, IgniteCheckedException err) {
        DdlOperationRunContext runCtx = operationRuns.get(opId);

        if (runCtx == null)
            throw new IgniteSQLException("Run context not found on coordinator [opId=" +
                opId + ']'); // Can't throw a checked ex from here

        DdlCommandArguments args = operationArgs.get(opId);

        assert args != null;

        if (err != null) {
            synchronized (runCtx) { // Let's synchronize for correct list mutation here
                List<IgniteCheckedException> errs = runCtx.errs;

                if (errs == null) {
                    errs = new ArrayList<>();

                    runCtx.errs = errs;
                }

                errs.add(err);
            }
        }

        // Return if we expect more messages like this
        if (runCtx.nodesCnt.decrementAndGet() > 0)
            return;

        List<IgniteCheckedException> errs = runCtx.errs;

        IgniteCheckedException resEx = null;

        if (errs != null) {
            resEx = new IgniteCheckedException("DDL operation execution has failed [opId=" + args.getOperationArguments()
                .opId + ']');

            for (int i = 0; i < errs.size(); i++)
                resEx.addSuppressed(errs.get(i));
        }

        sendResult(args.getOperationArguments(), resEx);
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
            idx.getLogger().error("Failed to notify client node about DDL operation failure " +
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
            idx.getLogger().warning("DDL operation not found at its client [opId=" + opId + ", nodeId=" +
                ctx.localNodeId() + ']');

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
        DdlCommandArguments args = msg.getArguments();

        if (msg.getNodesState() == null) {
            // Null state means we're at coordinator, so let's populate state with participating nodes
            // in accordance with topology version.
            // We take cache nodes and not just affinity nodes as long as we must create index on client
            // nodes as well to build query plans correctly.
            Collection<ClusterNode> nodes = args.getOperationArguments().opType.command().filterNodes(ctx, args,
                topVer);

            Map<UUID, IgniteCheckedException> newNodesState = new HashMap<>();

            for (ClusterNode node : nodes)
                newNodesState.put(node.id(), null);

            msg.setNodesState(newNodesState);

            operationRuns.put(args.getOperationArguments().opId, new DdlOperationRunContext(newNodesState.size()));
        }
        else if (!msg.getNodesState().containsKey(ctx.localNodeId()))
            return;

        try {
            operationArgs.put(args.getOperationArguments().opId, args);

            doInit(args);
        }
        catch (Throwable e) {
            msg.getNodesState().put(ctx.localNodeId(), wrapThrowableIfNeeded(e));
        }
    }

    /**
     * Perform actual INIT actions.
     * Exists as a separate method to allow overriding it in tests to check behavior in case of errors.
     *
     * @param args Operation arguments.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    void doInit(DdlCommandArguments args) throws IgniteCheckedException {
        args.getOperationArguments().opType.command().init(args);
    }

    /**
     * Optionally wrap a {@link Throwable} into an {@link IgniteCheckedException}.
     *
     * @param e Throwable to wrap.
     * @return {@code e} if it's an {@link IgniteCheckedException} or an {@link IgniteCheckedException} wrapping it.
     */
    private static IgniteCheckedException wrapThrowableIfNeeded(Throwable e) {
        if (e instanceof IgniteCheckedException)
            return (IgniteCheckedException) e;
        else
            return new IgniteCheckedException(e);
    }

    /**
     * Do cleanup.
     */
    public void stop() {
        isStopped = true;

        for (DdlOperationFuture f : operations.values())
            try {
                f.cancel();
            }
            catch (IgniteCheckedException e) {
                idx.getLogger().warning("Failed to cancel DDL future [opId=" + f.getId() + ']', e);
            }
    }

    /**
     * Execute DDL statement.
     *
     * @param stmt H2 statement to parse and execute.
     */
    public QueryCursor<List<?>> runDdlStatement(PreparedStatement stmt)
        throws IgniteCheckedException {
        if (isStopped)
            throw new IgniteCheckedException(new IllegalStateException("DDL processor has been stopped"));

        assert stmt instanceof JdbcPreparedStatement;

        GridSqlStatement gridStmt = new GridSqlQueryParser().parse(GridSqlQueryParser
            .prepared((JdbcPreparedStatement) stmt));

        IgniteUuid opId = IgniteUuid.randomUuid();

        if (gridStmt instanceof GridSqlCreateIndex) {
            GridSqlCreateIndex createIdx = (GridSqlCreateIndex) gridStmt;

            DdlOperationArguments opArgs = new DdlOperationArguments(ctx.localNodeId(), opId,
                DdlOperationType.CREATE_INDEX);

            CreateIndexArguments args = new CreateIndexArguments(opArgs, createIdx.index(), createIdx.cacheName(),
                createIdx.ifNotExists());

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
    private void execute(DdlCommandArguments args) throws IgniteCheckedException {
        DdlOperationFuture op = new DdlOperationFuture(args.getOperationArguments().opId);

        operations.put(args.getOperationArguments().opId, op);

        try {
            DdlOperationInit initMsg = new DdlOperationInit();

            initMsg.setArguments(args);

            ctx.discovery().sendCustomEvent(initMsg);

            op.get();
        }
        finally {
            operations.remove(args.getOperationArguments().opId);
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
            return U.unmarshal(ctx, ex, U.resolveClassLoader(ctx.config()));
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

    /**
     * Operation run context on the <b>coordinator</b>.
     */
    private static class DdlOperationRunContext {
        /** Latch replacement to expect all nodes to finish their local jobs for this operation. */
        final AtomicInteger nodesCnt;

        /** Errors from nodes, if any. */
        volatile List<IgniteCheckedException> errs;

        /**
         * @param nodesCnt Nodes count to initialize latch with.
         */
        DdlOperationRunContext(int nodesCnt) {
            this.nodesCnt = new AtomicInteger(nodesCnt);
        }
    }
}
