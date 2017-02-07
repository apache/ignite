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
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationInitError;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationNodeResult;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationResult;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.util.typedef.F;
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

        ctx.discovery().setCustomEventListener(DdlOperationInitError.class,
            new CustomEventListener<DdlOperationInitError>() {
            /** {@inheritDoc} */
            @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
                DdlOperationInitError msg) {
                onInitError(snd, msg);
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

                    onResult(res.getOperationId(), bytesToErrors(res.getErrors()));
                }

                if (msg instanceof DdlOperationNodeResult) {
                    DdlOperationNodeResult res = (DdlOperationNodeResult) msg;

                    onNodeResult(res.getOperationId(), nodeId, bytesToException(res.getError()));
                }
            }
        });
    }

    /**
     * Process local event of cancel. <b>Coordinator</b> and <b>client</b> take additional actions for cleanup.
     *
     * @param msg Message.
     */
    @SuppressWarnings("unchecked")
    private void onCancel(DdlOperationCancel msg) {
        DdlOperationArguments args = operationArgs.remove(msg.getOperationId());

        if (args == null)
            return;

        if (!ctx.grid().localNode().isClient())
            try {
                args.opType.command().cancel(args);
            }
            catch (Throwable e) {
                log.warning("Failed to process DDL CANCEL locally [opId=" + msg.getOperationId() +']', e);
            }

        DdlOperationRunContext runCtx = operationRuns.remove(msg.getOperationId());

        if (runCtx != null) {
            runCtx.isCancelled = true;

            // Let's free the waiting thread if there is one
            while (runCtx.latch.getCount() > 0)
                runCtx.latch.countDown();
        }

        if (ctx.localNodeId().equals(args.sndNodeId)) {
            DdlOperationFuture fut = operations.remove(msg.getOperationId());

            if (fut != null) {
                IgniteCheckedException ex = msg.getError();

                if (ex == null)
                    ex = new IgniteCheckedException("DDL operation has been cancelled (presumably by the user).");

                fut.onDone(ex);
            }
            else
                log.error("DDL operation not found on its client node [opId=" + msg.getOperationId() +']');
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
     * @param opId DDL operation ID.
     * @param uuid Peer id.
     * @param err Exception that occurred on the <b>peer</b>, or null if the local operation has been successful.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void onNodeResult(IgniteUuid opId, UUID uuid, IgniteCheckedException err) {
        DdlOperationArguments args = operationArgs.get(opId);

        DdlOperationRunContext runCtx = operationRuns.get(opId);

        // First error fails whole operation.
        if (args == null || runCtx == null || err != null) {
            cancel(opId, err);

            return;
        }

        runCtx.latch.countDown();

        // We've got our own message, let's wait here
        if (ctx.localNodeId().equals(uuid)) {
            /*try {
                runCtx.latch.await();
            }
            catch (InterruptedException e) {
                // We're interrupted, let's cancel
                if (!runCtx.isCancelled)
                    cancel(opId, new IgniteCheckedException(e));

                return;
            }*/

            // We've been cancelled while waiting, nothing to do here anymore
            // as peers have been notified already by the same cancellation message
            if (runCtx.isCancelled)
                return;

            DdlOperationResult res = new DdlOperationResult();

            res.setOperationId(opId);

            //Ok, we're done, let's send result to the client
            try {
                ctx.io().send(ctx.grid().cluster().node(args.sndNodeId), GridTopic.TOPIC_DDL, res, GridIoPolicy.IDX_POOL);
            }
            catch (Throwable e) {
                log.error("Failed to notify client about DLL operation completion [opId=" + opId +
                    ", sndNodeId=" + args.sndNodeId + ']', e);
            }
        }
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
     * Callback handling whole DDL operation result <b>on the client</b>.
     *
     * @param opId DDL operation ID.
     * @param errors Map of node IDs to their errors.
     */
    @SuppressWarnings("unchecked")
    private void onResult(IgniteUuid opId, Map<UUID, IgniteCheckedException> errors) {
        DdlOperationFuture fut = operations.get(opId);

        if (fut == null) {
            log.warning("DDL operation not found at its client [opId=" + opId + ", nodeId=" + ctx.localNodeId() + ']');

            return;
        }

        IgniteCheckedException resEx = null;

        if (!F.isEmpty(errors)) {
            resEx = new IgniteCheckedException("DDL operation failed");

            for (IgniteCheckedException e : errors.values())
                resEx.addSuppressed(e);
        }

        fut.onDone(null, resEx);
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
            Collection<ClusterNode> nodes = ctx.discovery().cacheAffinityNodes(args.cacheName,
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

    /**
     * Revert effects of {@code INIT} stage of DDL statement execution <b>on a peer node</b> because of error(s).
     * <b>Coordinator</b> additionally notifies <b>client</b> about whole operation failure.
     *
     * @param snd Sender.
     * @param msg Message.
     */
    @SuppressWarnings("unchecked")
    private void onInitError(ClusterNode snd, DdlOperationInitError msg) {
        DdlOperationArguments args = operationArgs.remove(msg.getOperationId());

        if (args != null)
            try {
                args.opType.command().initError(args);
            }
            catch (Throwable e) {
                log.warning("Failed to revert local INIT of a DDL operation [opId=" + args.opId + ", " +
                    "nodeId=" + ctx.localNodeId() + ']');
            }

        ClusterNode locNode = ctx.grid().localNode();

        boolean isCoord = F.eqNodes(snd, locNode);

        // Ring sent error message will be processed once at coordinator which is deemed as its sender
        // and who will notify initial client of the whole operation
        if (!isCoord)
            return;
        else if (args == null) {
            log.warning("DDL operation not found by its id at its coordinator [opId=" + msg.getOperationId() + ", " +
                "nodeId=" + ctx.localNodeId() + ']');

            return;
        }

        ClusterNode client = ctx.discovery().node(args.sndNodeId);

        if (client == null) {
            log.warning("Failed to notify client node about DDL operation INIT failure " +
                "(has node left grid?): [opId=" + args.opId + ", sndNodeId=" + args.sndNodeId + ']');

            return;
        }

        // How would we get here otherwise?
        assert !F.isEmpty(msg.getErrors());

        if (F.eq(client, locNode)) {
            onResult(args.opId, msg.getErrors());

            return;
        }

        DdlOperationResult res = new DdlOperationResult();

        res.setOperationId(args.opId);

        res.setErrors(errorsToBytes(msg.getErrors()));

        try {
            ctx.io().send(client, GridTopic.TOPIC_DDL, res, GridIoPolicy.IDX_POOL);
        }
        catch (IgniteCheckedException e) {
            log.warning("Failed to notify client node about DDL operation INIT failure: " +
                "[opId=" + args.opId + ", sndNodeId=" + args.sndNodeId + ']', e);
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
     * Convert map values which are {@link IgniteCheckedException}s to byte arrays to send to client.
     *
     * @param errors Map of node IDs to {@link IgniteCheckedException}s.
     * @return Map of node IDs to serialized {@link IgniteCheckedException}s.
     */
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "ThrowableInstanceNeverThrown"})
    private Map<UUID, byte[]> errorsToBytes(Map<UUID, IgniteCheckedException> errors){
        if (F.isEmpty(errors))
            return null;

        Map<UUID, byte[]> res = new HashMap<>();

        for (Map.Entry<UUID, IgniteCheckedException> e : errors.entrySet()) {
            assert e.getValue() != null;

            res.put(e.getKey(), exceptionToBytes(e.getValue()));
        }

        return res;
    }

    /**
     * Convert map values which are byte arrays back to {@link IgniteCheckedException}s to process at client.
     *
     * @param errors Map of node IDs to serialized {@link IgniteCheckedException}s.
     * @return Map of node IDs to {@link IgniteCheckedException}s.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private Map<UUID, IgniteCheckedException> bytesToErrors(Map<UUID, byte[]> errors) {
        if (F.isEmpty(errors))
            return null;

        Map<UUID, IgniteCheckedException> res = new HashMap<>();

        for (Map.Entry<UUID, byte[]> e : errors.entrySet()) {
            assert e.getValue() != null;

            res.put(e.getKey(), bytesToException(e.getValue()));
        }

        return res;
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
