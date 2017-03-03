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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.ddl.DdlOperationNodeResult;
import org.apache.ignite.internal.processors.query.ddl.DdlOperationResult;
import org.apache.ignite.internal.processors.query.h2.DmlStatementsProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationAck;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationInit;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
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

    /** State flag. */
    private AtomicBoolean isStopped = new AtomicBoolean();

    /** Running operations originating at this node as a client. */
    private Map<IgniteUuid, GridFutureAdapter> operations = new ConcurrentHashMap8<>();

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
                onInit(msg);
            }
        });

        ctx.discovery().setCustomEventListener(DdlOperationAck.class, new CustomEventListener<DdlOperationAck>() {
            /** {@inheritDoc} */
            @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, DdlOperationAck msg) {
                onAck(snd, msg);
            }
        });

        ctx.io().addMessageListener(GridTopic.TOPIC_QUERY, new GridMessageListener() {
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
     * Handle {@code ACK} message on a <b>peer node</b> - do local portion of actual DDL job and notify
     * <b>coordinator</b> about success or failure.
     *
     * @param snd Sender.
     * @param msg Message.
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown", "unchecked"})
    private void onAck(ClusterNode snd, DdlOperationAck msg) {
        IgniteCheckedException ex = null;

        DdlCommandArguments args = msg.getArguments();

        try {
            doAck(args);
        }
        catch (Throwable e) {
            ex = wrapThrowableIfNeeded(e);
        }

        try {
            DdlOperationNodeResult res = new DdlOperationNodeResult();

            res.setOperationId(msg.getOperationId());
            res.setError(exceptionToBytes(ex));

            ctx.io().send(snd, GridTopic.TOPIC_QUERY, res, GridIoPolicy.IDX_POOL);
        }
        catch (Throwable e) {
            idx.getLogger().error("Failed to notify coordinator about local DLL operation completion [opId=" +
                msg.getOperationId() + ", clientNodeId=" + snd.id() + ']', e);
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
        if (args instanceof CreateIndexArguments) {
            // No-op.
        }
    }

    /**
     * Handle local DDL operation result from <b>a peer node</b> on <b>the coordinator</b>.
     *
     * @param opId DDL operation ID.
     * @param err Exception that occurred on the <b>peer</b>, or null if the local operation has been successful.
     */
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "SynchronizationOnLocalVariableOrMethodParameter", "ForLoopReplaceableByForEach"})
    private void onNodeResult(IgniteUuid opId, IgniteCheckedException err) {
        // No-op.
    }

    /**
     * Process result of executing {@link DdlOperationInit} and react accordingly.
     * Called from {@link DdlOperationInit#ackMessage()}.
     *
     * @param msg {@link DdlOperationInit} message.
     * @return {@link DiscoveryCustomMessage} to return from {@link DdlOperationInit#ackMessage()}.
     */
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "UnnecessaryInitCause"})
    public DiscoveryCustomMessage onInitFinished(DdlOperationInit msg) {
        Map<UUID, IgniteCheckedException> nodesState = msg.getNodesState();

        assert nodesState != null;

        Map<UUID, IgniteCheckedException> errors = new HashMap<>();

        for (Map.Entry<UUID, IgniteCheckedException> e : nodesState.entrySet())
            if (e.getValue() != null)
                errors.put(e.getKey(), e.getValue());

        DdlCommandArguments args = msg.getArguments();

        if (!errors.isEmpty()) {
            IgniteCheckedException resEx = new IgniteCheckedException("DDL operation has been cancelled at INIT stage");

            if (errors.size() > 1) {
                for (IgniteCheckedException e : errors.values())
                    resEx.addSuppressed(e);
            }
            else
                resEx.initCause(errors.values().iterator().next());

            sendResult(args, resEx);

            return null;
        }
        else {
            // We should prepare ourselves to getting status messages from workers here
            /* operationRuns.put(args.operationId(), new DdlOperationRunContext(args, nodesState.size())); */

            DdlOperationAck ackMsg = new DdlOperationAck();

            ackMsg.setOperationId(args.operationId());

            ackMsg.setArguments(args);

            return ackMsg;
        }
    }

    /**
     * Notify client about result.
     *
     * @param args Operation arguments.
     * @param err Error, if any.
     */
    private void sendResult(DdlCommandArguments args, IgniteCheckedException err) {
        assert args != null;

        DdlOperationResult res = new DdlOperationResult();

        res.setError(exceptionToBytes(err));
        res.setOperationId(args.operationId());

        try {
            ctx.io().send(args.clientNodeId(), GridTopic.TOPIC_QUERY, res, GridIoPolicy.IDX_POOL);
        }
        catch (IgniteCheckedException e) {
            idx.getLogger().error("Failed to notify client node about DDL operation failure " +
                "[opId=" + args.operationId() + ", clientNodeId=" + args.clientNodeId() + ']', e);
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
        GridFutureAdapter fut = operations.get(opId);

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
     * @param msg {@code INIT} message.
     */
    @SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
    private void onInit(DdlOperationInit msg) {
        DdlCommandArguments args = msg.getArguments();

        try {
            // Let's tell everyone that we're participating if our init is successful...
            if (doInit(args))
                msg.getNodesState().put(ctx.localNodeId(), null);
        }
        catch (Throwable e) {
            // Or tell everyone about the error that occurred
            msg.getNodesState().put(ctx.localNodeId(), wrapThrowableIfNeeded(e));
        }
    }

    /**
     * Perform actual INIT actions.
     * Exists as a separate method to allow overriding it in tests to check behavior in case of errors.
     *
     * @param args Operation arguments.
     * @return Whether this node participates in this operation, or not.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    boolean doInit(DdlCommandArguments args) throws IgniteCheckedException {
        if (args instanceof CreateIndexArguments) {
            return true;
        }

        return false;
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
    public void stop() throws IgniteCheckedException {
        if (!isStopped.compareAndSet(false, true))
            throw new IgniteCheckedException(new IllegalStateException("DDL processor has been stopped already"));

        for (Map.Entry<IgniteUuid, GridFutureAdapter> e : operations.entrySet())
            e.getValue().onDone(new IgniteCheckedException("Operation has been cancelled [opId=" + e.getKey() +']'));
    }

    /**
     * Execute DDL statement.
     *
     * @param stmt H2 statement to parse and execute.
     */
    public QueryCursor<List<?>> runDdlStatement(PreparedStatement stmt)
        throws IgniteCheckedException {
        if (isStopped.get())
            throw new IgniteCheckedException(new IllegalStateException("DDL processor has been stopped"));

        assert stmt instanceof JdbcPreparedStatement;

        GridSqlStatement gridStmt = new GridSqlQueryParser().parse(GridSqlQueryParser
            .prepared((JdbcPreparedStatement) stmt));

        IgniteUuid opId = IgniteUuid.randomUuid();

        DdlCommandArguments args;

        if (gridStmt instanceof GridSqlCreateIndex) {
            GridSqlCreateIndex createIdx = (GridSqlCreateIndex) gridStmt;

            args = new CreateIndexArguments(opId, ctx.localNodeId(), createIdx.index(), createIdx.schemaName(),
                createIdx.tableName(), createIdx.ifNotExists());
        }
        else if (gridStmt instanceof GridSqlDropIndex)
            throw new UnsupportedOperationException("DROP INDEX");
        else
            throw new IgniteSQLException("Unexpected DDL operation [type=" + gridStmt.getClass() + ']',
                IgniteQueryErrorCode.UNEXPECTED_OPERATION);

        GridFutureAdapter op = new GridFutureAdapter();

        operations.put(args.operationId(), op);

        try {
            DdlOperationInit initMsg = new DdlOperationInit();

            initMsg.setArguments(args);

            ctx.discovery().sendCustomEvent(initMsg);

            op.get();
        }
        finally {
            operations.remove(args.operationId());
        }

        return DmlStatementsProcessor.cursorForUpdateResult(0L);
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
}
