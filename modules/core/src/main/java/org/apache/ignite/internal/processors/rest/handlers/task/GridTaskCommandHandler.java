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

package org.apache.ignite.internal.processors.rest.handlers.task;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.ComputeTaskInternalFuture;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterGroupEmptyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.client.message.GridClientTaskResultBean;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestTaskRequest;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.util.VisorClusterGroupEmptyException;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REST_MAX_TASK_RESULTS;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridClosureCallMode.BALANCE;
import static org.apache.ignite.internal.GridTopic.TOPIC_REST;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.EXE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.NOOP;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.RESULT;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_NO_FAILOVER;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SUBJ_ID;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_TIMEOUT;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.PER_SEGMENT_Q;

/**
 * Command handler for API requests.
 */
public class GridTaskCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(EXE, RESULT, NOOP);

    /** Default maximum number of task results. */
    private static final int DFLT_MAX_TASK_RESULTS = 10240;

    /** Maximum number of task results. */
    private final int maxTaskResults = getInteger(IGNITE_REST_MAX_TASK_RESULTS, DFLT_MAX_TASK_RESULTS);

    /** Task results. */
    private final Map<IgniteUuid, TaskDescriptor> taskDescs =
        new GridBoundedConcurrentLinkedHashMap<>(maxTaskResults, 16, 0.75f, 4, PER_SEGMENT_Q);

    /** Topic ID generator. */
    private final AtomicLong topicIdGen = new AtomicLong();

    /**
     * @param ctx Context.
     */
    public GridTaskCommandHandler(final GridKernalContext ctx) {
        super(ctx);

        ctx.io().addMessageListener(TOPIC_REST, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                if (!(msg instanceof GridTaskResultRequest)) {
                    U.warn(log, "Received unexpected message instead of task result request: " + msg);

                    return;
                }

                try {
                    GridTaskResultRequest req = (GridTaskResultRequest)msg;

                    GridTaskResultResponse res = new GridTaskResultResponse();

                    IgniteUuid taskId = req.taskId();

                    TaskDescriptor desc = taskDescs.get(taskId);

                    if (desc != null) {
                        res.found(true);
                        res.finished(desc.finished());

                        Throwable err = desc.error();

                        if (err != null)
                            res.error(err.getMessage());
                        else {
                            res.result(desc.result());
                            res.resultBytes(ctx.config().getMarshaller().marshal(desc.result()));
                        }
                    }
                    else
                        res.found(false);

                    Object topic = ctx.config().getMarshaller().unmarshal(req.topicBytes(), null);

                    ctx.io().send(nodeId, topic, res, SYSTEM_POOL);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send job task result response.", e);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        try {
            return handleAsyncUnsafe(req);
        }
        catch (IgniteCheckedException e) {
            if (!X.hasCause(e, VisorClusterGroupEmptyException.class))
                U.error(log, "Failed to execute task command: " + req, e);

            return new GridFinishedFuture<>(e);
        }
        finally {
            if (log.isDebugEnabled())
                log.debug("Handled task REST request: " + req);
        }
    }

    /**
     * @param req Request.
     * @return Future.
     * @throws IgniteCheckedException On any handling exception.
     */
    private IgniteInternalFuture<GridRestResponse> handleAsyncUnsafe(final GridRestRequest req) throws IgniteCheckedException {
        assert req instanceof GridRestTaskRequest : "Invalid command for topology handler: " + req;

        assert SUPPORTED_COMMANDS.contains(req.command());

        if (log.isDebugEnabled())
            log.debug("Handling task REST request: " + req);

        GridRestTaskRequest req0 = (GridRestTaskRequest) req;

        final GridFutureAdapter<GridRestResponse> fut = new GridFutureAdapter<>();

        final GridRestResponse res = new GridRestResponse();

        final GridClientTaskResultBean taskRestRes = new GridClientTaskResultBean();

        // Set ID placeholder for the case it wouldn't be available due to remote execution.
        taskRestRes.setId('~' + ctx.localNodeId().toString());

        final boolean locExec = req0.destinationId() == null || req0.destinationId().equals(ctx.localNodeId()) ||
            ctx.discovery().node(req0.destinationId()) == null;

        switch (req.command()) {
            case EXE: {
                final boolean async = req0.async();

                final String name = req0.taskName();

                if (F.isEmpty(name))
                    throw new IgniteCheckedException(missingParameter("name"));

                final List<Object> params = req0.params();

                long timeout = req0.timeout();

                final UUID clientId = req.clientId();

                final IgniteInternalFuture<Object> taskFut;

                if (locExec) {
                    ctx.task().setThreadContextIfNotNull(TC_SUBJ_ID, clientId);
                    ctx.task().setThreadContext(TC_TIMEOUT, timeout);

                    Object arg = !F.isEmpty(params) ? params.size() == 1 ? params.get(0) : params.toArray() : null;

                    taskFut = ctx.task().execute(name, arg);
                }
                else {
                    // Using predicate instead of node intentionally
                    // in order to provide user well-structured EmptyProjectionException.
                    ClusterGroup prj = ctx.grid().cluster().forPredicate(F.nodeForNodeId(req.destinationId()));

                    ctx.task().setThreadContext(TC_NO_FAILOVER, true);

                    taskFut = ctx.closure().callAsync(
                        BALANCE,
                        new ExeCallable(name, params, timeout, clientId), prj.nodes());
                }

                if (async) {
                    if (locExec) {
                        IgniteUuid tid = ((ComputeTaskInternalFuture)taskFut).getTaskSession().getId();

                        taskDescs.put(tid, new TaskDescriptor(false, null, null));

                        taskRestRes.setId(tid.toString() + '~' + ctx.localNodeId().toString());

                        res.setResponse(taskRestRes);
                    }
                    else
                        res.setError("Asynchronous task execution is not supported for routing request.");

                    fut.onDone(res);
                }

                taskFut.listen(new IgniteInClosure<IgniteInternalFuture<Object>>() {
                    @Override public void apply(IgniteInternalFuture<Object> taskFut) {
                        try {
                            TaskDescriptor desc;

                            try {
                                desc = new TaskDescriptor(true, taskFut.get(), null);
                            }
                            catch (IgniteCheckedException e) {
                                if (e.hasCause(ClusterTopologyCheckedException.class, ClusterGroupEmptyCheckedException.class))
                                    U.warn(log, "Failed to execute task due to topology issues (are all mapped " +
                                        "nodes alive?) [name=" + name + ", clientId=" + req.clientId() +
                                        ", err=" + e + ']');
                                else {
                                    if (!X.hasCause(e, VisorClusterGroupEmptyException.class))
                                        U.error(log, "Failed to execute task [name=" + name + ", clientId=" +
                                            req.clientId() + ']', e);
                                }

                                desc = new TaskDescriptor(true, null, e);
                            }

                            if (async && locExec) {
                                assert taskFut instanceof ComputeTaskInternalFuture;

                                IgniteUuid tid = ((ComputeTaskInternalFuture)taskFut).getTaskSession().getId();

                                taskDescs.put(tid, desc);
                            }

                            if (!async) {
                                if (desc.error() == null) {
                                    try {
                                        taskRestRes.setFinished(true);
                                        taskRestRes.setResult(desc.result());

                                        res.setResponse(taskRestRes);
                                        fut.onDone(res);
                                    }
                                    catch (IgniteException e) {
                                        fut.onDone(new IgniteCheckedException("Failed to marshal task result: " +
                                            desc.result(), e));
                                    }
                                }
                                else
                                    fut.onDone(desc.error());
                            }
                        }
                        finally {
                            if (!async && !fut.isDone())
                                fut.onDone(new IgniteCheckedException("Failed to execute task (see server logs for details)."));
                        }
                    }
                });

                break;
            }

            case RESULT: {
                String id = req0.taskId();

                if (F.isEmpty(id))
                    throw new IgniteCheckedException(missingParameter("id"));

                StringTokenizer st = new StringTokenizer(id, "~");

                if (st.countTokens() != 2)
                    throw new IgniteCheckedException("Failed to parse id parameter: " + id);

                String tidParam = st.nextToken();
                String resHolderIdParam = st.nextToken();

                taskRestRes.setId(id);

                try {
                    IgniteUuid tid = !F.isEmpty(tidParam) ? IgniteUuid.fromString(tidParam) : null;

                    UUID resHolderId = !F.isEmpty(resHolderIdParam) ? UUID.fromString(resHolderIdParam) : null;

                    if (tid == null || resHolderId == null)
                        throw new IgniteCheckedException("Failed to parse id parameter: " + id);

                    if (ctx.localNodeId().equals(resHolderId)) {
                        TaskDescriptor desc = taskDescs.get(tid);

                        if (desc == null)
                            throw new IgniteCheckedException("Task with provided id has never been started on provided node" +
                                " [taskId=" + tidParam + ", taskResHolderId=" + resHolderIdParam + ']');

                        taskRestRes.setFinished(desc.finished());

                        if (desc.error() != null)
                            throw new IgniteCheckedException(desc.error().getMessage());

                        taskRestRes.setResult(desc.result());

                        res.setResponse(taskRestRes);
                    }
                    else {
                        IgniteBiTuple<String, GridTaskResultResponse> t = requestTaskResult(resHolderId, tid);

                        if (t.get1() != null)
                            throw new IgniteCheckedException(t.get1());

                        GridTaskResultResponse taskRes = t.get2();

                        assert taskRes != null;

                        if (!taskRes.found())
                            throw new IgniteCheckedException("Task with provided id has never been started on provided node " +
                                "[taskId=" + tidParam + ", taskResHolderId=" + resHolderIdParam + ']');

                        taskRestRes.setFinished(taskRes.finished());

                        if (taskRes.error() != null)
                            throw new IgniteCheckedException(taskRes.error());

                        taskRestRes.setResult(taskRes.result());

                        res.setResponse(taskRestRes);
                    }
                }
                catch (IllegalArgumentException e) {
                    String msg = "Failed to parse parameters [taskId=" + tidParam + ", taskResHolderId="
                        + resHolderIdParam + ", err=" + e.getMessage() + ']';

                    if (log.isDebugEnabled())
                        log.debug(msg);

                    throw new IgniteCheckedException(msg, e);
                }

                fut.onDone(res);

                break;
            }

            case NOOP: {
                fut.onDone(new GridRestResponse());

                break;
            }

            default:
                assert false : "Invalid command for task handler: " + req;
        }

        if (log.isDebugEnabled())
            log.debug("Handled task REST request [res=" + res + ", req=" + req + ']');

        return fut;
    }

    /**
     * @param resHolderId Result holder.
     * @param taskId Task ID.
     * @return Response from task holder.
     */
    private IgniteBiTuple<String, GridTaskResultResponse> requestTaskResult(final UUID resHolderId, IgniteUuid taskId) {
        ClusterNode taskNode = ctx.discovery().node(resHolderId);

        if (taskNode == null)
            return F.t("Task result holder has left grid: " + resHolderId, null);

        // Tuple: error message-response.
        final IgniteBiTuple<String, GridTaskResultResponse> t = F.t2();

        final Lock lock = new ReentrantLock();
        final Condition cond = lock.newCondition();

        GridMessageListener msgLsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                String err = null;
                GridTaskResultResponse res = null;

                if (!(msg instanceof GridTaskResultResponse))
                    err = "Received unexpected message: " + msg;
                else if (!nodeId.equals(resHolderId))
                    err = "Received task result response from unexpected node [resHolderId=" + resHolderId +
                        ", nodeId=" + nodeId + ']';
                else
                    // Sender and message type are fine.
                    res = (GridTaskResultResponse)msg;

                try {
                    res.result(ctx.config().getMarshaller().unmarshal(res.resultBytes(), null));
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to unmarshal task result: " + res, e);
                }

                lock.lock();

                try {
                    if (t.isEmpty()) {
                        t.set(err, res);

                        cond.signalAll();
                    }
                }
                finally {
                    lock.unlock();
                }
            }
        };

        GridLocalEventListener discoLsnr = new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                assert evt instanceof DiscoveryEvent &&
                    (evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT) : "Unexpected event: " + evt;

                DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                if (resHolderId.equals(discoEvt.eventNode().id())) {
                    lock.lock();

                    try {
                        if (t.isEmpty()) {
                            t.set("Node that originated task execution has left grid: " + resHolderId, null);

                            cond.signalAll();
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }
            }
        };

        // 1. Create unique topic name and register listener.
        Object topic = TOPIC_REST.topic("task-result", topicIdGen.getAndIncrement());

        try {
            ctx.io().addMessageListener(topic, msgLsnr);

            // 2. Send message.
            try {
                byte[] topicBytes = ctx.config().getMarshaller().marshal(topic);

                ctx.io().send(taskNode, TOPIC_REST, new GridTaskResultRequest(taskId, topic, topicBytes), SYSTEM_POOL);
            }
            catch (IgniteCheckedException e) {
                String errMsg = "Failed to send task result request [resHolderId=" + resHolderId +
                    ", err=" + e.getMessage() + ']';

                if (log.isDebugEnabled())
                    log.debug(errMsg);

                return F.t(errMsg, null);
            }

            // 3. Listen to discovery events.
            ctx.event().addLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);

            // 4. Check whether node has left before disco listener has been installed.
            taskNode = ctx.discovery().node(resHolderId);

            if (taskNode == null)
                return F.t("Task result holder has left grid: " + resHolderId, null);

            // 5. Wait for result.
            lock.lock();

            try {
                long netTimeout = ctx.config().getNetworkTimeout();

                if (t.isEmpty())
                    cond.await(netTimeout, MILLISECONDS);

                if (t.isEmpty())
                    t.set1("Timed out waiting for task result (consider increasing 'networkTimeout' " +
                        "configuration property) [resHolderId=" + resHolderId + ", netTimeout=" + netTimeout + ']');

                // Return result
                return t;
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();

                return F.t("Interrupted while waiting for task result.", null);
            }
            finally {
                lock.unlock();
            }
        }
        finally {
            ctx.io().removeMessageListener(topic, msgLsnr);
            ctx.event().removeLocalEventListener(discoLsnr);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTaskCommandHandler.class, this);
    }

    /**
     * Immutable task execution state descriptor.
     */
    private static class TaskDescriptor {
        /** */
        private final boolean finished;

        /** */
        private final Object res;

        /** */
        private final Throwable err;

        /**
         * @param finished Finished flag.
         * @param res Result.
         * @param err Error.
         */
        private TaskDescriptor(boolean finished, @Nullable Object res, @Nullable Throwable err) {
            this.finished = finished;
            this.res = res;
            this.err = err;
        }

        /**
         * @return {@code true} if finished.
         */
        public boolean finished() {
            return finished;
        }

        /**
         * @return Task result.
         */
        @Nullable public Object result() {
            return res;
        }

        /**
         * @return Error.
         */
        @Nullable public Throwable error() {
            return err;
        }
    }

    /**
     * Callable for EXE request routing.
     */
    @GridInternal
    private static class ExeCallable implements Callable<Object>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String name;

        /** */
        private List<Object> params;

        /** */
        private long timeout;

        /** */
        private UUID clientId;

        /** */
        @IgniteInstanceResource
        private IgniteEx g;

        /**
         * Required by {@link Externalizable}.
         */
        public ExeCallable() {
            // No-op.
        }

        /**
         * @param name Name.
         * @param params Params.
         * @param timeout Timeout.
         * @param clientId Client ID.
         */
        private ExeCallable(String name, List<Object> params, long timeout, UUID clientId) {
            this.name = name;
            this.params = params;
            this.timeout = timeout;
            this.clientId = clientId;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return g.compute(g.cluster().forSubjectId(clientId)).execute(
                name,
                !params.isEmpty() ? params.size() == 1 ? params.get(0) : params.toArray() : null);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, name);
            out.writeObject(params);
            out.writeLong(timeout);
            U.writeUuid(out, clientId);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = U.readString(in);
            params = (List<Object>)in.readObject();
            timeout = in.readLong();
            clientId = U.readUuid(in);
        }
    }
}