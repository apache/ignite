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

package org.gridgain.grid.kernal.processors.rest.handlers.task;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.portables.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.kernal.processors.rest.request.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;
import static org.gridgain.grid.kernal.processors.rest.GridRestCommand.*;
import static org.jdk8.backport.ConcurrentLinkedHashMap.QueuePolicy.*;

/**
 * Command handler for API requests.
 */
public class GridTaskCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(EXE, RESULT, NOOP);

    /** Default maximum number of task results. */
    private static final int DFLT_MAX_TASK_RESULTS = 10240;

    /** Maximum number of task results. */
    private final int maxTaskResults = getInteger(GG_REST_MAX_TASK_RESULTS, DFLT_MAX_TASK_RESULTS);

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
    @Override public IgniteFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        try {
            return handleAsyncUnsafe(req);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to execute task command: " + req, e);

            return new GridFinishedFuture<>(ctx, e);
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
    private IgniteFuture<GridRestResponse> handleAsyncUnsafe(final GridRestRequest req) throws IgniteCheckedException {
        assert req instanceof GridRestTaskRequest : "Invalid command for topology handler: " + req;

        assert SUPPORTED_COMMANDS.contains(req.command());

        if (log.isDebugEnabled())
            log.debug("Handling task REST request: " + req);

        GridRestTaskRequest req0 = (GridRestTaskRequest) req;

        final GridFutureAdapter<GridRestResponse> fut = new GridFutureAdapter<>(ctx);

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

                final ComputeTaskFuture<Object> taskFut;

                if (locExec) {
                    ClusterGroup prj = ctx.grid().forSubjectId(clientId);

                    IgniteCompute comp = ctx.grid().compute(prj).withTimeout(timeout).enableAsync();

                    Object arg = !F.isEmpty(params) ? params.size() == 1 ? params.get(0) : params.toArray() : null;

                    comp.execute(name, arg);

                    taskFut = comp.future();
                }
                else {
                    // Using predicate instead of node intentionally
                    // in order to provide user well-structured EmptyProjectionException.
                    ClusterGroup prj = ctx.grid().forPredicate(F.nodeForNodeId(req.destinationId()));

                    IgniteCompute comp = ctx.grid().compute(prj).withNoFailover().enableAsync();

                    comp.call(new ExeCallable(name, params, timeout, clientId));

                    taskFut = comp.future();
                }

                if (async) {
                    if (locExec) {
                        IgniteUuid tid = taskFut.getTaskSession().getId();

                        taskDescs.put(tid, new TaskDescriptor(false, null, null));

                        taskRestRes.setId(tid.toString() + '~' + ctx.localNodeId().toString());

                        res.setResponse(taskRestRes);
                    }
                    else
                        res.setError("Asynchronous task execution is not supported for routing request.");

                    fut.onDone(res);
                }

                taskFut.listenAsync(new IgniteInClosure<IgniteFuture<Object>>() {
                    @Override public void apply(IgniteFuture<Object> f) {
                        try {
                            TaskDescriptor desc;

                            try {
                                desc = new TaskDescriptor(true, f.get(), null);
                            }
                            catch (IgniteCheckedException e) {
                                if (e.hasCause(ClusterTopologyException.class, ClusterGroupEmptyException.class))
                                    U.warn(log, "Failed to execute task due to topology issues (are all mapped " +
                                        "nodes alive?) [name=" + name + ", clientId=" + req.clientId() +
                                        ", err=" + e + ']');
                                else
                                    U.error(log, "Failed to execute task [name=" + name + ", clientId=" +
                                        req.clientId() + ']', e);

                                desc = new TaskDescriptor(true, null, e);
                            }

                            if (async && locExec) {
                                assert taskFut instanceof ComputeTaskFuture;

                                IgniteUuid tid = ((ComputeTaskFuture)taskFut).getTaskSession().getId();

                                taskDescs.put(tid, desc);
                            }

                            if (!async) {
                                if (desc.error() == null) {
                                    try {
                                        taskRestRes.setFinished(true);
                                        taskRestRes.setResult(req.portableMode() ?
                                            ctx.portable().marshalToPortable(desc.result()) : desc.result());

                                        res.setResponse(taskRestRes);
                                        fut.onDone(res);
                                    }
                                    catch (PortableException e) {
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
            @Override public void onEvent(IgniteEvent evt) {
                assert evt instanceof IgniteDiscoveryEvent &&
                    (evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT) : "Unexpected event: " + evt;

                IgniteDiscoveryEvent discoEvt = (IgniteDiscoveryEvent)evt;

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
        private GridEx g;

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
            return g.compute(g.forSubjectId(clientId)).execute(
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
