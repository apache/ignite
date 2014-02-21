// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.task;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;
import static org.gridgain.grid.util.ConcurrentLinkedHashMap.QueuePolicy.*;

/**
 * Command handler for API requests.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridTaskCommandHandler extends GridRestCommandHandlerAdapter {
    /** Default maximum number of task results. */
    private static final int DFLT_MAX_TASK_RESULTS = 10240;

    /** Maximum number of task results. */
    private final int maxTaskResults = Integer.getInteger(GridSystemProperties.GG_REST_MAX_TASK_RESULTS,
        DFLT_MAX_TASK_RESULTS);

    /** Task results. */
    private final Map<GridUuid, TaskDescriptor> taskDescs =
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

                    GridUuid taskId = req.taskId();

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
                catch (GridException e) {
                    U.error(log, "Failed to send job task result response.", e);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
    @Override public boolean supported(GridRestCommand cmd) {
        switch (cmd) {
            case EXE:
            case RESULT:
            case NOOP:
                return true;

            default:
                return false;
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        try {
            return handleAsyncUnsafe(req);
        }
        catch (GridException e) {
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
     * @throws GridException On any handling exception.
     */
    private GridFuture<GridRestResponse> handleAsyncUnsafe(final GridRestRequest req) throws GridException {
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Handling task REST request: " + req);

        final GridFutureAdapter<GridRestResponse> fut = new GridFutureAdapter<>(ctx);

        final GridRestResponse res = new GridRestResponse();

        final GridClientTaskResultBean taskRestRes = new GridClientTaskResultBean();

        // Set ID placeholder for the case it wouldn't be available due to remote execution.
        taskRestRes.setId('~' + ctx.localNodeId().toString());

        final boolean locExec = req.getDestId() == null || req.getDestId().equals(ctx.localNodeId());

        switch (req.getCommand()) {
            case EXE: {
                final boolean async = Boolean.parseBoolean((String)value("async", req));

                final String name = value("name", req);

                if (F.isEmpty(name))
                    throw new GridException(missingParameter("name"));

                final List<Object> params = values("p", req);

                String s = value("timeout", req);

                long timeout = 0;

                if (s != null) {
                    try {
                        timeout = Long.parseLong(s);
                    }
                    catch (NumberFormatException ignore) {
                        throw new GridException(invalidNumericParameter("timeout"));
                    }
                }

                final long timeout0 = timeout;

                final GridFuture<Object> taskFut =
                    locExec ?
                        ctx.grid().compute().withTimeout(timeout).execute(
                            name,
                            !params.isEmpty() ? params.size() == 1 ? params.get(0) : params.toArray() : null)
                        :
                        // Using predicate instead of node intentionally
                        // in order to provide user well-structured EmptyProjectionException.
                        ctx.grid().forPredicate(F.nodeForNodeId(req.getDestId())).
                            compute().withResultClosure(X.NO_FAILOVER).call(new ExeCallable(name, params, timeout0));

                if (async) {
                    if (locExec) {
                        assert taskFut instanceof GridComputeTaskFuture;

                        GridUuid tid = ((GridComputeTaskFuture)taskFut).getTaskSession().getId();

                        taskDescs.put(tid, new TaskDescriptor(false, null, null));

                        taskRestRes.setId(tid.toString() + '~' + ctx.localNodeId().toString());

                        res.setResponse(taskRestRes);
                    }
                    else
                        res.setError("Asynchronous task execution is not supported for routing request.");

                    fut.onDone(res);
                }

                taskFut.listenAsync(new GridInClosure<GridFuture<Object>>() {
                    @Override public void apply(GridFuture<Object> f) {
                        try {
                            TaskDescriptor desc;

                            try {
                                desc = new TaskDescriptor(true, f.get(), null);
                            }
                            catch (GridException e) {
                                U.error(log, "Failed to execute task [name=" + name + ", clientId=" +
                                    req.getClientId() + ']', e);

                                desc = new TaskDescriptor(true, null, e);
                            }

                            if (async && locExec) {
                                assert taskFut instanceof GridComputeTaskFuture;

                                GridUuid tid = ((GridComputeTaskFuture)taskFut).getTaskSession().getId();

                                taskDescs.put(tid, desc);
                            }

                            if (!async) {
                                if (desc.error() == null) {
                                    taskRestRes.setFinished(true);
                                    taskRestRes.setResult(desc.result());

                                    res.setResponse(taskRestRes);
                                    fut.onDone(res);
                                }
                                else
                                    fut.onDone(desc.error());
                            }
                        }
                        finally {
                            if (!async && !fut.isDone())
                                fut.onDone(new GridException("Failed to execute task (see server logs for details)."));
                        }
                    }
                });

                break;
            }

            case RESULT:
                String id = value("id", req);

                if (F.isEmpty(id))
                    throw new GridException(missingParameter("id"));

                StringTokenizer st = new StringTokenizer(id, "~");

                if (st.countTokens() != 2)
                    throw new GridException("Failed to parse id parameter: " + id);

                String tidParam = st.nextToken();
                String resHolderIdParam = st.nextToken();

                taskRestRes.setId(id);

                try {
                    GridUuid tid = !F.isEmpty(tidParam) ? GridUuid.fromString(tidParam) : null;

                    UUID resHolderId = !F.isEmpty(resHolderIdParam) ? UUID.fromString(resHolderIdParam) : null;

                    if (tid == null || resHolderId == null)
                        throw new GridException("Failed to parse id parameter: " + id);

                    if (ctx.localNodeId().equals(resHolderId)) {
                        TaskDescriptor desc = taskDescs.get(tid);

                        if (desc == null)
                            throw new GridException("Task with provided id has never been started on provided node" +
                                " [taskId=" + tidParam + ", taskResHolderId=" + resHolderIdParam + ']');

                        taskRestRes.setFinished(desc.finished());

                        if (desc.error() != null)
                            throw new GridException(desc.error().getMessage());

                        taskRestRes.setResult(desc.result());

                        res.setResponse(taskRestRes);
                    }
                    else {
                        GridBiTuple<String, GridTaskResultResponse> t = requestTaskResult(resHolderId, tid);

                        if (t.get1() != null)
                            throw new GridException(t.get1());

                        GridTaskResultResponse taskRes = t.get2();

                        assert taskRes != null;

                        if (!taskRes.found())
                            throw new GridException("Task with provided id has never been started on provided node " +
                                "[taskId=" + tidParam + ", taskResHolderId=" + resHolderIdParam + ']');

                        taskRestRes.setFinished(taskRes.finished());

                        if (taskRes.error() != null)
                            throw new GridException(taskRes.error());

                        taskRestRes.setResult(taskRes.result());

                        res.setResponse(taskRestRes);
                    }
                }
                catch (IllegalArgumentException e) {
                    String msg = "Failed to parse parameters [taskId=" + tidParam + ", taskResHolderId="
                        + resHolderIdParam + ", err=" + e.getMessage() + ']';

                    if (log.isDebugEnabled())
                        log.debug(msg);

                    throw new GridException(msg, e);
                }

                fut.onDone(res);

                break;

            case NOOP:
                fut.onDone(new GridRestResponse());

                break;

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
    private GridBiTuple<String, GridTaskResultResponse> requestTaskResult(final UUID resHolderId, GridUuid taskId) {
        GridNode taskNode = ctx.discovery().node(resHolderId);

        if (taskNode == null)
            return F.t("Task result holder has left grid: " + resHolderId, null);

        // Tuple: error message-response.
        final GridBiTuple<String, GridTaskResultResponse> t = F.t2();

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
                catch (GridException e) {
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
            @Override public void onEvent(GridEvent evt) {
                assert evt instanceof GridDiscoveryEvent &&
                    (evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT) : "Unexpected event: " + evt;

                GridDiscoveryEvent discoEvt = (GridDiscoveryEvent)evt;

                if (resHolderId.equals(discoEvt.eventNodeId())) {
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
            catch (GridException e) {
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
        private String name;

        /** */
        private List<Object> params;

        /** */
        private long timeout;

        /** */
        @GridInstanceResource
        private Grid g;

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
         */
        private ExeCallable(String name, List<Object> params, long timeout) {
            this.name = name;
            this.params = params;
            this.timeout = timeout;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return g.compute().execute(
                name,
                !params.isEmpty() ? params.size() == 1 ? params.get(0) : params.toArray() : null).get();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, name);
            out.writeObject(params);
            out.writeLong(timeout);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = U.readString(in);
            params = (List<Object>)in.readObject();
            timeout = in.readLong();
        }
    }
}
