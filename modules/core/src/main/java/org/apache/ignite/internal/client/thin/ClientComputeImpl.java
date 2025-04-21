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

package org.apache.ignite.internal.client.thin;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientCompute;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientFeatureNotSupportedByServerException;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.streams.BinaryStreams;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.client.thin.ClientNotificationType.COMPUTE_TASK_FINISHED;
import static org.apache.ignite.internal.client.thin.ClientOperation.COMPUTE_TASK_EXECUTE;
import static org.apache.ignite.internal.client.thin.ClientOperation.RESOURCE_CLOSE;

/**
 * Implementation of {@link ClientCompute}.
 */
class ClientComputeImpl implements ClientCompute {
    /** No failover flag mask. */
    private static final byte NO_FAILOVER_FLAG_MASK = 0x01;

    /** No result cache flag mask. */
    private static final byte NO_RESULT_CACHE_FLAG_MASK = 0x02;

    /** Channel. */
    private final ReliableChannel ch;

    /** Utils for serialization/deserialization. */
    private final ClientUtils utils;

    /** Default cluster group. */
    private final ClientClusterGroupImpl dfltGrp;

    /** Active tasks count. */
    private final AtomicInteger tasksCnt = new AtomicInteger();

    /** Constructor. */
    ClientComputeImpl(ReliableChannel ch, ClientBinaryMarshaller marsh, ClientClusterGroupImpl dfltGrp) {
        this.ch = ch;
        this.dfltGrp = dfltGrp;

        utils = new ClientUtils(marsh);
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup clusterGroup() {
        return dfltGrp;
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(String taskName, @Nullable T arg) throws ClientException, InterruptedException {
        return execute0(taskName, arg, dfltGrp, (byte)0, 0L);
    }

    /** {@inheritDoc} */
    @Override public <T, R> Future<R> executeAsync(String taskName, @Nullable T arg) throws ClientException {
        return executeAsync0(taskName, arg, dfltGrp, (byte)0, 0L);
    }

    /** {@inheritDoc} */
    @Override public <T, R> IgniteClientFuture<R> executeAsync2(String taskName, @Nullable T arg) throws ClientException {
        return executeAsync0(taskName, arg, dfltGrp, (byte)0, 0L);
    }

    /** {@inheritDoc} */
    @Override public ClientCompute withTimeout(long timeout) {
        return timeout == 0L ? this : new ClientComputeModificator(this, dfltGrp, (byte)0, timeout);
    }

    /** {@inheritDoc} */
    @Override public ClientCompute withNoFailover() {
        return new ClientComputeModificator(this, dfltGrp, NO_FAILOVER_FLAG_MASK, 0L);
    }

    /** {@inheritDoc} */
    @Override public ClientCompute withNoResultCache() {
        return new ClientComputeModificator(this, dfltGrp, NO_RESULT_CACHE_FLAG_MASK, 0L);
    }

    /**
     * Gets compute facade over the specified cluster group.
     *
     * @param grp Cluster group.
     */
    public ClientCompute withClusterGroup(ClientClusterGroupImpl grp) {
        return new ClientComputeModificator(this, grp, (byte)0, 0L);
    }

    /**
     * @param taskName Task name.
     * @param arg Argument.
     * @param clusterGrp Cluster group.
     * @param flags Flags.
     * @param timeout Timeout.
     */
    private <T, R> R execute0(
        String taskName,
        @Nullable T arg,
        ClientClusterGroupImpl clusterGrp,
        byte flags,
        long timeout
    ) throws ClientException {
        try {
            return (R)executeAsync0(taskName, arg, clusterGrp, flags, timeout).get();
        }
        catch (ExecutionException | InterruptedException e) {
            throw convertException(e);
        }
    }

    /**
     * Converts the exception.
     *
     * @param t Throwable.
     * @return Resulting client exception.
     */
    private ClientException convertException(Throwable t) {
        if (t instanceof ClientException)
            return (ClientException)t;
        else if (t.getCause() instanceof ClientException)
            return (ClientException)t.getCause();
        else
            return new ClientException(t);
    }

    /**
     * @param taskName Task name.
     * @param arg Argument.
     * @param clusterGrp Cluster group.
     * @param flags Flags.
     * @param timeout Timeout.
     */
    private <T, R> IgniteClientFuture<R> executeAsync0(
        String taskName,
        @Nullable T arg,
        ClientClusterGroupImpl clusterGrp,
        byte flags,
        long timeout
    ) throws ClientException {
        Collection<UUID> nodeIds = clusterGrp.nodeIds();

        if (F.isEmpty(taskName))
            throw new ClientException("Task name can't be null or empty.");

        if (nodeIds != null && nodeIds.isEmpty())
            throw new ClientException("Cluster group is empty.");

        Consumer<PayloadOutputChannel> payloadWriter =
                ch -> writeExecuteTaskRequest(ch, taskName, arg, nodeIds, flags, timeout);

        Function<PayloadInputChannel, ClientComputeTask<R>> payloadReader = ch -> {
            Long taskId = ch.in().readLong();

            ClientComputeTask<R> task = new ClientComputeTask<>(utils, ch.clientChannel(), taskId);

            ch.clientChannel().addNotificationListener(COMPUTE_TASK_FINISHED, taskId, task);

            return task;
        };

        IgniteClientFuture<ClientComputeTask<R>> initFut = ch.serviceAsync(
                COMPUTE_TASK_EXECUTE, payloadWriter, payloadReader);

        CompletableFuture<R> resFut = new CompletableFuture<>();
        AtomicReference<Object> cancellationToken = new AtomicReference<>();

        initFut.handle((task, err) -> handleExecuteInitFuture(resFut, cancellationToken, task, err));

        return new IgniteClientFutureImpl<>(resFut, mayInterruptIfRunning -> {
            // 1. initFut has not completed - store cancellation flag.
            // 2. initFut has completed - cancel compute future.
            if (!cancellationToken.compareAndSet(null, mayInterruptIfRunning)) {
                GridFutureAdapter<?> fut = (GridFutureAdapter<?>)cancellationToken.get();

                if (!cancelGridFuture(fut, mayInterruptIfRunning))
                    return false;
            }

            resFut.cancel(mayInterruptIfRunning);

            return true;
        });
    }

    /**
     * Handles execute initialization.
     *
     * @param resFut Resulting future.
     * @param cancellationToken Cancellation token holder.
     * @param task Task.
     * @param err Error
     * @param <R> Result type.
     * @return Null.
     */
    private <R> Object handleExecuteInitFuture(
            CompletableFuture<R> resFut,
            AtomicReference<Object> cancellationToken,
            ClientComputeTask<R> task,
            Throwable err) {
        if (err != null)
            resFut.completeExceptionally(new ClientException(err));
        else {
            if (!cancellationToken.compareAndSet(null, task.fut))
                cancelGridFuture(task.fut, (Boolean)cancellationToken.get());

            tasksCnt.incrementAndGet();

            task.fut.listen(f -> {
                tasksCnt.decrementAndGet();

                if (!f.isCancelled()) {
                    if (f.error() == null)
                        resFut.complete(f.result());
                    else
                        resFut.completeExceptionally(f.error());
                }
            });
        }

        return null;
    }

    /**
     * Cancels grid future.
     *
     * @param fut Future.
     * @param mayInterruptIfRunning true if the thread executing this task should be interrupted;
     *                             otherwise, in-progress tasks are allowed to complete.
     */
    private static boolean cancelGridFuture(GridFutureAdapter<?> fut, Boolean mayInterruptIfRunning) {
        try {
            return mayInterruptIfRunning ? fut.cancel() : fut.onCancelled();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     *
     */
    private <T> void writeExecuteTaskRequest(
        PayloadOutputChannel ch,
        String taskName,
        @Nullable T arg,
        Collection<UUID> nodeIds,
        byte flags,
        long timeout
    ) throws ClientException {
        if (!ch.clientChannel().protocolCtx().isFeatureSupported(ProtocolBitmaskFeature.EXECUTE_TASK_BY_NAME)) {
            throw new ClientFeatureNotSupportedByServerException("Compute grid functionality for thin " +
                "client not supported by server node (" + ch.clientChannel().serverNodeId() + ')');
        }

        try (BinaryRawWriterEx w = utils.createBinaryWriter(ch.out())) {
            if (nodeIds == null) // Include all nodes.
                w.writeInt(0);
            else {
                w.writeInt(nodeIds.size());

                for (UUID nodeId : nodeIds) {
                    w.writeLong(nodeId.getMostSignificantBits());
                    w.writeLong(nodeId.getLeastSignificantBits());
                }
            }

            w.writeByte(flags);
            w.writeLong(timeout);
            w.writeString(taskName);
            w.writeObject(arg);
        }
    }

    /**
     * Gets count of active tasks started by client.
     * Used only for tests.
     */
    int activeTasksCount() {
        return tasksCnt.get();
    }

    /**
     * ClientCompute with modificators.
     */
    private static class ClientComputeModificator implements ClientCompute {
        /** Delegate. */
        private final ClientComputeImpl delegate;

        /** Cluster group. */
        private final ClientClusterGroupImpl clusterGrp;

        /** Task flags. */
        private final byte flags;

        /** Task timeout. */
        private final long timeout;

        /**
         * Constructor.
         */
        private ClientComputeModificator(ClientComputeImpl delegate, ClientClusterGroupImpl clusterGrp, byte flags,
            long timeout) {
            this.delegate = delegate;
            this.clusterGrp = clusterGrp;
            this.flags = flags;
            this.timeout = timeout;
        }

        /** {@inheritDoc} */
        @Override public ClientClusterGroup clusterGroup() {
            return clusterGrp;
        }

        /** {@inheritDoc} */
        @Override public <T, R> R execute(String taskName, @Nullable T arg) throws ClientException, InterruptedException {
            return delegate.execute0(taskName, arg, clusterGrp, flags, timeout);
        }

        /** {@inheritDoc} */
        @Override public <T, R> IgniteClientFuture<R> executeAsync(String taskName, @Nullable T arg) throws ClientException {
            return delegate.executeAsync0(taskName, arg, clusterGrp, flags, timeout);
        }

        /** {@inheritDoc} */
        @Override public <T, R> IgniteClientFuture<R> executeAsync2(String taskName, @Nullable T arg) throws ClientException {
            return delegate.executeAsync0(taskName, arg, clusterGrp, flags, timeout);
        }

        /** {@inheritDoc} */
        @Override public ClientCompute withTimeout(long timeout) {
            return timeout == this.timeout ? this : new ClientComputeModificator(delegate, clusterGrp, flags, timeout);
        }

        /** {@inheritDoc} */
        @Override public ClientCompute withNoFailover() {
            return (flags & NO_FAILOVER_FLAG_MASK) != 0 ? this :
                new ClientComputeModificator(delegate, clusterGrp, (byte)(flags | NO_FAILOVER_FLAG_MASK), timeout);
        }

        /** {@inheritDoc} */
        @Override public ClientCompute withNoResultCache() {
            return (flags & NO_RESULT_CACHE_FLAG_MASK) != 0 ? this :
                new ClientComputeModificator(delegate, clusterGrp, (byte)(flags | NO_RESULT_CACHE_FLAG_MASK), timeout);
        }
    }

    /**
     * Compute task internal class.
     *
     * @param <R> Result type.
     */
    private static class ClientComputeTask<R> implements NotificationListener {
        /** Client channel. */
        private final ClientChannel ch;

        /** Task id. */
        private final long taskId;

        /** Future. */
        private final GridFutureAdapter<R> fut;

        /** */
        private final ClientUtils utils;

        /**
         * @param ch Client channel.
         * @param taskId Task id.
         */
        private ClientComputeTask(ClientUtils utils, ClientChannel ch, Long taskId) {
            this.utils = utils;
            this.ch = ch;
            this.taskId = taskId;

            fut = new GridFutureAdapter<R>() {
                @Override public boolean cancel() {
                    if (onCancelled()) {
                        try {
                            ch.service(RESOURCE_CLOSE, req -> req.out().writeLong(taskId), null);
                        }
                        catch (ClientServerError e) {
                            // Ignore "resource doesn't exist" error. The task can be completed concurrently on the
                            // server, but we already complete future with "cancelled" state, so result will never be
                            // received by a client.
                            if (e.getCode() != ClientStatus.RESOURCE_DOES_NOT_EXIST)
                                throw new ClientException(e);
                        }

                        return true;
                    }
                    else
                        return false;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public void acceptNotification(ByteBuffer payload, Exception err) {
            if (err == null) {
                try {
                    R res = payload == null ? null :
                        utils.readObject(BinaryStreams.createInputStream(payload), false);

                    fut.onDone(res);
                }
                catch (Throwable e) {
                    fut.onDone(e);
                }
            }
            else
                fut.onDone(err);

            ch.removeNotificationListener(COMPUTE_TASK_FINISHED, taskId);
        }

        /** {@inheritDoc} */
        @Override public void onChannelClosed(Exception reason) {
            fut.onDone(new ClientConnectionException("Connection to server is closed", reason));
        }
    }
}
