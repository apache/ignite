/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.localtask;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTaskResult;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState.State.COMPLETED;
import static org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState.State.INIT;
import static org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState.State.PREPARE;
import static org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState.State.STARTED;

/**
 * Processor that is responsible for durable background tasks that are executed on local node.
 */
public class DurableBackgroundTasksProcessor extends GridProcessorAdapter implements MetastorageLifecycleListener,
    CheckpointListener {
    /** Prefix for metastorage keys for durable background tasks. */
    private static final String TASK_PREFIX = "durable-background-task-";

    /** MetaStorage synchronization mutex. */
    private final Object metaStorageMux = new Object();

    /** Current tasks. Mapping: {@link DurableBackgroundTask#name task name} -> task state. */
    private final ConcurrentMap<String, DurableBackgroundTaskState> tasks = new ConcurrentHashMap<>();

    /** Lock for canceling tasks. */
    private final ReadWriteLock cancelLock = new ReentrantReadWriteLock(true);

    /**
     * Tasks to be removed from the MetaStorage after the end of a checkpoint.
     * Mapping: {@link DurableBackgroundTask#name task name} -> task.
     */
    private final ConcurrentMap<String, DurableBackgroundTask> toRmv = new ConcurrentHashMap<>();

    /** Prohibiting the execution of tasks. */
    private volatile boolean prohibitionExecTasks = true;

    /** Node stop lock. */
    private final GridBusyLock stopLock = new GridBusyLock();

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public DurableBackgroundTasksProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        cancelTasks();

        stopLock.block();
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) {
        if (!stopLock.enterBusy())
            return;

        try {
            metaStorageOperation(metaStorage -> {
                assert metaStorage != null;

                metaStorage.iterate(
                    TASK_PREFIX,
                    (k, v) -> {
                        DurableBackgroundTask t = (DurableBackgroundTask)v;

                        tasks.put(t.name(), new DurableBackgroundTaskState(t, null, true));
                    },
                    true
                );
            });
        }
        finally {
            stopLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) {
        ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).addCheckpointListener(this);
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) {
        /* No op. */
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        for (Iterator<DurableBackgroundTaskState> it = tasks.values().iterator(); it.hasNext(); ) {
            DurableBackgroundTaskState taskState = it.next();

            if (taskState.state() == COMPLETED) {
                assert taskState.saved();

                DurableBackgroundTask t = taskState.task();

                toRmv.put(t.name(), t);

                it.remove();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {
        /* No op. */
    }

    /** {@inheritDoc} */
    @Override public void afterCheckpointEnd(Context ctx) {
        if (!stopLock.enterBusy())
            return;

        try {
            for (Iterator<DurableBackgroundTask> it = toRmv.values().iterator(); it.hasNext(); ) {
                DurableBackgroundTask t = it.next();

                metaStorageOperation(metaStorage -> {
                    if (metaStorage != null) {
                        if (!tasks.containsKey(t.name()))
                            metaStorage.remove(metaStorageKey(t));

                        it.remove();
                    }
                });
            }
        }
        finally {
            stopLock.leaveBusy();
        }
    }

    /**
     * Callback at the start of a global state change.
     *
     * @param msg Message for change cluster global state.
     */
    public void onStateChangeStarted(ChangeGlobalStateMessage msg) {
        if (msg.state() == ClusterState.INACTIVE)
            cancelTasks();
    }

    /**
     * Callback on finish of a global state change.
     *
     * @param msg Finish message for change cluster global state.
     */
    public void onStateChangeFinish(ChangeGlobalStateFinishMessage msg) {
        if (msg.state() != ClusterState.INACTIVE) {
            prohibitionExecTasks = false;

            for (DurableBackgroundTaskState taskState : tasks.values()) {
                if (!prohibitionExecTasks)
                    executeAsync0(taskState.task());
            }
        }
    }

    /**
     * Asynchronous execution of a durable background task.
     *
     * A new task will be added for execution either if there is no task with
     * the same {@link DurableBackgroundTask#name name} or it (previous) will be completed.
     *
     * If the task is required to be completed after restarting the node,
     * then it must be saved to the MetaStorage.
     *
     * If the task is saved to the Metastorage, then it will be deleted from it
     * only after its completion and at the end of the checkpoint. Otherwise, it
     * will be removed as soon as it is completed.
     *
     * @param task Durable background task.
     * @param save Save task to MetaStorage.
     * @return Futures that will complete when the task is completed.
     */
    public IgniteInternalFuture<Void> executeAsync(DurableBackgroundTask task, boolean save) {
        if (!stopLock.enterBusy())
            throw new IgniteException("Node is stopping.");

        try {
            DurableBackgroundTaskState taskState = tasks.compute(task.name(), (taskName, prev) -> {
                if (prev != null && prev.state() != COMPLETED) {
                    throw new IllegalArgumentException("Task is already present and has not been completed: " +
                        taskName);
                }

                return new DurableBackgroundTaskState(task, new GridFutureAdapter<>(), save);
            });

            if (save) {
                metaStorageOperation(metaStorage -> {
                    if (metaStorage != null)
                        metaStorage.write(metaStorageKey(task), task);
                });
            }

            if (!prohibitionExecTasks)
                executeAsync0(task);

            return taskState.outFuture();
        }
        finally {
            stopLock.leaveBusy();
        }
    }

    /**
     * Overloading the {@link #executeAsync(DurableBackgroundTask, boolean)}.
     * If task is applied to persistent cache, saves it to MetaStorage.
     *
     * @param t Durable background task.
     * @param cacheCfg Cache configuration.
     * @return Futures that will complete when the task is completed.
     */
    public IgniteInternalFuture<Void> executeAsync(DurableBackgroundTask t, CacheConfiguration cacheCfg) {
        return executeAsync(t, CU.isPersistentCache(cacheCfg, ctx.config().getDataStorageConfiguration()));
    }

    /**
     * Asynchronous execution of a durable background task.
     *
     * @param t Durable background task.
     */
    private void executeAsync0(DurableBackgroundTask t) {
        cancelLock.readLock().lock();

        try {
            DurableBackgroundTaskState taskState = tasks.get(t.name());

            if (taskState != null && taskState.state(INIT, PREPARE)) {
                if (log.isInfoEnabled())
                    log.info("Executing durable background task: " + t.name());

                t.executeAsync(ctx).listen(f -> {
                    DurableBackgroundTaskResult res = null;

                    try {
                        res = f.get();
                    }
                    catch (Throwable e) {
                        log.error("Task completed with an error: " + t.name(), e);
                    }

                    assert res != null;

                    if (res.error() != null)
                        log.error("Could not execute durable background task: " + t.name(), res.error());

                    if (res.completed()) {
                        if (res.error() == null && log.isInfoEnabled())
                            log.info("Execution of durable background task completed: " + t.name());

                        if (taskState.saved())
                            taskState.state(COMPLETED);
                        else
                            tasks.remove(t.name());

                        GridFutureAdapter<Void> outFut = taskState.outFuture();

                        if (outFut != null)
                            outFut.onDone(res.error());
                    }
                    else {
                        assert res.restart();

                        if (log.isInfoEnabled())
                            log.info("Execution of durable background task will be restarted: " + t.name());

                        taskState.state(INIT);
                    }
                });

                taskState.state(PREPARE, STARTED);
            }
        }
        finally {
            cancelLock.readLock().unlock();
        }
    }

    /**
     * Canceling tasks that are currently being executed.
     * Prohibiting the execution of tasks.
     */
    private void cancelTasks() {
        prohibitionExecTasks = true;

        cancelLock.writeLock().lock();

        try {
            for (DurableBackgroundTaskState taskState : tasks.values()) {
                if (taskState.state() == STARTED)
                    taskState.task().cancel();
            }
        }
        finally {
            cancelLock.writeLock().unlock();
        }
    }

    /**
     * Performing an operation on a {@link MetaStorage}.
     * Guarded by {@link #metaStorageMux}.
     *
     * @param consumer MetaStorage operation, argument can be {@code null}.
     * @throws IgniteException If an exception is thrown from the {@code consumer}.
     */
    private void metaStorageOperation(IgniteThrowableConsumer<MetaStorage> consumer) throws IgniteException {
        synchronized (metaStorageMux) {
            IgniteCacheDatabaseSharedManager dbMgr = ctx.cache().context().database();

            dbMgr.checkpointReadLock();

            try {
                MetaStorage metaStorage = dbMgr.metaStorage();

                consumer.accept(metaStorage);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                dbMgr.checkpointReadUnlock();
            }
        }
    }

    /**
     * Getting the task key for the MetaStorage.
     *
     * @param t Durable background task.
     * @return MetaStorage {@code t} key.
     */
    static String metaStorageKey(DurableBackgroundTask t) {
        return TASK_PREFIX + t.name();
    }
}
