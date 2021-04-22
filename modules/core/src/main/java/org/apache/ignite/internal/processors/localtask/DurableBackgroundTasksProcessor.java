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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 * Processor that is responsible for durable background tasks that are executed on local node
 * and should be continued even after node restart.
 */
public class DurableBackgroundTasksProcessor extends GridProcessorAdapter implements MetastorageLifecycleListener,
    CheckpointListener {
    /** Prefix for metastorage keys for durable background tasks. */
    private static final String TASK_PREFIX = "durable-background-task-";

    /** MetaStorage synchronization mutex. */
    private final Object metaStorageMux = new Object();

    /** Current tasks. Mapping: {@link DurableBackgroundTask#shortName() short name} -> task. */
    private final Map<String, DurableBackgroundTask> tasks = new ConcurrentHashMap<>();

    /** Completed tasks. */
    private final Collection<DurableBackgroundTask> completed = new ConcurrentLinkedQueue<>();

    /** Tasks to be removed from the MetaStorage. */
    private final Collection<DurableBackgroundTask> toRmv = new ConcurrentLinkedQueue<>();

    /** Prohibiting the execution of tasks. */
    private volatile boolean prohibitionExecTasks = true;

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
        prohibitionExecTasks = true;

        cancelTasks();
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) {
        metaStorageOperation(metaStorage -> {
            assert metaStorage != null;

            metaStorage.iterate(
                TASK_PREFIX,
                (k, v) -> {
                    DurableBackgroundTask t = (DurableBackgroundTask)v;

                    tasks.put(t.shortName(), t);
                },
                true
            );
        });
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
        for (Iterator<DurableBackgroundTask> it = completed.iterator(); it.hasNext(); ) {
            DurableBackgroundTask t = it.next();

            assert t.completed();

            toRmv.add(t);

            it.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {
        /* No op. */
    }

    /** {@inheritDoc} */
    @Override public void afterCheckpointEnd(Context ctx) {
        for (Iterator<DurableBackgroundTask> it = toRmv.iterator(); it.hasNext(); ) {
            DurableBackgroundTask t = it.next();

            assert t.completed();

            metaStorageOperation(metaStorage -> {
                if (metaStorage != null)
                    metaStorage.remove(metaStorageKey(t));
            });

            it.remove();
        }
    }

    /**
     * Callback at the start of a global state change.
     *
     * @param msg Message for change cluster global state.
     */
    public void onStateChangeStarted(ChangeGlobalStateMessage msg) {
        if (msg.state() == ClusterState.INACTIVE) {
            prohibitionExecTasks = true;

            cancelTasks();
        }
    }

    /**
     * Callback on finish of a global state change.
     *
     * @param msg Finish message for change cluster global state.
     */
    public void onStateChangeFinish(ChangeGlobalStateFinishMessage msg) {
        if (msg.state() != ClusterState.INACTIVE) {
            prohibitionExecTasks = false;

            for (DurableBackgroundTask t : tasks.values()) {
                if (!t.started() && !t.completed())
                    executeAsync(t);
            }
        }
    }

    /**
     * Asynchronous execution of a durable background task.
     *
     * @param t Durable background task.
     * @param save Saving the task to the MetaStorage.
     * @return Future that completes when a task is completed.
     */
    public IgniteInternalFuture<?> executeAsync(DurableBackgroundTask t, boolean save) {
        assert !t.completed();
        assert !t.started();

        if (prohibitionExecTasks)
            throw new IgniteException("Can not perform the operation because the cluster is inactive.");

        DurableBackgroundTask prevTask = tasks.put(t.shortName(), t);

        assert prevTask == null;

        if (save) {
            metaStorageOperation(metaStorage -> {
                if (metaStorage != null)
                    metaStorage.write(metaStorageKey(t), t);
            });
        }

        return executeAsync(t);
    }

    /**
     * Asynchronous execution of a durable background task.
     * If task is applied to persistent cache, saves it to MetaStorage.
     *
     * @param t Durable background task.
     * @param cacheCfg Cache configuration.
     * @return Future that completes when a task is completed.
     */
    public IgniteInternalFuture<?> executeAsync(DurableBackgroundTask t, CacheConfiguration cacheCfg) {
        return executeAsync(t, CU.isPersistentCache(cacheCfg, ctx.config().getDataStorageConfiguration()));
    }

    /**
     * Asynchronous execution of a durable background task.
     *
     * @param t Durable background task.
     * @return Future that completes when a task is completed.
     */
    private IgniteInternalFuture<?> executeAsync(DurableBackgroundTask t) {
        assert !t.started();
        assert !t.completed();
        assert !prohibitionExecTasks;

        GridFutureAdapter<?> outFut = new GridFutureAdapter<>();

        if (log.isInfoEnabled())
            log.info("Executing durable background task: " + t.shortName());

        t.executeAsync(ctx).listen(f -> {
            Throwable err = f.error();

            if (err != null)
                log.error("Could not execute durable background task: " + t.shortName(), err);
            else {
                if (log.isInfoEnabled())
                    log.info("Execution of durable background task completed: " + t.shortName());
            }

            if (t.completed()) {
                tasks.remove(t.shortName());

                completed.add(t);
            }

            outFut.onDone(err);
        });

        return outFut;
    }

    /**
     * Cancellation of current {@link #tasks}.
     */
    private void cancelTasks() {
        for (DurableBackgroundTask t : tasks.values()) {
            if (!t.completed())
                t.cancel();
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
    private String metaStorageKey(DurableBackgroundTask t) {
        return TASK_PREFIX + t.shortName();
    }
}
