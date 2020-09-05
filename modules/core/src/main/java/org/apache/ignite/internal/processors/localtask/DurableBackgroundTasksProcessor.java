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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.client.util.GridConcurrentHashSet;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageTree;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.internal.util.IgniteUtils.awaitForWorkersStop;

/**
 * Processor that is responsible for durable background tasks that are executed on local node
 * and should be continued even after node restart.
 */
public class DurableBackgroundTasksProcessor extends GridProcessorAdapter implements MetastorageLifecycleListener,
        DbCheckpointListener {
    /** Prefix for metastorage keys for durable background tasks. */
    private static final String STORE_DURABLE_BACKGROUND_TASK_PREFIX = "durable-background-task-";

    /** Metastorage. */
    private volatile ReadWriteMetastorage metastorage;

    /** Metastorage synchronization mutex. */
    private final Object metaStorageMux = new Object();

    /** Set of workers that executing durable background tasks. */
    private final Set<GridWorker> asyncDurableBackgroundTaskWorkers = new GridConcurrentHashSet<>();

    /** Count of workers that executing durable background tasks. */
    private final AtomicInteger asyncDurableBackgroundTasksWorkersCntr = new AtomicInteger(0);

    /** Durable background tasks map. */
    private final ConcurrentHashMap<String, DurableBackgroundTask> durableBackgroundTasks = new ConcurrentHashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public DurableBackgroundTasksProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Starts the asynchronous operation of pending tasks execution. Is called on start.
     */
    private void asyncDurableBackgroundTasksExecution() {
        assert durableBackgroundTasks != null;

        for (DurableBackgroundTask task : durableBackgroundTasks.values()) {
            if (!task.isCompleted())
                asyncDurableBackgroundTaskExecute(task, false);
        }
    }

    /**
     * Creates a worker to execute single durable background task.
     * @param task Task.
     * @param dropTaskIfFailed Whether to delete task from metastorage, if it has failed.
     */
    private void asyncDurableBackgroundTaskExecute(DurableBackgroundTask task, boolean dropTaskIfFailed) {
        String workerName = "async-durable-background-task-executor-" + asyncDurableBackgroundTasksWorkersCntr.getAndIncrement();

        GridWorker worker = new GridWorker(ctx.igniteInstanceName(), workerName, log) {
            @Override protected void body() {
                try {
                    log.info("Executing durable background task: " + task.shortName());

                    task.execute(ctx);

                    task.complete();

                    log.info("Execution of durable background task completed: " + task.shortName());
                }
                catch (Throwable e) {
                    log.error("Could not execute durable background task: " + task.shortName(), e);

                    if (dropTaskIfFailed)
                        removeDurableBackgroundTask(task);
                }
                finally {
                    asyncDurableBackgroundTaskWorkers.remove(this);
                }
            }
        };

        asyncDurableBackgroundTaskWorkers.add(worker);

        Thread asyncTask = new IgniteThread(worker);

        asyncTask.start();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) {
        asyncDurableBackgroundTasksExecution();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        // Waiting for workers, but not cancelling them, trying to complete running tasks.
        awaitForWorkersStop(asyncDurableBackgroundTaskWorkers, false, log);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
    }

    /**
     * @param msg Message.
     */
    public void onStateChangeFinish(ChangeGlobalStateFinishMessage msg) {
        if (!msg.clusterActive())
            awaitForWorkersStop(asyncDurableBackgroundTaskWorkers, true, log);
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) {
        synchronized (metaStorageMux) {
            if (durableBackgroundTasks.isEmpty()) {
                try {
                    metastorage.iterate(
                        STORE_DURABLE_BACKGROUND_TASK_PREFIX,
                        (key, val) -> durableBackgroundTasks.put(key, (DurableBackgroundTask)val),
                        true
                    );
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to iterate durable background tasks storage.", e);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) {
        synchronized (metaStorageMux) {
            try {
                for (Map.Entry<String, DurableBackgroundTask> entry : durableBackgroundTasks.entrySet()) {
                    if (metastorage.readRaw(entry.getKey()) == null)
                        metastorage.write(entry.getKey(), entry.getValue());
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to read key from durable background tasks storage.", e);
            }
        }

        ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).addCheckpointListener(this);

        this.metastorage = metastorage;
    }

    /**
     * Builds a metastorage key for durable background task object.
     *
     * @param obj Object.
     * @return Metastorage key.
     */
    private String durableBackgroundTaskMetastorageKey(DurableBackgroundTask obj) {
        String k = STORE_DURABLE_BACKGROUND_TASK_PREFIX + obj.shortName();

        if (k.length() > MetastorageTree.MAX_KEY_LEN) {
            int hashLenLimit = 5;

            String hash = String.valueOf(k.hashCode());

            k = k.substring(0, MetastorageTree.MAX_KEY_LEN - hashLenLimit) +
                (hash.length() > hashLenLimit ? hash.substring(0, hashLenLimit) : hash);
        }

        return k;
    }

    /**
     * Adds durable background task object.
     *
     * @param obj Object.
     */
    private void addDurableBackgroundTask(DurableBackgroundTask obj) {
        String objName = durableBackgroundTaskMetastorageKey(obj);

        synchronized (metaStorageMux) {
            durableBackgroundTasks.put(objName, obj);

            if (metastorage != null) {
                ctx.cache().context().database().checkpointReadLock();

                try {
                    metastorage.write(objName, obj);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
                finally {
                    ctx.cache().context().database().checkpointReadUnlock();
                }
            }
        }
    }

    /**
     * Removes durable background task object.
     *
     * @param obj Object.
     */
    private void removeDurableBackgroundTask(DurableBackgroundTask obj) {
        String objName = durableBackgroundTaskMetastorageKey(obj);

        synchronized (metaStorageMux) {
            durableBackgroundTasks.remove(objName);

            if (metastorage != null) {
                ctx.cache().context().database().checkpointReadLock();

                try {
                    metastorage.remove(objName);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
                finally {
                    ctx.cache().context().database().checkpointReadUnlock();
                }
            }
        }
    }

    /**
     * Starts durable background task. If task is applied to persistent cache, saves it to metastorage.
     *
     * @param task Continuous task.
     * @param ccfg Cache configuration.
     */
    public void startDurableBackgroundTask(DurableBackgroundTask task, CacheConfiguration ccfg) {
        if (CU.isPersistentCache(ccfg, ctx.config().getDataStorageConfiguration()))
            addDurableBackgroundTask(task);

        asyncDurableBackgroundTaskExecute(task, false);
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        for (DurableBackgroundTask task : durableBackgroundTasks.values()) {
            if (task.isCompleted())
                removeDurableBackgroundTask(task);
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {
        /* No op. */
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) {
        /* No op. */
    }
}
