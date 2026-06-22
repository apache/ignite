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

package org.apache.ignite.internal.classpath;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.NEW;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.READY;

/**
 * TODO:
 * 1. How to check data integrity on start?
 * Do we want to do this for txt file or for jar only?
 * 2. Check and remove obsolete icp from dist on start.
 * Do we want to have some flag to skip remove in this case? (if we preparing for ICP registration).
 * 3. Should we include CP into snapshots and dumps?
 * 4. Should we control file size to ensure disk free space enough to upload CP files?
 *
 // TODO: remove from deployedNodes on OnNodeLeft event.
 */
public class ClassPathProcessor extends GridProcessorAdapter implements DistributedMetastorageLifecycleListener {
    /** Prefix for metastorage keys. */
    static final String METASTORE_PREFIX = "icp.";

    /** Handles download requests for {@link IgniteClassPath} files. */
    final ClassPathFilesTransmissionHandler icpFilesHnd;

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;

    /** */
    private final ConcurrentMap<UUID, Queue<ClassPathTask<?>>> icpTasks = new ConcurrentHashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public ClassPathProcessor(GridKernalContext ctx) {
        super(ctx);

        icpFilesHnd = new ClassPathFilesTransmissionHandler(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        icpFilesHnd.start();

        synchronized (this) {
            ctx.event().addDiscoveryEventListener(discoLsnr = (evt, discoCache) -> {
                UUID leftNodeId = evt.eventNode().id();

                try {
                    ctx.distributedMetastorage().iterate(METASTORE_PREFIX, (key, val) -> {
                        if (!isClassPath(val)) {
                            log.warning("Wrong data in IgniteClassPath metastorage data [key=" + key + ", val=" + (val) + ']');

                            return;
                        }

                        IgniteClassPath icp = (IgniteClassPath)val;

                        addClassPathTask(icp, new RemoveNodeFromClassPathTask(ctx, icp.id(), leftNodeId));
                    });
                }
                catch (IgniteCheckedException e) {
                    log.warning("Distributed metastore iteration error", e);
                }

            }, EVT_NODE_LEFT, EVT_NODE_FAILED);
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        super.onKernalStart(active);

        ctx.distributedMetastorage().listen(key -> key.startsWith(METASTORE_PREFIX), new ClassPathChangeListener(ctx));
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        icpFilesHnd.stop();

        synchronized (this) {
            if (discoLsnr != null)
                ctx.event().removeDiscoveryEventListener(discoLsnr);
        }
    }

    /**
     * Register new classpath in metastorage it same name not exists.
     * Fails if exists.
     *
     * @param name Class path name.
     * @param files Files included.
     * @param lengths Files lengths.
     * @return Class path id.
     */
    public UUID startCreation(String name, String[] files, long[] lengths) throws IgniteCheckedException {
        A.ensure(files.length == lengths.length, "wrong arrays lengths");
        A.ensure(U.alphanumericUnderscore(name), "Classpath name must satisfy the following name pattern: a-zA-Z0-9_");

        for (String file : files)
            ensureFilename(file);

        IgniteClassPath icp = new IgniteClassPath(UUID.randomUUID(), Set.of(ctx.localNodeId()), name, files, lengths, NEW);

        File root = ctx.pdsFolderResolver().fileTree().classPathRoot(name);

        createRootAndCheckIsEmpty(root);

        Boolean metastorageWritten = casToMetastorageAsync(null, icp).get();

        if (metastorageWritten != null && !metastorageWritten)
            throw new IgniteException("Fail to register ClassPath. Same ClassPath exists, already?");

        log.info("New IgniteClasspath created [root = " + root + ", icp=" + icp + ']');

        return icp.id();
    }

    /**
     * Writes {@code batch} to the Ignite Class Path file.
     *
     * @param icpId ClassPath id.
     * @param name File name.
     * @param offset Offset to write data to.
     * @param batch Batch.
     */
    public synchronized void writeFilePartFromClient(
        UUID icpId,
        String name,
        long offset,
        byte[] batch
    ) {
        IgniteClassPath icp = fromMetastorage(icpId, NEW, ctx);

        try {
            ensureKnownFilename(name, icp);

            File root = ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name());

            File f = new File(root, name);

            if (offset == 0) {
                A.ensure(root.equals(f.getParentFile()), "filename");

                log.info("Creating new classpath file: " + f);

                if (!f.createNewFile())
                    throw new IgniteException("File exists: " + f);
            }

            try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
                if (raf.length() < offset) {
                    throw new IgniteException("Wrong offset [icp=" + icp.name() + ", lib=" + name + ", " +
                        "fileLength=" + raf.length() + ", offset=" + offset + ']');
                }

                raf.seek(offset);
                raf.write(batch);
            }
        }
        catch (Throwable e) {
            log.error("Failed to upload ClassPath file, the ClassPath will be removed " +
                "[name=" + icp.name() + ", id=" + icpId + ", file=" + name + ']', e);

            cleanup(icp, false);

            throw new IgniteException(e);
        }
    }

    /**
     * Copies local file to class path directory.
     *
     * @param icpId ClassPath id.
     * @param file File to copy.
     */
    public void copyClassPathFileLocally(UUID icpId, Path file) {
        IgniteClassPath icp = fromMetastorage(icpId, NEW, ctx);

        try {
            String name = file.getFileName().toString();

            ensureKnownFilename(name, icp);

            File root = ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name());

            Path f = new File(root, name).toPath();

            if (Files.exists(f)) {
                if (Files.isSameFile(file, f)) {
                    log.info("Skip copying new classpath file, already there: " + f);

                    return;
                }

                throw new IgniteException("File exists: " + f);
            }

            A.ensure(root.equals(f.toFile().getParentFile()), "filename");

            log.info("Copying new classpath file: " + f);

            Files.copy(file, f);
        }
        catch (Throwable e) {
            log.error("Failed to copy ClassPath file locally, the ClassPath will be removed " +
                "[name=" + icp.name() + ", id=" + icpId + ']', e);

            cleanup(icp, false);

            throw new IgniteException(e);
        }
    }

    /** */
    public void makeReady(UUID icpId) throws IgniteCheckedException {
        IgniteClassPath icp = fromMetastorage(icpId, NEW, ctx);

        ChangeClassPathStateTask task = new ChangeClassPathStateTask(ctx, icpId, READY);

        addClassPathTask(icp, task);

        task.result().get();
    }

    /** */
    <R> void addClassPathTask(IgniteClassPath icp, ClassPathTask<R> task) {
        UUID icpId = icp.id();

        task.result().listen(doneFut -> {
            if (task.result().error() == null)
                log.info("IgniteClassPath task done [task=" + task.name() + ", icp=" + icp + ", res=" + task.result().result() + ']');
            else
                log.warning("IgniteClassPath task failure [task=" + task.name() + ", icp=" + icp + ']', task.result().error());

            icpTasks.compute(icpId, (ignored, tasks) -> {
                if (tasks == null)
                    tasks = new ConcurrentLinkedQueue<>();

                if (tasks.peek().result() == doneFut) {
                    tasks.poll();

                    startAsync(tasks.peek());
                }
                else
                    tasks.removeIf(t -> t == task);

                return tasks;
            });
        });

        icpTasks.compute(icpId, (ignored, tasks) -> {
            if (tasks == null)
                tasks = new ConcurrentLinkedQueue<>();

            while (!F.isEmpty(tasks) && tasks.peek().result().isDone())
                tasks.poll();

            boolean isFirst = F.isEmpty(tasks);

            tasks.add(task);

            if (isFirst)
                startAsync(task);

            return tasks;
        });
    }

    /** */
    private <R> void startAsync(ClassPathTask<R> t) {
        // TODO: check is stopped.
        // TODO: async task to invoke start required?
        try {
            ctx.pools().getSystemExecutorService().submit(() -> {
                try {
                    t.start();
                }
                catch (Throwable e) {
                    t.result().onDone(e);
                }
            });
        }
        catch (RejectedExecutionException e) {
            t.result().onDone(e);
        }
    }

    /** */
    GridFutureAdapter<Boolean> casToMetastorageAsync(@Nullable IgniteClassPath prev, IgniteClassPath icp) {
        try {
            String key = metastorageKey(icp.name());

            if (log.isDebugEnabled())
                log.debug("Writing new ClassPath state [new=" + icp + ", prev=" + prev + ']');

            GridFutureAdapter<Boolean> res = ctx.distributedMetastorage().compareAndSetAsync(key, prev, icp);

            res.listen(casFut -> {
                if (casFut.error() == null)
                    return;

                try {
                    Object val = ctx.distributedMetastorage().read(key);

                    log.warning("Fail to write new ClassPath state [exp=" + prev + ", actual=" + val + ']');
                }
                catch (IgniteCheckedException e) {
                    log.warning("Can't read metastore key", e);
                }
            });

            return res;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Modifies {@link IgniteClassPath} in metastorage with thre provided {@code change}.
     * Retries modification if {@link #casToMetastorageAsync(IgniteClassPath, IgniteClassPath)} failed.
     * If {@code change} returns same {@link IgniteClassPath} no updates will be done.
     * If {@code change} or read from metastorge throws then operation will finish with the corresponding exception.
     */
    public GridFutureAdapter<Void> modifyInMetastorageAsync(
        UUID icpId,
        @Nullable IgniteClassPathState state,
        Function<IgniteClassPath, IgniteClassPath> change
    ) {
        GridFutureAdapter<Void> res = new GridFutureAdapter<>();

        modifyWithRetriesAsync(icpId, state, change, res, 100);

        return res;
    }

    /** */
    private void modifyWithRetriesAsync(
        UUID icpId,
        @Nullable IgniteClassPathState state,
        Function<IgniteClassPath, IgniteClassPath> change,
        GridFutureAdapter<Void> res,
        int hardLimit
    ) {
        if (hardLimit == 0) {
            log.error("Reached hard limit on CAS attempts. Fail operation [icpId=" + icpId + ", state=" + state + ']');

            res.onDone(new IgniteException("Hard limit of CAS reached"));
        }

        try {
            IgniteClassPath prev = fromMetastorage(icpId, state, ctx);
            IgniteClassPath icp = change.apply(prev);

            if (Objects.equals(prev, icp)) {
                res.onDone();

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Trying to CAS [prev=" + prev + ", new=" + icp + ", hardLmit=" + hardLimit + ']');

            casToMetastorageAsync(prev, icp).listen(casRes -> {
                if (casRes.error() != null) {
                    res.onDone(casRes.error());

                    return;
                }

                boolean metastorageWritten = casRes.result() != null && casRes.result();

                if (metastorageWritten) {
                    res.onDone((Void)null);

                    return;
                }

                // Concurrent node modifies first? It's OK, trying to repeat.
                modifyWithRetriesAsync(icpId, state, change, res, hardLimit - 1);

            });
        }
        catch (Exception e) {
            res.onDone(e);
        }
    }

    /**
     * @param icpId ClassPath id.
     * @param ctx Kernal context.
     * @return Class path.
     */
    static IgniteClassPath fromMetastorage(UUID icpId, @Nullable IgniteClassPathState expState, GridKernalContext ctx) {
        try {
            IgniteClassPath[] icp = new IgniteClassPath[1];

            ctx.distributedMetastorage().iterate(METASTORE_PREFIX, (key, icp0) -> {
                if (icpId.equals(((IgniteClassPath)icp0).id()))
                    icp[0] = (IgniteClassPath)icp0;
            });

            if (icp[0] == null)
                throw new IgniteException("ClassPath not found: " + icpId);

            if (expState != null && icp[0].state() != expState)
                throw new IgniteException("ClassPath in wrong state [expected=" + expState + ", status=" + icp[0].state() + ']');

            return icp[0];
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public static String metastorageKey(String name) {
        return METASTORE_PREFIX + name;
    }

    /** */
    private static void ensureFilename(String file) {
        Path path = Path.of(file);

        A.ensure(path.getNameCount() == 1 && !path.isAbsolute(), "simple filename expected");
    }

    /** */
    private static void ensureKnownFilename(String name, IgniteClassPath icp) {
        ensureFilename(name);

        if (F.indexOf(icp.files(), name) == -1)
            throw new IllegalArgumentException("Unknown lib [icp=" + icp.name() + ", unknown_lib=" + name + ']');
    }

    /** */
    static void createRootAndCheckIsEmpty(File root) {
        if (!root.exists())
            NodeFileTree.mkdir(root, "Ignite Class Path root");
        else if (!F.isEmpty(root.listFiles()))
            throw new IgniteException("ClassPath root exists and not empty: " + root);
    }

    /**
     * Asynchronously removes the classpath metastorage record and deletes its local files.
     *
     * @param icp ClassPath to clean up.
     * @param loc If {@code true} then delete local files, only. Don't remove distributed key.
     */
    IgniteInternalFuture<Void> cleanupAsync(IgniteClassPath icp, boolean loc) {
        try {
            GridFutureAdapter<Void> res = new GridFutureAdapter<>();

            ctx.pools().getSystemExecutorService().submit(() -> {
                try {
                    cleanup(icp, loc);

                    res.onDone((Void)null);
                }
                catch (Throwable e) {
                    res.onDone(e);
                }
            });

            if (log.isDebugEnabled()) {
                res.listen(f -> {
                    if (log.isDebugEnabled())
                        log.debug("Async cleanup done. " + (res.error() == null ? "OK" : "FAIL"));
                });
            }

            return res;
        }
        catch (RejectedExecutionException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * Removes the classpath metastorage record and deletes its local files.
     *
     * @param icp ClassPath to clean up.
     * @param loc If {@code true} then delete local files, only. Don't remove distributed key.
     */
    void cleanup(IgniteClassPath icp, boolean loc) {
        if (!loc) {
            try {
                ctx.distributedMetastorage().remove(metastorageKey(icp.name()));
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to remove ClassPath metastorage record [name=" + icp.name() + ']', e);
            }
        }

        U.delete(ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name()));
    }

    /** */
    public static boolean isClassPath(Serializable icp) {
        return icp == null || icp instanceof IgniteClassPath;
    }

    /** */
    public static String className(@Nullable Serializable oldVal) {
        return oldVal == null ? null : oldVal.getClass().getName();
    }

    /** */
    abstract static class ClassPathTask<R> {
        /** */
        private final GridFutureAdapter<R> taskRes = new GridFutureAdapter<R>();

        /** */
        protected final GridKernalContext ctx;

        /** */
        protected final UUID icpId;

        /** */
        protected final IgniteLogger log;

        /** */
        protected ClassPathTask(GridKernalContext ctx, UUID icpId) {
            this.ctx = ctx;
            this.icpId = icpId;
            this.log = ctx.log(getClass());

            result().listen(resFut -> {
                if (resFut.error() == null)
                    ok();
                else
                    fail(resFut.error());
            });

        }

        /** */
        void finishTaskWithFutureResult(IgniteInternalFuture<?> action) {
            if (action.error() == null)
                result().onDone();
            else
                result().onDone(action.error());
        }

        /**
         * Starts task execution.
         * Must be nonblocking and fast.
         * In case any exception throwed, {@link #result()} will be finished and task treated as done.
         */
        abstract void start() throws Exception;

        /**
         * @return Task name.
         */
        abstract String name();

        /**
         * @return Task results.
         */
        GridFutureAdapter<R> result() {
            return taskRes;
        }

        /** */
        abstract void ok();

        /** */
        abstract void fail(Throwable t);
    }
}
