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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
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
import static org.apache.ignite.internal.classpath.ChangeNodesTask.addNode;
import static org.apache.ignite.internal.classpath.ChangeNodesTask.removeNode;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.NEW;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.READY;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.REMOVING;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.TMP_SUFFIX;

/**
 * TODO:
 * 1. How to check data integrity on start?
 * Do we want to do this for txt file or for jar only?
 * 2. Check and remove obsolete icp from dist on start.
 * Do we want to have some flag to skip remove in this case? (if we preparing for ICP registration).
 * 3. Should we include CP into snapshots and dumps?
 * 4. Should we control file size to ensure disk free space enough to upload CP files?
 * TODO: update local file on changes.
 */
public class ClassPathProcessor extends GridProcessorAdapter implements DistributedMetastorageLifecycleListener {
    /** */
    private static final Queue<ClassPathTask<?>> NO_NEW_TASKS = new ConcurrentLinkedQueue<>();

    /** Prefix for metastorage keys. */
    static final String METASTORE_PREFIX = "icp.";

    /** Handles download requests for {@link IgniteClassPath} files. */
    final ClassPathFilesTransmissionHandler icpFilesHnd;

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;

    /**
     * {@link IgniteClassPath} tasks.
     * Any actions with the specific {@link IgniteClassPath} instance must be done one by one to ensure local atomicity of changes.
     */
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
                    ctx.pools().getSystemExecutorService().submit(() -> {
                        try {
                            iterateMetastorage(
                                (key, icp) -> addClassPathTask(icp, removeNode(ctx, icp.id(), leftNodeId))
                            );
                        }
                        catch (IgniteCheckedException e) {
                            log.warning("Distributed metastore iteration error", e);
                        }
                    });
                }
                catch (RejectedExecutionException e) {
                    log.warning("System pool rejected task", e);
                }
            }, EVT_NODE_LEFT, EVT_NODE_FAILED);
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        super.onKernalStart(active);

        // Removing stale local data:
        // 1. Partially downloaded: descriptor not exists or can't be read.
        // 2. Differs from cluster state: id not equals.
        // 3. Remove in progress: state REMOVING.
        File[] staleRoots = staleLocalRoots();

        if (!F.isEmpty(staleRoots)) {
            for (File cpRoot : staleRoots)
                if (!U.delete(cpRoot))
                    throw new IgniteException("Cant delete stale ClassPath root: " + cpRoot);
        }

        ctx.distributedMetastorage().listen(key -> key.startsWith(METASTORE_PREFIX), new ClassPathChangeListener(ctx));

        iterateMetastorage((key, icp) -> {
            NodeFileTree ft = ctx.pdsFolderResolver().fileTree();

            IgniteClassPath loc = readClassPathDescriptor(ft.classPathDescriptor(icp.name()));

            // Partially downloaded or locally unknown ClassPath.
            if (loc == null) {
                addClassPathTask(icp, CleanupTask.removeFiles(ctx, icp));

                if (icp.state() == READY)
                    addClassPathTask(icp, new DownloadTask(ctx, icp.id()));

                return;
            }

            addClassPathTask(icp, addNode(ctx, icp.id(), ctx.localNodeId()));
        });
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        icpFilesHnd.stop();

        synchronized (this) {
            if (discoLsnr != null)
                ctx.event().removeDiscoveryEventListener(discoLsnr);
        }

        if (cancel) {
            Set<UUID> ids = icpTasks.keySet();

            for (UUID id : ids)
                cancelTasksAndDisallowNew(id, "unknown", false);
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

        createRootAndCheckIsEmpty(icp);

        Boolean metastorageWritten = casToMetastorageAsync(null, icp).get();

        if (metastorageWritten == null || !metastorageWritten) {
            removeClassPathLocally(icp, true);

            throw new IgniteException("Fail to register ClassPath. Same ClassPath exists, already?");
        }

        File cpRoot = ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name());

        log.info("New IgniteClasspath created [root = " + cpRoot + ", icp=" + icp + ']');

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
            ensureKnownFilename(name, icp, () -> offset + batch.length);

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

            // Cleaning up synchronously.
            cleanAll(icp);

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

            ensureKnownFilename(name, icp, () -> {
                try {
                    return Files.size(file);
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }
            });

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

            // Cleaning up synchronously.
            cleanAll(icp);

            throw new IgniteException(e);
        }
    }

    /** */
    private void cleanAll(IgniteClassPath icp) {
        try {
            removeFromMetastorage(icp, () -> false);
        }
        catch (IgniteCheckedException e) {
            log.warning("Remove from mestastorage fail", e);
        }
        finally {
            removeClassPathLocally(icp, true);
        }
    }

    /** */
    public void cleanQueue(UUID icpId) {
        icpTasks.remove(icpId);
    }

    /** */
    public void makeReady(UUID icpId) throws IgniteCheckedException {
        IgniteClassPath icp = fromMetastorage(icpId, NEW, ctx);

        ChangeStateTask task = new ChangeStateTask(ctx, icpId, READY);

        addClassPathTask(icp, task);

        task.result().get();
    }

    /** */
    public IgniteInternalFuture<Void> remove(String name, boolean force) {
        try {
            Serializable icp0 = ctx.distributedMetastorage().read(metastorageKey(name));

            if (!isClassPath(icp0))
                throw new IgniteException("ClassPath not found: " + name);

            IgniteClassPath icp = (IgniteClassPath)icp0;

            if (force) {
                log.warning("Forcefully removing ClassPath: " + icp);

                try {
                    removeFromMetastorage(icp, () -> true);

                    // Cleanup will be performed in {@link ClassPathChangeListener}.
                    return new GridFinishedFuture<>();
                }
                catch (Exception e) {
                    return new GridFinishedFuture<>(e);
                }
                finally {
                    // Cleanup queue after key removed from metastore.
                    cancelTasksAndDisallowNew(icp.id(), icp.name(), false);
                }
            }

            if (icp.state() == REMOVING)
                return new GridFinishedFuture<>();

            // Cleanup and metastorage removal will be performed in {@link ClassPathChangeListener}.
            ChangeStateTask changeState = new ChangeStateTask(ctx, icp.id(), REMOVING);

            addClassPathTask(icp, changeState);

            return changeState.result();
        }
        catch (Exception e) {
            log.warning("Error while removing ClassPath", e);

            return new GridFinishedFuture<>(e);
        }
    }

    /** */
    void cancelTasksAndDisallowNew(UUID icpId, String name, boolean keepQueueLock) {
        Queue<ClassPathTask<?>> tasks = keepQueueLock
            ? icpTasks.put(icpId, NO_NEW_TASKS)
            : icpTasks.remove(icpId);

        if (!F.isEmpty(tasks)) {
            for (ClassPathTask<?> task : tasks) {
                log.info("Cancelling task [icp=" + name + ", task=" + task.name() + ']');

                task.stopped = true;
            }

            for (ClassPathTask<?> task : tasks) {
                try {
                    task.result().get();
                }
                catch (IgniteCheckedException e) {
                    log.warning("Stopped task exception", e);
                }
            }
        }
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
                if (tasks == NO_NEW_TASKS) {
                    log.debug("Tasks dropped. No new task allowed for ClassPath. Remove in progress? [icp=" + icp + ']');

                    return NO_NEW_TASKS;
                }

                if (tasks == null)
                    tasks = new ConcurrentLinkedQueue<>();

                if (tasks.peek() != null && tasks.peek().result() == doneFut) {
                    tasks.poll();

                    if (!F.isEmpty(tasks))
                        startAsync(tasks.peek());
                }
                else
                    tasks.removeIf(t -> t == task);

                return F.isEmpty(tasks) ? null : tasks;
            });
        });

        icpTasks.compute(icpId, (ignored, tasks) -> {
            if (tasks == NO_NEW_TASKS) {
                log.debug("Tasks dropped. No new task allowed for ClassPath. Remove in progress? [icp=" + icp + ']');

                return NO_NEW_TASKS;
            }

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
    <R> void startAsync(ClassPathTask<R> t) {
        try {
            ctx.pools().getSystemExecutorService().submit(() -> {
                if (ctx.isStopping()) {
                    t.result().onDone(new IgniteException("Node stopping"));

                    return;
                }

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
    void removeFromMetastorage(IgniteClassPath icp, BooleanSupplier stopped) throws IgniteCheckedException {
        String key = metastorageKey(icp.name());

        int iter = 0;

        while (true) {
            ensureNotStopped(stopped);

            Serializable curData = ctx.distributedMetastorage().read(key);

            if (curData == null || !isClassPath(curData) || !Objects.equals(((IgniteClassPath)curData).id(), icp.id()))
                break;

            if (ctx.distributedMetastorage().compareAndRemove(key, curData))
                break;

            iter++;

            if (iter == 500)
                throw new IgniteException("Too many iterations");

            if (iter % 100 == 0)
                log.warning("Remove operation makes too many iterations. Bug? [icp=" + icp + ", curData=" + curData + ']');

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
     * @return Future with the new {@link IgniteClassPath} instance.
     */
    public GridFutureAdapter<IgniteClassPath> modifyInMetastorageAsync(
        UUID icpId,
        @Nullable IgniteClassPathState state,
        Function<IgniteClassPath, IgniteClassPath> change,
        BooleanSupplier stopped
    ) {
        GridFutureAdapter<IgniteClassPath> res = new GridFutureAdapter<>();

        modifyWithRetriesAsync(icpId, state, change, res, stopped, 100);

        return res;
    }

    /** */
    private void modifyWithRetriesAsync(
        UUID icpId,
        @Nullable IgniteClassPathState state,
        Function<IgniteClassPath, IgniteClassPath> change,
        GridFutureAdapter<IgniteClassPath> res,
        BooleanSupplier stopped,
        int hardLimit
    ) {
        if (hardLimit == 0) {
            log.error("Reached hard limit on CAS attempts. Fail operation [icpId=" + icpId + ", state=" + state + ']');

            res.onDone(new IgniteException("Hard limit of CAS reached"));

            return;
        }

        try {
            ensureNotStopped(stopped);

            IgniteClassPath prev = fromMetastorage(icpId, state, ctx);
            IgniteClassPath icp = change.apply(prev);

            if (Objects.equals(prev, icp)) {
                res.onDone(icp);

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
                    res.onDone(icp);

                    return;
                }

                // Concurrent node modifies first? It's OK, trying to repeat.
                modifyWithRetriesAsync(icpId, state, change, res, stopped, hardLimit - 1);

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
    private static void ensureKnownFilename(String name, IgniteClassPath icp, LongSupplier lastByteOffset) {
        ensureFilename(name);

        int idx = F.indexOf(icp.files(), name);

        if (idx == -1)
            throw new IllegalArgumentException("Unknown lib [icp=" + icp.name() + ", unknown_lib=" + name + ']');

        long length = icp.lengths()[idx];

        if (lastByteOffset.getAsLong() > length)
            throw new IllegalArgumentException("Unexpected file offset [icp=" + icp.name() + ", file=" + name + ']');
    }

    /**
     * Iterates all existing {@link IgniteClassPath} in metastorage.
     * @param action Action to invoke for each {@link IgniteClassPath} instance.
     * @throws IgniteCheckedException In case of error.
     */
    private void iterateMetastorage(BiConsumer<String, IgniteClassPath> action) throws IgniteCheckedException {
        ctx.distributedMetastorage().iterate(METASTORE_PREFIX, (key, val) -> {
            if (!isClassPath(val)) {
                log.warning("Wrong data in IgniteClassPath metastorage data [key=" + key + ", val=" + (val) + ']');

                return;
            }

            action.accept(key, (IgniteClassPath)val);
        });
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
    public static void writeClassPathDescriptor(NodeFileTree ft, IgniteClassPath icp) throws IOException {
        File desc = ft.classPathDescriptor(icp.name());

        File tmp = new File(desc.getParentFile(), desc.getName() + TMP_SUFFIX);

        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            icp.toProperties().store(fos, null);

            fos.getFD().sync();
        }

        Files.move(tmp.toPath(), desc.toPath(), StandardCopyOption.ATOMIC_MOVE);
    }

    /** */
    public @Nullable IgniteClassPath readClassPathDescriptor(File desc) {
        if (!desc.exists())
            return null;

        try (FileInputStream fis = new FileInputStream(desc)) {
            Properties p = new Properties();

            p.load(fis);

            return IgniteClassPath.fromProperties(p, ctx.localNodeId());
        }
        catch (Exception e) {
            log.warning("Can't read ClassPath descriptor", e);

            return null;
        }
    }

    /** */
    synchronized void createRootAndCheckIsEmpty(IgniteClassPath icp) {
        File cpRoot = ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name());

        if (!cpRoot.exists())
            NodeFileTree.mkdir(cpRoot, "Ignite Class Path root");
        else if (!F.isEmpty(cpRoot.listFiles()))
            throw new IgniteException("ClassPath root exists and not empty: " + cpRoot);

        try {
            guardFile(cpRoot, icp.id()).createNewFile();
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    synchronized void removeClassPathLocally(IgniteClassPath icp, boolean force) {
        File cpRoot = ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name());

        if (!guardFile(cpRoot, icp.id()).exists() && !force) {
            log.debug("No guard file found. Skip local data removal. " +
                "ClassPath not presented locally or concurrently recreated? [icp=" + icp + ']');

            return;
        }

        if (!cpRoot.exists() || U.delete(cpRoot))
            return;

        String err = "Can't delete local files. Remove manually [dir=" + cpRoot + ']';

        log.warning(err);

        ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, new IgniteException(err)));
    }

    /** */
    public static File guardFile(File cpRoot, UUID icpId) {
        return new File(cpRoot, "." + icpId);
    }

    /**
     * Checks local ClassPath roots with the cluster state.
     *
     * @return Array of local ClassPath root directories to remove on start.
     */
    private File[] staleLocalRoots() {
        NodeFileTree ft = ctx.pdsFolderResolver().fileTree();

        return ft.classPathRoot().listFiles(cpRoot -> {
            if (!cpRoot.isDirectory())
                return false;

            try {
                IgniteClassPath loc = readClassPathDescriptor(ft.classPathDescriptor(cpRoot.getName()));

                if (loc == null)
                    return true;

                Serializable fromMetastore = ctx.distributedMetastorage().read(metastorageKey(loc.name()));

                if (fromMetastore == null) {
                    log.info("Unknown local data. Removing [icp=" + loc + ']');

                    return true;
                }

                if (!isClassPath(fromMetastore)) {
                    log.warning("Wrong data in IgniteClassPath metastorage data " +
                        "[key=" + metastorageKey(loc.name()) + ", val=" + fromMetastore + ']');

                    return true;
                }

                IgniteClassPath rmt = (IgniteClassPath)fromMetastore;

                if (!Objects.equals(loc.id(), rmt.id()) || rmt.state() == REMOVING) {
                    log.info("Stale local data. Removing [loc=" + loc + ", rmt=" + rmt + ']');

                    return true;
                }
            }
            catch (Exception e) {
                log.warning("Error filter local root: " + cpRoot, e);

                return true;
            }

            return false;
        });
    }

    /** */
    static void ensureNotStopped(BooleanSupplier stopped) {
        if (stopped.getAsBoolean())
            throw new IgniteException("Stopped");
    }

    /** */
    abstract static class ClassPathTask<R> {
        /** */
        private final GridFutureAdapter<R> res = new GridFutureAdapter<R>();

        /** */
        protected final GridKernalContext ctx;

        /** */
        protected final UUID icpId;

        /** */
        protected final IgniteLogger log;

        /** */
        volatile boolean stopped;

        /** */
        protected ClassPathTask(GridKernalContext ctx, UUID icpId) {
            this.ctx = ctx;
            this.icpId = icpId;
            this.log = ctx.log(getClass());

            result().listen(resFut -> {
                try {
                    if (resFut.error() == null)
                        ok();
                    else
                        fail(resFut.error());
                }
                finally {
                    ClassPathTask<R> t = this;

                    ctx.classPath().icpTasks.compute(icpId, (ignored, tasks) -> {
                        if (tasks == null)
                            return null;

                        tasks.remove(t);

                        return F.isEmpty(tasks) ? null : tasks;
                    });
                }
            });
        }

        /** */
        void finishTaskWithFutureResult(IgniteInternalFuture<IgniteClassPath> action) {
            if (action.error() == null) {
                if (updateDescriptor(action.result()))
                    result().onDone();
            }
            else
                result().onDone(action.error());
        }

        /**
         * Starts task execution.
         * Must be nonblocking and fast.
         * In case any exception throwed, {@link #result()} will be finished and task treated as done.
         */
        final void start() {
            if (stopped) {
                result().onDone(new IgniteException("Stopped before start"));

                return;
            }

            try {
                start0();
            }
            catch (Throwable e) {
                res.onDone(e);
            }
        }

        /**
         * Starts task execution.
         * Must be nonblocking and fast.
         * In case any exception throwed, {@link #result()} will be finished and task treated as done.
         */
        abstract void start0();

        /**
         * @return Task name.
         */
        abstract String name();

        /**
         * @return Task results.
         */
        GridFutureAdapter<R> result() {
            return res;
        }

        /** */
        boolean stopped() {
            return stopped;
        }

        /** */
        protected boolean updateDescriptor(IgniteClassPath icp) {
            try {
                writeClassPathDescriptor(ctx.pdsFolderResolver().fileTree(), icp);

                return true;
            }
            catch (Exception e) {
                result().onDone(e);

                return false;
            }
        }

        /** */
        abstract void ok();

        /** */
        abstract void fail(Throwable t);
    }
}
