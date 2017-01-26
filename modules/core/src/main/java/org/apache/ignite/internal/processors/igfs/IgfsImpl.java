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

package org.apache.ignite.internal.processors.igfs;

import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.igfs.IgfsPerBlockLruEvictionPolicy;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.events.IgfsEvent;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsInvalidPathException;
import org.apache.ignite.igfs.IgfsMetrics;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathIsDirectoryException;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.igfs.IgfsPathSummary;
import org.apache.ignite.igfs.mapreduce.IgfsRecordResolver;
import org.apache.ignite.igfs.mapreduce.IgfsTask;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.hadoop.HadoopPayloadAware;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientAffinityCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientDeleteCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientExistsCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientInfoCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientListFilesCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientListPathsCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientMkdirsCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientRenameCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientSetTimesCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientSizeCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientSummaryCallable;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientUpdateCallable;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_IGFS_DIR_DELETED;
import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_DELETED;
import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_OPENED_READ;
import static org.apache.ignite.events.EventType.EVT_IGFS_META_UPDATED;
import static org.apache.ignite.igfs.IgfsMode.DUAL_ASYNC;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * Cache-based IGFS implementation.
 */
public final class IgfsImpl implements IgfsEx {
    /** Default permissions for file system entry. */
    private static final String PERMISSION_DFLT_VAL = "0777";

    /** Index generator for async format threads. */
    private static final AtomicInteger FORMAT_THREAD_IDX_GEN = new AtomicInteger();

    /** Default directory metadata. */
    static final Map<String, String> DFLT_DIR_META = F.asMap(IgfsUtils.PROP_PERMISSION, PERMISSION_DFLT_VAL);

    /** Handshake message. */
    private final IgfsPaths secondaryPaths;

    /** Cache based structure (meta data) manager. */
    private IgfsMetaManager meta;

    /** Cache based file's data container. */
    private IgfsDataManager data;

    /** FS configuration. */
    private FileSystemConfiguration cfg;

    /** IGFS context. */
    private IgfsContext igfsCtx;

    /** Event storage manager. */
    private GridEventStorageManager evts;

    /** Logger. */
    private IgniteLogger log;

    /** Mode resolver. */
    private final IgfsModeResolver modeRslvr;

    /** Connection to the secondary file system. */
    private IgfsSecondaryFileSystem secondaryFs;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Writers map. */
    private final ConcurrentHashMap8<IgfsPath, IgfsFileWorkerBatch> workerMap = new ConcurrentHashMap8<>();

    /** Client log directory. */
    private volatile String logDir;

    /** Eviction policy (if set). */
    private IgfsPerBlockLruEvictionPolicy evictPlc;

    /** Pool for threads working in DUAL mode. */
    private final IgniteThreadPoolExecutor dualPool;

    /**
     * Creates IGFS instance with given context.
     *
     * @param igfsCtx Context.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("ConstantConditions")
    IgfsImpl(IgfsContext igfsCtx) throws IgniteCheckedException {
        assert igfsCtx != null;

        this.igfsCtx = igfsCtx;

        cfg = igfsCtx.configuration();
        log = igfsCtx.kernalContext().log(IgfsImpl.class);
        evts = igfsCtx.kernalContext().event();
        meta = igfsCtx.meta();
        data = igfsCtx.data();
        secondaryFs = cfg.getSecondaryFileSystem();

        if (secondaryFs instanceof IgfsKernalContextAware)
            ((IgfsKernalContextAware)secondaryFs).setKernalContext(igfsCtx.kernalContext());

        if (secondaryFs instanceof LifecycleAware)
            ((LifecycleAware)secondaryFs).start();

        /* Default IGFS mode. */
        IgfsMode dfltMode;

        if (secondaryFs == null) {
            if (cfg.getDefaultMode() == PROXY)
                throw new IgniteCheckedException("Mode cannot be PROXY if secondary file system hasn't been defined.");

            dfltMode = PRIMARY;
        }
        else
            dfltMode = cfg.getDefaultMode();

        Map<String, IgfsMode> cfgModes = new LinkedHashMap<>();
        Map<String, IgfsMode> dfltModes = new LinkedHashMap<>(4, 1.0f);

        if (cfg.isInitializeDefaultPathModes() && IgfsUtils.isDualMode(dfltMode)) {
            dfltModes.put("/ignite/primary", PRIMARY);

            if (secondaryFs != null) {
                dfltModes.put("/ignite/proxy", PROXY);
                dfltModes.put("/ignite/sync", DUAL_SYNC);
                dfltModes.put("/ignite/async", DUAL_ASYNC);
            }
        }

        cfgModes.putAll(dfltModes);

        if (cfg.getPathModes() != null) {
            for (Map.Entry<String, IgfsMode> e : cfg.getPathModes().entrySet()) {
                if (!dfltModes.containsKey(e.getKey()))
                    cfgModes.put(e.getKey(), e.getValue());
                else
                    U.warn(log, "Ignoring path mode because it conflicts with Ignite reserved path " +
                        "(use another path) [mode=" + e.getValue() + ", path=" + e.getKey() + ']');
            }
        }

        ArrayList<T2<IgfsPath, IgfsMode>> modes = null;

        if (!cfgModes.isEmpty()) {
            modes = new ArrayList<>(cfgModes.size());

            for (Map.Entry<String, IgfsMode> mode : cfgModes.entrySet()) {
                IgfsMode mode0;

                if (mode.getValue() == PROXY) {
                    if (secondaryFs == null)
                        throw new IgniteCheckedException("Mode cannot be PROXY if secondary file system hasn't" +
                            " been defined: " + mode.getKey());

                    mode0 = PROXY;
                }
                else
                    mode0 = secondaryFs == null ? PRIMARY : mode.getValue();

                try {
                    modes.add(new T2<>(new IgfsPath(mode.getKey()), mode0));
                }
                catch (IllegalArgumentException e) {
                    throw new IgniteCheckedException("Invalid path found in mode pattern: " + mode.getKey(), e);
                }
            }
        }

        modeRslvr = new IgfsModeResolver(dfltMode, modes);

        Object secondaryFsPayload = null;

        if (secondaryFs instanceof HadoopPayloadAware)
            secondaryFsPayload = ((HadoopPayloadAware) secondaryFs).getPayload();

        secondaryPaths = new IgfsPaths(secondaryFsPayload, dfltMode, modeRslvr.modesOrdered());

        // Check whether IGFS LRU eviction policy is set on data cache.
        String dataCacheName = igfsCtx.configuration().getDataCacheName();

        for (CacheConfiguration cacheCfg : igfsCtx.kernalContext().config().getCacheConfiguration()) {
            if (F.eq(dataCacheName, cacheCfg.getName())) {
                EvictionPolicy evictPlc = cacheCfg.getEvictionPolicy();

                if (evictPlc != null & evictPlc instanceof IgfsPerBlockLruEvictionPolicy)
                    this.evictPlc = (IgfsPerBlockLruEvictionPolicy)evictPlc;

                break;
            }
        }

        dualPool = secondaryFs != null ? new IgniteThreadPoolExecutor(4, Integer.MAX_VALUE, 5000L,
            new SynchronousQueue<Runnable>(), new IgfsThreadFactory(cfg.getName()), null) : null;
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        busyLock.block();

        // Clear interrupted flag temporarily.
        boolean interrupted = Thread.interrupted();

        if (secondaryFs != null) {
            // Force all workers to finish their batches.
            for (IgfsFileWorkerBatch batch : workerMap.values())
                batch.cancel();

            try {
                if (secondaryFs instanceof LifecycleAware)
                    ((LifecycleAware)secondaryFs).stop();
            }
            catch (Exception e) {
                log.error("Failed to close secondary file system.", e);
            }
        }

        // Restore interrupted flag.
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Create batch for the file.
     *
     * @param path File path in the secondary file system.
     * @param out Output stream to that file.
     * @return Created batch.
     * @throws IgniteCheckedException In case new batch cannot be created.
     */
    private IgfsFileWorkerBatch newBatch(final IgfsPath path, OutputStream out) throws IgniteCheckedException {
        assert path != null;
        assert out != null;

        if (enterBusy()) {
            try {
                // Create new batch.
                IgfsFileWorkerBatch batch = new IgfsFileWorkerBatch(path, out) {
                    @Override protected void onDone() {
                        workerMap.remove(path, this);
                    }
                };

                // Submit it to the thread pool immediately.
                assert dualPool != null;

                dualPool.execute(batch);

                // Spin in case another batch is currently running.
                while (true) {
                    IgfsFileWorkerBatch prevBatch = workerMap.putIfAbsent(path, batch);

                    if (prevBatch == null)
                        break;
                    else
                        prevBatch.await();
                }

                return batch;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Cannot create new output stream to the secondary file system " +
                "because IGFS is stopping: " + path);
    }

    /**
     * @return {@code True} if entered busy section.
     * @throws IgniteException If failed to await caches start.
     */
    private boolean enterBusy() {
        meta.awaitInit();
        data.awaitInit();

        return busyLock.enterBusy();
    }

    /** {@inheritDoc} */
    @Override public void await(IgfsPath... paths) {
        assert paths != null;

        for (Map.Entry<IgfsPath, IgfsFileWorkerBatch> workerEntry : workerMap.entrySet()) {
            IgfsPath workerPath = workerEntry.getKey();

            boolean await = false;

            for (IgfsPath path : paths) {
                if (workerPath.isSubDirectoryOf(path) || F.eq(workerPath, path))  {
                    await = true;

                    break;
                }
            }

            if (await) {
                IgfsFileWorkerBatch batch = workerEntry.getValue();

                if (batch != null) {
                    try {
                        // We wait only on files which had been closed, but their async writes are still in progress.
                        // This check is racy if several threads are modifying file system concurrently, but we are ok
                        // with that. The sole purpose of this waiting is to ensure happens-before semantics for a
                        // single thread. E.g., we have thread A working with Hadoop file system in another process.
                        // This file system communicates with a node and actual processing occurs in threads B and C
                        // of the current process. What we need to ensure is that if thread A called "close" then
                        // subsequent operations of this thread "see" this close and wait for async writes to finish.
                        // And as we do not on which paths thread A performed writes earlier, we have to wait for all
                        // batches on current path and all it's known children.
                        if (batch.finishing())
                            batch.await();
                    }
                    catch (IgniteCheckedException ignore) {
                        // No-op.
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsContext context() {
        return igfsCtx;
    }

    /**
     * @return Mode resolver.
     */
    IgfsModeResolver modeResolver() {
        return modeRslvr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String name() {
        return cfg.getName();
    }

    /** {@inheritDoc} */
    @Override public FileSystemConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public IgfsPaths proxyPaths() {
        return secondaryPaths;
    }

    /** {@inheritDoc} */
    @Override public String clientLogDirectory() {
        return logDir;
    }

    /** {@inheritDoc} */
    @Override public void clientLogDirectory(String logDir) {
        this.logDir = logDir;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public IgfsStatus globalSpace() {
        return safeOp(new Callable<IgfsStatus>() {
            @Override public IgfsStatus call() throws Exception {
                IgniteBiTuple<Long, Long> space = igfsCtx.kernalContext().grid().compute().execute(
                    new IgfsGlobalSpaceTask(name()), null);

                return new IgfsStatus(space.get1(), space.get2());
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void globalSampling(@Nullable final Boolean val) throws IgniteCheckedException {
        safeOp(new Callable<Void>() {
            @Override public Void call() throws Exception {
                if (meta.sampling(val)) {
                    if (val == null)
                        log.info("Sampling flag has been cleared. All further file system connections will perform " +
                            "logging depending on their configuration.");
                    else if (val)
                        log.info("Sampling flag has been set to \"true\". All further file system connections will " +
                            "perform logging.");
                    else
                        log.info("Sampling flag has been set to \"false\". All further file system connections will " +
                            "not perform logging.");
                }

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override @Nullable public Boolean globalSampling() {
        return safeOp(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                return meta.sampling();
            }
        });
    }
    /** {@inheritDoc} */
    @Override public long groupBlockSize() {
        return data.groupBlockSize();
    }

    /** {@inheritDoc} */
    @Override public boolean exists(final IgfsPath path) {
        A.notNull(path, "path");

        if (meta.isClient())
            return meta.runClientTask(new IgfsClientExistsCallable(cfg.getName(), path));

        return safeOp(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Check file exists: " + path);

                IgfsMode mode = resolveMode(path);

                boolean res = false;

                switch (mode) {
                    case PRIMARY:
                        res = meta.fileId(path) != null;

                        break;

                    case DUAL_SYNC:
                    case DUAL_ASYNC:
                        res = meta.fileId(path) != null;

                        if (!res) {
                            try {
                                res = secondaryFs.exists(path);
                            }
                            catch (Exception e) {
                                U.error(log, "Exists in DUAL mode failed [path=" + path + ']', e);

                                throw e;
                            }
                        }

                        break;

                    case PROXY:
                        res = secondaryFs.exists(path);

                        break;
                }

                return res;
            }
        });
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgfsFile info(final IgfsPath path) {
        A.notNull(path, "path");

        if (meta.isClient())
            return meta.runClientTask(new IgfsClientInfoCallable(cfg.getName(), path));

        return safeOp(new Callable<IgfsFile>() {
            @Override public IgfsFile call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Get file info: " + path);

                IgfsMode mode = resolveMode(path);

                return resolveFileInfo(path, mode);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgfsMode mode(IgfsPath path) {
        A.notNull(path, "path");

        return modeRslvr.resolveMode(path);
    }

    /** {@inheritDoc} */
    @Override public IgfsPathSummary summary(final IgfsPath path) {
        A.notNull(path, "path");

        if (meta.isClient())
            return meta.runClientTask(new IgfsClientSummaryCallable(cfg.getName(), path));

        return safeOp(new Callable<IgfsPathSummary>() {
            @Override public IgfsPathSummary call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Calculating path summary: " + path);

                return summary0(path);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgfsFile update(final IgfsPath path, final Map<String, String> props) {
        A.notNull(path, "path");
        A.notNull(props, "props");
        A.ensure(!props.isEmpty(), "!props.isEmpty()");

        if (meta.isClient())
            return meta.runClientTask(new IgfsClientUpdateCallable(cfg.getName(), path, props));

        return safeOp(new Callable<IgfsFile>() {
            @Override public IgfsFile call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Set file properties [path=" + path + ", props=" + props + ']');

                IgfsMode mode = resolveMode(path);

                switch (mode) {
                    case PRIMARY:
                    {
                        List<IgniteUuid> fileIds = meta.idsForPath(path);

                        IgniteUuid fileId = fileIds.get(fileIds.size() - 1);

                        if (fileId != null) {
                            IgfsEntryInfo info = meta.updateProperties(fileId, props);

                            if (info != null) {
                                if (evts.isRecordable(EVT_IGFS_META_UPDATED))
                                    evts.record(new IgfsEvent(path, igfsCtx.localNode(), EVT_IGFS_META_UPDATED, props));

                                return new IgfsFileImpl(path, info, data.groupBlockSize());
                            }
                        }

                        break;
                    }

                    case DUAL_ASYNC:
                    case DUAL_SYNC:
                    {
                        await(path);

                        IgfsEntryInfo info = meta.updateDual(secondaryFs, path, props);

                        if (info != null)
                            return new IgfsFileImpl(path, info, data.groupBlockSize());

                        break;
                    }

                    default:
                        assert mode == PROXY : "Unknown mode: " + mode;

                        IgfsFile file = secondaryFs.update(path, props);

                        if (file != null)
                            return new IgfsFileImpl(secondaryFs.update(path, props), data.groupBlockSize());
                }

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void rename(final IgfsPath src, final IgfsPath dest) {
        A.notNull(src, "src");
        A.notNull(dest, "dest");

        if (meta.isClient()) {
            meta.runClientTask(new IgfsClientRenameCallable(cfg.getName(), src, dest));

            return;
        }

        safeOp(new Callable<Void>() {
            @Override public Void call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Rename file [src=" + src + ", dest=" + dest + ']');

                IgfsMode mode = resolveMode(src);

                if (src.equals(dest))
                    return null; // Rename to itself is a no-op.

                // Cannot rename root directory.
                if (src.parent() == null)
                    throw new IgfsInvalidPathException("Root directory cannot be renamed.");

                // Cannot move directory of upper level to self sub-dir.
                if (dest.isSubDirectoryOf(src))
                    throw new IgfsInvalidPathException("Failed to rename directory (cannot move directory of " +
                        "upper level to self sub-dir) [src=" + src + ", dest=" + dest + ']');

                if (evictExclude(src, mode == PRIMARY) != evictExclude(dest, modeRslvr.resolveMode(dest) == PRIMARY))
                    throw new IgfsInvalidPathException("Cannot move file to a path with different eviction " +
                        "exclude setting (need to copy and remove)");

                switch (mode) {
                    case PRIMARY:
                        meta.move(src, dest);

                        break;

                    case DUAL_ASYNC:
                    case DUAL_SYNC:
                        await(src, dest);

                        meta.renameDual(secondaryFs, src, dest);

                        break;

                    default:
                        assert mode == PROXY : "Unknown mode: " + mode;

                        secondaryFs.rename(src, dest);

                        break;
                }

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean delete(final IgfsPath path, final boolean recursive) {
        A.notNull(path, "path");

        if (meta.isClient())
            return meta.runClientTask(new IgfsClientDeleteCallable(cfg.getName(), path, recursive));

        return safeOp(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Deleting file [path=" + path + ", recursive=" + recursive + ']');

                if (F.eq(IgfsPath.ROOT, path))
                    return false;

                IgfsMode mode = resolveMode(path);

                if (mode == PROXY)
                    return secondaryFs.delete(path, recursive);

                boolean dual = IgfsUtils.isDualMode(mode);

                if (dual)
                    await(path);

                IgfsDeleteResult res = meta.softDelete(path, recursive, dual ? secondaryFs : null);

                // Record event if needed.
                if (res.success() && res.info() != null)
                    IgfsUtils.sendEvents(igfsCtx.kernalContext(), path,
                        res.info().isFile() ? EVT_IGFS_FILE_DELETED : EVT_IGFS_DIR_DELETED);

                return res.success();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path) {
        mkdirs(path, null);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(final IgfsPath path, @Nullable final Map<String, String> props)  {
        A.notNull(path, "path");

        if (meta.isClient()) {
            meta.runClientTask(new IgfsClientMkdirsCallable(cfg.getName(), path, props));

            return ;
        }

        safeOp(new Callable<Void>() {
            @Override public Void call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Make directories: " + path);

                IgfsMode mode = resolveMode(path);

                switch (mode) {
                    case PRIMARY:
                        meta.mkdirs(path, props == null ? DFLT_DIR_META : new HashMap<>(props));

                        break;

                    case DUAL_ASYNC:
                    case DUAL_SYNC:
                        await(path);

                        meta.mkdirsDual(secondaryFs, path, props);

                        break;

                    case PROXY:
                        secondaryFs.mkdirs(path, props);

                        break;
                }

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Collection<IgfsPath> listPaths(final IgfsPath path) {
        A.notNull(path, "path");

        if (meta.isClient())
            meta.runClientTask(new IgfsClientListPathsCallable(cfg.getName(), path));

        return safeOp(new Callable<Collection<IgfsPath>>() {
            @Override public Collection<IgfsPath> call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("List directory: " + path);

                IgfsMode mode = resolveMode(path);

                Collection<IgfsPath> files = new HashSet<>();

                if (mode != PRIMARY) {
                    assert secondaryFs != null;

                    try {
                        Collection<IgfsPath> children = secondaryFs.listPaths(path);

                        files.addAll(children);

                        if (mode == PROXY || !modeRslvr.hasPrimaryChild(path))
                            return files;
                    }
                    catch (Exception e) {
                        U.error(log, "List paths in DUAL mode failed [path=" + path + ']', e);

                        throw e;
                    }
                }

                IgfsEntryInfo info = primaryInfoForListing(path);

                if (info != null) {
                    // Perform the listing.
                    for (String child : info.listing().keySet())
                        files.add(new IgfsPath(path, child));
                }
                else if (mode == PRIMARY)
                    throw new IgfsPathNotFoundException("Failed to list paths (path not found): " + path);

                return files;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsFile> listFiles(final IgfsPath path) {
        A.notNull(path, "path");

        if (meta.isClient())
            meta.runClientTask(new IgfsClientListFilesCallable(cfg.getName(), path));

        return safeOp(new Callable<Collection<IgfsFile>>() {
            @Override public Collection<IgfsFile> call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("List directory details: " + path);

                IgfsMode mode = resolveMode(path);

                Collection<IgfsFile> files = new HashSet<>();

                if (mode != PRIMARY) {
                    assert secondaryFs != null;

                    try {
                        Collection<IgfsFile> children = secondaryFs.listFiles(path);

                        for (IgfsFile child : children) {
                            IgfsFileImpl impl = new IgfsFileImpl(child, data.groupBlockSize());

                            files.add(impl);
                        }

                        if (mode == PROXY || !modeRslvr.hasPrimaryChild(path))
                            return files;
                    }
                    catch (Exception e) {
                        U.error(log, "List files in DUAL mode failed [path=" + path + ']', e);

                        throw e;
                    }
                }

                IgfsEntryInfo info = primaryInfoForListing(path);

                if (info != null) {
                    if (info.isFile())
                        // If this is a file, return its description.
                        return Collections.<IgfsFile>singleton(new IgfsFileImpl(path, info,
                            data.groupBlockSize()));

                    // Perform the listing.
                    for (Map.Entry<String, IgfsListingEntry> e : info.listing().entrySet()) {
                        IgfsEntryInfo childInfo = meta.info(e.getValue().fileId());

                        if (childInfo != null) {
                            IgfsPath childPath = new IgfsPath(path, e.getKey());

                            files.add(new IgfsFileImpl(childPath, childInfo, data.groupBlockSize()));
                        }
                    }
                }
                else if (mode == PRIMARY)
                    throw new IgfsPathNotFoundException("Failed to list files (path not found): " + path);

                return files;
            }
        });
    }

    /**
     * Get primary file system info for listing operation.
     *
     * @param path Path.
     * @return Info or {@code null} if not found.
     * @throws IgniteCheckedException If failed.
     */
    private IgfsEntryInfo primaryInfoForListing(IgfsPath path) throws IgniteCheckedException {
        IgniteUuid fileId = meta.fileId(path);

        return fileId != null ? meta.info(fileId) : null;
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() {
        return metrics().localSpaceSize();
    }

    /** {@inheritDoc} */
    @Override public IgfsInputStream open(IgfsPath path) {
        return open(path, cfg.getStreamBufferSize(), cfg.getSequentialReadsBeforePrefetch());
    }

    /** {@inheritDoc} */
    @Override public IgfsInputStream open(IgfsPath path, int bufSize) {
        return open(path, bufSize, cfg.getSequentialReadsBeforePrefetch());
    }

    /** {@inheritDoc} */
    @Override public IgfsInputStream open(final IgfsPath path, final int bufSize,
        final int seqReadsBeforePrefetch) {
        A.notNull(path, "path");
        A.ensure(bufSize >= 0, "bufSize >= 0");
        A.ensure(seqReadsBeforePrefetch >= 0, "seqReadsBeforePrefetch >= 0");

        return safeOp(new Callable<IgfsInputStream>() {
            @Override public IgfsInputStream call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Open file for reading [path=" + path + ", bufSize=" + bufSize + ']');

                int bufSize0 = bufSize == 0 ? cfg.getStreamBufferSize() : bufSize;

                IgfsMode mode = resolveMode(path);

                switch (mode) {
                    case PRIMARY: {
                        IgfsEntryInfo info = meta.infoForPath(path);

                        if (info == null)
                            throw new IgfsPathNotFoundException("File not found: " + path);

                        if (!info.isFile())
                            throw new IgfsPathIsDirectoryException("Failed to open file (not a file): " + path);

                        // Input stream to read data from grid cache with separate blocks.
                        IgfsInputStreamImpl os = new IgfsInputStreamImpl(igfsCtx, path, info,
                            cfg.getPrefetchBlocks(), seqReadsBeforePrefetch, null,
                            info.length(), info.blockSize(), info.blocksCount(), false);

                        IgfsUtils.sendEvents(igfsCtx.kernalContext(), path, EVT_IGFS_FILE_OPENED_READ);

                        return os;
                    }

                    case DUAL_ASYNC:
                    case DUAL_SYNC: {
                        assert IgfsUtils.isDualMode(mode);

                        IgfsSecondaryInputStreamDescriptor desc = meta.openDual(secondaryFs, path, bufSize0);

                        IgfsEntryInfo info = desc.info();

                        IgfsInputStreamImpl os = new IgfsInputStreamImpl(igfsCtx, path, info,
                            cfg.getPrefetchBlocks(), seqReadsBeforePrefetch, desc.reader(),
                            info.length(), info.blockSize(), info.blocksCount(), false);

                        IgfsUtils.sendEvents(igfsCtx.kernalContext(), path, EVT_IGFS_FILE_OPENED_READ);

                        return os;
                    }

                    case PROXY: {
                        assert secondaryFs != null;

                        IgfsFile info = info(path);

                        if (info == null)
                            throw new IgfsPathNotFoundException("File not found: " + path);

                        if (!info.isFile())
                            throw new IgfsPathIsDirectoryException("Failed to open file (not a file): " + path);

                        IgfsSecondaryFileSystemPositionedReadable secReader =
                            new IgfsLazySecondaryFileSystemPositionedReadable(secondaryFs, path, bufSize);

                        long len = info.length();

                        int blockSize = info.blockSize() > 0 ? info.blockSize() : cfg.getBlockSize();

                        long blockCnt = len / blockSize;

                        if (len % blockSize != 0)
                            blockCnt++;

                        IgfsInputStream os = new IgfsInputStreamImpl(igfsCtx, path, null,
                            cfg.getPrefetchBlocks(), seqReadsBeforePrefetch, secReader,
                            info.length(), blockSize, blockCnt, true);

                        IgfsUtils.sendEvents(igfsCtx.kernalContext(), path, EVT_IGFS_FILE_OPENED_READ);

                        return os;
                    }

                    default:
                        assert false : "Unexpected mode " + mode;
                        return null;
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream create(IgfsPath path, boolean overwrite) {
        return create0(path, cfg.getStreamBufferSize(), overwrite, null, 0, null, true);
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) {
        return create0(path, bufSize, overwrite, null, replication, props, false);
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream create(IgfsPath path, int bufSize, boolean overwrite,
        @Nullable IgniteUuid affKey, int replication, long blockSize, @Nullable Map<String, String> props) {
        return create0(path, bufSize, overwrite, affKey, replication, props, false);
    }

    /**
     * Create new file.
     *
     * @param path Path.
     * @param bufSize Buffer size.
     * @param overwrite Overwrite flag.
     * @param affKey Affinity key.
     * @param replication Replication factor.
     * @param props Properties.
     * @param simpleCreate Whether new file should be created in secondary FS using create(Path, boolean) method.
     * @return Output stream.
     */
    private IgfsOutputStream create0(
        final IgfsPath path,
        final int bufSize,
        final boolean overwrite,
        @Nullable final IgniteUuid affKey,
        final int replication,
        @Nullable final Map<String, String> props,
        final boolean simpleCreate
    ) {
        A.notNull(path, "path");
        A.ensure(bufSize >= 0, "bufSize >= 0");

        return safeOp(new Callable<IgfsOutputStream>() {
            @Override public IgfsOutputStream call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Open file for writing [path=" + path + ", bufSize=" + bufSize + ", overwrite=" +
                        overwrite + ", props=" + props + ']');

                // Resolve mode.
                final IgfsMode mode = resolveMode(path);

                // Prepare properties.
                final Map<String, String> dirProps, fileProps;

                if (props == null) {
                    dirProps = DFLT_DIR_META;

                    fileProps = null;
                }
                else
                    dirProps = fileProps = new HashMap<>(props);

                if (mode == PROXY) {
                    assert secondaryFs != null;

                    OutputStream secondaryStream = secondaryFs.create(path, bufSize, overwrite, replication,
                        groupBlockSize(), props);

                    IgfsFileWorkerBatch batch = newBatch(path, secondaryStream);

                    return new IgfsOutputStreamProxyImpl(igfsCtx, path, info(path), bufferSize(bufSize), batch);
                }

                // Prepare context for DUAL mode.
                IgfsSecondaryFileSystemCreateContext secondaryCtx = null;

                if (mode != PRIMARY)
                    secondaryCtx = new IgfsSecondaryFileSystemCreateContext(secondaryFs, path, overwrite, simpleCreate,
                        fileProps, (short)replication, groupBlockSize(), bufSize);

                // Await for async ops completion if in DUAL mode.
                if (mode != PRIMARY)
                    await(path);

                // Perform create.
                IgfsCreateResult res = meta.create(
                    path,
                    dirProps,
                    overwrite,
                    cfg.getBlockSize(),
                    affKey,
                    evictExclude(path, mode == PRIMARY),
                    fileProps,
                    secondaryCtx
                );

                assert res != null;

                // Create secondary file system batch.
                OutputStream secondaryStream = res.secondaryOutputStream();

                IgfsFileWorkerBatch batch = secondaryStream != null ? newBatch(path, secondaryStream) : null;

                return new IgfsOutputStreamImpl(igfsCtx, path, res.info(), bufferSize(bufSize), mode, batch);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream append(IgfsPath path, boolean create) {
        return append(path, cfg.getStreamBufferSize(), create, null);
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream append(final IgfsPath path, final int bufSize, final boolean create,
        @Nullable final Map<String, String> props) {
        A.notNull(path, "path");
        A.ensure(bufSize >= 0, "bufSize >= 0");

        return safeOp(new Callable<IgfsOutputStream>() {
            @Override public IgfsOutputStream call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Open file for appending [path=" + path + ", bufSize=" + bufSize + ", create=" + create +
                        ", props=" + props + ']');

                final IgfsMode mode = resolveMode(path);

                if (mode == PROXY) {
                    assert secondaryFs != null;

                    OutputStream secondaryStream = secondaryFs.append(path, bufSize, create, props);

                    IgfsFileWorkerBatch batch = newBatch(path, secondaryStream);

                    return new IgfsOutputStreamProxyImpl(igfsCtx, path, info(path), bufferSize(bufSize), batch);
                }

                if (mode != PRIMARY) {
                    assert IgfsUtils.isDualMode(mode);

                    await(path);

                    IgfsCreateResult desc = meta.appendDual(secondaryFs, path, bufSize, create);

                    IgfsFileWorkerBatch batch = newBatch(path, desc.secondaryOutputStream());

                    return new IgfsOutputStreamImpl(igfsCtx, path, desc.info(), bufferSize(bufSize), mode, batch);
                }

                final List<IgniteUuid> ids = meta.idsForPath(path);

                final IgniteUuid id = ids.get(ids.size() - 1);

                if (id == null) {
                    if (!create)
                        throw new IgfsPathNotFoundException("File not found: " + path);
                }

                // Prevent attempt to append to ROOT in early stage:
                if (ids.size() == 1)
                    throw new IgfsPathIsDirectoryException("Failed to open file (not a file): " + path);

                final Map<String, String> dirProps, fileProps;

                if (props == null) {
                    dirProps = DFLT_DIR_META;

                    fileProps = null;
                }
                else
                    dirProps = fileProps = new HashMap<>(props);

                IgfsEntryInfo res = meta.append(
                    path,
                    dirProps,
                    create,
                    cfg.getBlockSize(),
                    null/*affKey*/,
                    evictExclude(path, true),
                    fileProps
                );

                assert res != null;

                return new IgfsOutputStreamImpl(igfsCtx, path, res, bufferSize(bufSize), mode, null);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void setTimes(final IgfsPath path, final long accessTime, final long modificationTime) {
        A.notNull(path, "path");

        if (accessTime == -1 && modificationTime == -1)
            return;

        if (meta.isClient()) {
            meta.runClientTask(new IgfsClientSetTimesCallable(cfg.getName(), path, accessTime, modificationTime));

            return;
        }

        safeOp(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgfsMode mode = resolveMode(path);

                if (mode == PROXY) {
                    if (secondaryFs instanceof IgfsSecondaryFileSystemV2)
                        ((IgfsSecondaryFileSystemV2)secondaryFs).setTimes(path, accessTime, modificationTime);
                    else
                        throw new UnsupportedOperationException("setTimes is not supported in PROXY mode for " +
                            "this secondary file system,");
                }
                else {
                    boolean useSecondary =
                        IgfsUtils.isDualMode(mode) && secondaryFs instanceof IgfsSecondaryFileSystemV2;

                    meta.updateTimes(path, accessTime, modificationTime,
                        useSecondary ? (IgfsSecondaryFileSystemV2) secondaryFs : null);
                }

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len) {
        return affinity(path, start, len, 0L);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(final IgfsPath path, final long start, final long len,
        final long maxLen) {
        A.notNull(path, "path");
        A.ensure(start >= 0, "start >= 0");
        A.ensure(len >= 0, "len >= 0");

        if (meta.isClient())
            return meta.runClientTask(new IgfsClientAffinityCallable(cfg.getName(), path, start, len, maxLen));

        return safeOp(new Callable<Collection<IgfsBlockLocation>>() {
            @Override public Collection<IgfsBlockLocation> call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Get affinity for file block [path=" + path + ", start=" + start + ", len=" + len + ']');

                IgfsMode mode = resolveMode(path);

                // Check memory first.
                IgfsEntryInfo info = meta.infoForPath(path);

                if (info == null && mode != PRIMARY) {
                    assert IgfsUtils.isDualMode(mode);
                    assert secondaryFs != null;

                    // Synchronize
                    info = meta.synchronizeFileDual(secondaryFs, path);
                }

                if (info == null)
                    throw new IgfsPathNotFoundException("File not found: " + path);

                if (!info.isFile())
                    throw new IgfsPathIsDirectoryException("Failed to get affinity for path because it is not " +
                        "a file: " + path);

                return data.affinity(info, start, len, maxLen);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgfsMetrics metrics() {
        return safeOp(new Callable<IgfsMetrics>() {
            @Override public IgfsMetrics call() throws Exception {
                IgfsPathSummary sum = summary0(IgfsPath.ROOT);

                long secondarySpaceSize = 0;

                if (secondaryFs != null) {
                    try {
                        secondarySpaceSize = secondaryFs.usedSpaceSize();
                    }
                    catch (IgniteException e) {
                        LT.error(log, e, "Failed to get secondary file system consumed space size.");

                        secondarySpaceSize = -1;
                    }
                }

                IgfsLocalMetrics metrics = igfsCtx.metrics();

                return new IgfsMetricsAdapter(
                    igfsCtx.data().spaceSize(),
                    igfsCtx.data().maxSpaceSize(),
                    secondarySpaceSize,
                    sum.directoriesCount(),
                    sum.filesCount(),
                    metrics.filesOpenedForRead(),
                    metrics.filesOpenedForWrite(),
                    metrics.readBlocks(),
                    metrics.readBlocksSecondary(),
                    metrics.writeBlocks(),
                    metrics.writeBlocksSecondary(),
                    metrics.readBytes(),
                    metrics.readBytesTime(),
                    metrics.writeBytes(),
                    metrics.writeBytesTime());
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        igfsCtx.metrics().reset();
    }

    /** {@inheritDoc} */
    @Override public long size(final IgfsPath path) {
        A.notNull(path, "path");

        if (meta.isClient())
            return meta.runClientTask(new IgfsClientSizeCallable(cfg.getName(), path));

        return safeOp(new Callable<Long>() {
            @Override public Long call() throws Exception {
                return summary0(path).totalLength();
            }
        });
    }

    /**
     * Get summary for path.
     *
     * @param path Path.
     * @return Summary.
     * @throws IgniteCheckedException If failed.
     */
    private IgfsPathSummary summary0(IgfsPath path) throws IgniteCheckedException {
        IgfsFile info = info(path);

        if (info == null)
            throw new IgfsPathNotFoundException("Failed to get path summary (path not found): " + path);

        IgfsPathSummary sum = new IgfsPathSummary(path);

        summaryRecursive(info, sum);

        return sum;
    }

    /**
     * Calculates size of directory or file for given ID.
     *
     * @param file IGFS File object.
     * @param sum Summary object that will collect information.
     * @throws IgniteCheckedException If failed.
     */
    private void summaryRecursive(IgfsFile file, IgfsPathSummary sum) throws IgniteCheckedException {
        assert file != null;
        assert sum != null;

        if (file.isDirectory()) {
            if (!F.eq(IgfsPath.ROOT, file.path()))
                sum.directoriesCount(sum.directoriesCount() + 1);

            for (IgfsFile childFile : listFiles(file.path()))
                summaryRecursive(childFile, sum);
        }
        else {
            sum.filesCount(sum.filesCount() + 1);
            sum.totalLength(sum.totalLength() + file.length());
        }
    }

    /** {@inheritDoc} */
    @Override public void format() {
        try {
            IgniteUuid id = meta.format();

            // If ID is null, then file system is already empty.
            if (id == null)
                return;

            while (true) {
                if (enterBusy()) {
                    try {
                        if (!meta.exists(id))
                            return;
                    }
                    finally {
                        busyLock.leaveBusy();
                    }
                }

                U.sleep(10);
            }
        }
        catch (Exception e) {
            throw IgfsUtils.toIgfsException(e);
        }
    }

    /**
     * Formats the file system removing all existing entries from it.
     *
     * @return Future.
     */
    IgniteInternalFuture<?> formatAsync() {
        GridFutureAdapter<?> fut = new GridFutureAdapter<>();

        Thread t = new Thread(new FormatRunnable(fut), "igfs-format-" + cfg.getName() + "-" +
            FORMAT_THREAD_IDX_GEN.incrementAndGet());

        t.setDaemon(true);

        t.start();

        return fut;
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(IgfsTask<T, R> task, @Nullable IgfsRecordResolver rslvr,
        Collection<IgfsPath> paths, @Nullable T arg) {
        try {
            return executeAsync(task, rslvr, paths, arg).get();
        }
        catch (Exception e) {
            throw IgfsUtils.toIgfsException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(IgfsTask<T, R> task, @Nullable IgfsRecordResolver rslvr,
        Collection<IgfsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg) {
        try {
            return executeAsync(task, rslvr, paths, skipNonExistentFiles, maxRangeLen, arg).get();
        }
        catch (Exception e) {
            throw IgfsUtils.toIgfsException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends IgfsTask<T, R>> taskCls,
        @Nullable IgfsRecordResolver rslvr, Collection<IgfsPath> paths, @Nullable T arg) {
        try {
            return executeAsync(taskCls, rslvr, paths, arg).get();
        }
        catch (Exception e) {
            throw IgfsUtils.toIgfsException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends IgfsTask<T, R>> taskCls,
        @Nullable IgfsRecordResolver rslvr, Collection<IgfsPath> paths, boolean skipNonExistentFiles,
        long maxRangeSize, @Nullable T arg) {
        try {
            return executeAsync(taskCls, rslvr, paths, skipNonExistentFiles, maxRangeSize, arg).get();
        }
        catch (Exception e) {
            throw IgfsUtils.toIgfsException(e);
        }
    }

    /**
     * Executes IGFS task asynchronously.
     *
     * @param task Task to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param arg Optional task argument.
     * @return Execution future.
     */
    <T, R> IgniteInternalFuture<R> executeAsync(IgfsTask<T, R> task, @Nullable IgfsRecordResolver rslvr,
        Collection<IgfsPath> paths, @Nullable T arg) {
        return executeAsync(task, rslvr, paths, true, cfg.getMaximumTaskRangeLength(), arg);
    }

    /**
     * Executes IGFS task with overridden maximum range length (see
     * {@link org.apache.ignite.configuration.FileSystemConfiguration#getMaximumTaskRangeLength()} for more information).
     *
     * @param task Task to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param skipNonExistentFiles Whether to skip non existent files. If set to {@code true} non-existent files will
     *     be ignored. Otherwise an exception will be thrown.
     * @param maxRangeLen Optional maximum range length. If {@code 0}, then by default all consecutive
     *      IGFS blocks will be included.
     * @param arg Optional task argument.
     * @return Execution future.
     */
    <T, R> IgniteInternalFuture<R> executeAsync(IgfsTask<T, R> task, @Nullable IgfsRecordResolver rslvr,
        Collection<IgfsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg) {
        return igfsCtx.kernalContext().task().execute(task, new IgfsTaskArgsImpl<>(cfg.getName(), paths, rslvr,
            skipNonExistentFiles, maxRangeLen, arg));
    }

    /**
     * Executes IGFS task asynchronously.
     *
     * @param taskCls Task class to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param arg Optional task argument.
     * @return Execution future.
     */
    <T, R> IgniteInternalFuture<R> executeAsync(Class<? extends IgfsTask<T, R>> taskCls,
        @Nullable IgfsRecordResolver rslvr, Collection<IgfsPath> paths, @Nullable T arg) {
        return executeAsync(taskCls, rslvr, paths, true, cfg.getMaximumTaskRangeLength(), arg);
    }

    /**
     * Executes IGFS task asynchronously with overridden maximum range length (see
     * {@link org.apache.ignite.configuration.FileSystemConfiguration#getMaximumTaskRangeLength()} for more information).
     *
     * @param taskCls Task class to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param skipNonExistentFiles Whether to skip non existent files. If set to {@code true} non-existent files will
     *     be ignored. Otherwise an exception will be thrown.
     * @param maxRangeLen Maximum range length.
     * @param arg Optional task argument.
     * @return Execution future.
     */
    @SuppressWarnings("unchecked")
    <T, R> IgniteInternalFuture<R> executeAsync(Class<? extends IgfsTask<T, R>> taskCls,
        @Nullable IgfsRecordResolver rslvr, Collection<IgfsPath> paths, boolean skipNonExistentFiles,
        long maxRangeLen, @Nullable T arg) {
        return igfsCtx.kernalContext().task().execute((Class<IgfsTask<T, R>>)taskCls,
            new IgfsTaskArgsImpl<>(cfg.getName(), paths, rslvr, skipNonExistentFiles, maxRangeLen, arg));
    }

    /** {@inheritDoc} */
    @Override public boolean evictExclude(IgfsPath path, boolean primary) {
        assert path != null;

        try {
            // Exclude all PRIMARY files + the ones specified in eviction policy as exclusions.
            return primary || evictPlc == null || evictPlc.exclude(path);
        }
        catch (IgniteCheckedException e) {
            LT.error(log, e, "Failed to check whether the path must be excluded from evictions: " + path);

            return false;
        }
    }

    /**
     * Resolve file info for the given path and the given mode.
     *
     * @param path Path.
     * @param mode Mode.
     * @return File info or {@code null} in case file is not found.
     * @throws Exception If failed.
     */
    private IgfsFileImpl resolveFileInfo(IgfsPath path, IgfsMode mode) throws Exception {
        assert path != null;
        assert mode != null;

        IgfsEntryInfo info = null;

        switch (mode) {
            case PRIMARY:
                info = meta.infoForPath(path);

                break;

            case DUAL_SYNC:
            case DUAL_ASYNC:
                try {
                    IgfsFile status = secondaryFs.info(path);

                    if (status != null)
                        return new IgfsFileImpl(status, data.groupBlockSize());
                }
                catch (Exception e) {
                    U.error(log, "File info operation in DUAL mode failed [path=" + path + ']', e);

                    throw e;
                }

                break;

            default:
                assert mode == PROXY : "Unknown mode: " + mode;

                IgfsFile status = secondaryFs.info(path);

                if (status != null)
                    return new IgfsFileImpl(status, data.groupBlockSize());
                else
                    return null;
        }

        if (info == null)
            return null;

        return new IgfsFileImpl(path, info, data.groupBlockSize());
    }

    /** {@inheritDoc} */
    @Override public IgniteFileSystem withAsync() {
        return new IgfsAsyncImpl(this);
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        throw new IllegalStateException("Asynchronous mode is not enabled.");
    }

    /**
     * Space calculation task.
     */
    @GridInternal
    private static class IgfsGlobalSpaceTask extends ComputeTaskSplitAdapter<Object, IgniteBiTuple<Long, Long>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** IGFS name. */
        private String igfsName;

        /**
         * @param igfsName IGFS name.
         */
        private IgfsGlobalSpaceTask(@Nullable String igfsName) {
            this.igfsName = igfsName;
        }

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            Collection<ComputeJob> res = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                res.add(new ComputeJobAdapter() {
                    /** Injected grid. */
                    @IgniteInstanceResource
                    private Ignite g;

                    @Nullable @Override public IgniteBiTuple<Long, Long> execute() {
                        IgniteFileSystem igfs = ((IgniteKernal)g).context().igfs().igfs(igfsName);

                        if (igfs == null)
                            return F.t(0L, 0L);

                        IgfsMetrics metrics = igfs.metrics();

                        long loc = metrics.localSpaceSize();

                        return F.t(loc, metrics.maxSpaceSize());
                    }
                });
            }

            return res;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Long, Long> reduce(List<ComputeJobResult> results) {
            long used = 0;
            long max = 0;

            for (ComputeJobResult res : results) {
                IgniteBiTuple<Long, Long> data = res.getData();

                if (data != null) {
                    used += data.get1();
                    max += data.get2();
                }
            }

            return F.t(used, max);
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            // Never failover.
            return ComputeJobResultPolicy.WAIT;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid nextAffinityKey() {
        return safeOp(new Callable<IgniteUuid>() {
            @Override public IgniteUuid call() throws Exception {
                return data.nextAffinityKey(null);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean isProxy(URI path) {
        IgfsMode mode = F.isEmpty(cfg.getPathModes()) ? cfg.getDefaultMode() :
            modeRslvr.resolveMode(new IgfsPath(path));

        return mode == PROXY;
    }

    /** {@inheritDoc} */
    @Override public IgfsSecondaryFileSystem asSecondary() {
        return new IgfsSecondaryFileSystemImpl(this);
    }

    /**
     * Resolve mode for the given path.
     *
     * @param path Path.
     * @return Mode.
     */
    private IgfsMode resolveMode(IgfsPath path) {
        return modeRslvr.resolveMode(path);
    }

    /**
     * Perform IGFS operation in safe context.
     *
     * @param act Action.
     * @return Result.
     */
    private <T> T safeOp(Callable<T> act) {
        if (enterBusy()) {
            try {
                return act.call();
            }
            catch (Exception e) {
                throw IgfsUtils.toIgfsException(e);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to perform IGFS action because grid is stopping.");
    }

    /**
     * Get buffer size.
     *
     * @param bufSize Original buffer size.
     * @return Real buffer size.
     */
    private int bufferSize(int bufSize) {
        return bufSize == 0 ? cfg.getStreamBufferSize() : bufSize;
    }

    /**
     * IGFS thread factory.
     */
    @SuppressWarnings("NullableProblems")
    private static class IgfsThreadFactory implements ThreadFactory {
        /** IGFS name. */
        private final String name;

        /** Counter. */
        private final AtomicLong ctr = new AtomicLong();

        /**
         * Constructor.
         *
         * @param name IGFS name.
         */
        private IgfsThreadFactory(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public Thread newThread(Runnable r) {
            Thread t = new Thread(r);

            t.setName("igfs-<" + name + ">-batch-worker-thread-" + ctr.incrementAndGet());
            t.setDaemon(true);

            return t;
        }
    }

    /**
     * Format runnable.
     */
    private class FormatRunnable implements Runnable {
        /** Target future. */
        private final GridFutureAdapter<?> fut;

        /**
         * Constructor.
         *
         * @param fut Future.
         */
        public FormatRunnable(GridFutureAdapter<?> fut) {
            this.fut = fut;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgfsException err = null;

            try {
                format();
            }
            catch (Throwable err0) {
                err = IgfsUtils.toIgfsException(err0);
            }
            finally {
                if (err == null)
                    fut.onDone();
                else
                    fut.onDone(err);
            }
        }
    }
}
