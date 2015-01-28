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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.cache.eviction.ggfs.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.fs.mapreduce.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.apache.ignite.fs.IgniteFsMode.*;
import static org.apache.ignite.internal.GridNodeAttributes.*;
import static org.apache.ignite.internal.GridTopic.*;
import static org.apache.ignite.internal.processors.fs.GridGgfsFileInfo.*;

/**
 * Cache-based GGFS implementation.
 */
public final class GridGgfsImpl implements GridGgfsEx {
    /** Default permissions for file system entry. */
    private static final String PERMISSION_DFLT_VAL = "0777";

    /** Default directory metadata. */
    private static final Map<String, String> DFLT_DIR_META = F.asMap(PROP_PERMISSION, PERMISSION_DFLT_VAL);

    /** Handshake message. */
    private final GridGgfsPaths secondaryPaths;

    /** Cache based structure (meta data) manager. */
    private GridGgfsMetaManager meta;

    /** Cache based file's data container. */
    private GridGgfsDataManager data;

    /** FS configuration. */
    private IgniteFsConfiguration cfg;

    /** Ggfs context. */
    private GridGgfsContext ggfsCtx;

    /** Event storage manager. */
    private GridEventStorageManager evts;

    /** Local node. */
    private ClusterNode locNode;

    /** Logger. */
    private IgniteLogger log;

    /** Mode resolver. */
    private final GridGgfsModeResolver modeRslvr;

    /** Connection to the secondary file system. */
    private IgniteFsFileSystem secondaryFs;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Writers map. */
    private final ConcurrentHashMap8<IgniteFsPath, GridGgfsFileWorker> workerMap = new ConcurrentHashMap8<>();

    /** Delete futures. */
    private final ConcurrentHashMap8<IgniteUuid, GridFutureAdapter<Object>> delFuts = new ConcurrentHashMap8<>();

    /** Delete message listener. */
    private final GridMessageListener delMsgLsnr = new FormatMessageListener();

    /** Format discovery listener. */
    private final GridLocalEventListener delDiscoLsnr = new FormatDiscoveryListener();

    /** Local metrics holder. */
    private final GridGgfsLocalMetrics metrics = new GridGgfsLocalMetrics();

    /** Client log directory. */
    private volatile String logDir;

    /** Message topic. */
    private Object topic;

    /** Eviction policy (if set). */
    private CacheGgfsPerBlockLruEvictionPolicy evictPlc;

    /**
     * Creates GGFS instance with given context.
     *
     * @param ggfsCtx Context.
     * @throws IgniteCheckedException In case of error.
     */
    GridGgfsImpl(GridGgfsContext ggfsCtx) throws IgniteCheckedException {
        assert ggfsCtx != null;

        this.ggfsCtx = ggfsCtx;

        cfg = ggfsCtx.configuration();
        log = ggfsCtx.kernalContext().log(GridGgfsImpl.class);
        evts = ggfsCtx.kernalContext().event();
        meta = ggfsCtx.meta();
        data = ggfsCtx.data();
        secondaryFs = cfg.getSecondaryFileSystem();

        /* Default GGFS mode. */
        IgniteFsMode dfltMode;

        if (secondaryFs == null) {
            if (cfg.getDefaultMode() == PROXY)
                throw new IgniteCheckedException("Mode cannot be PROXY if secondary file system hasn't been defined.");

            dfltMode = PRIMARY;
        }
        else
            dfltMode = cfg.getDefaultMode();

        Map<String, IgniteFsMode> cfgModes = new LinkedHashMap<>();
        Map<String, IgniteFsMode> dfltModes = new LinkedHashMap<>(4, 1.0f);

        dfltModes.put("/gridgain/primary", PRIMARY);

        if (secondaryFs != null) {
            dfltModes.put("/gridgain/proxy", PROXY);
            dfltModes.put("/gridgain/sync", DUAL_SYNC);
            dfltModes.put("/gridgain/async", DUAL_ASYNC);
        }

        cfgModes.putAll(dfltModes);

        if (ggfsCtx.configuration().getPathModes() != null) {
            for (Map.Entry<String, IgniteFsMode> e : ggfsCtx.configuration().getPathModes().entrySet()) {
                if (!dfltModes.containsKey(e.getKey()))
                    cfgModes.put(e.getKey(), e.getValue());
                else
                    U.warn(log, "Ignoring path mode because it conflicts with GridGain reserved path " +
                        "(use another path) [mode=" + e.getValue() + ", path=" + e.getKey() + ']');
            }
        }

        ArrayList<T2<IgniteFsPath, IgniteFsMode>> modes = null;

        if (!cfgModes.isEmpty()) {
            modes = new ArrayList<>(cfgModes.size());

            for (Map.Entry<String, IgniteFsMode> mode : cfgModes.entrySet()) {
                IgniteFsMode mode0 = secondaryFs == null ? mode.getValue() == PROXY ? PROXY : PRIMARY : mode.getValue();

                try {
                    modes.add(new T2<>(new IgniteFsPath(mode.getKey()), mode0));
                }
                catch (IllegalArgumentException e) {
                    throw new IgniteCheckedException("Invalid path found in mode pattern: " + mode.getKey(), e);
                }
            }
        }

        modeRslvr = new GridGgfsModeResolver(dfltMode, modes);

        secondaryPaths = new GridGgfsPaths(secondaryFs == null ? null : secondaryFs.properties(), dfltMode,
            modeRslvr.modesOrdered());

        // Check whether GGFS LRU eviction policy is set on data cache.
        String dataCacheName = ggfsCtx.configuration().getDataCacheName();

        for (CacheConfiguration cacheCfg : ggfsCtx.kernalContext().config().getCacheConfiguration()) {
            if (F.eq(dataCacheName, cacheCfg.getName())) {
                CacheEvictionPolicy evictPlc = cacheCfg.getEvictionPolicy();

                if (evictPlc != null & evictPlc instanceof CacheGgfsPerBlockLruEvictionPolicy)
                    this.evictPlc = (CacheGgfsPerBlockLruEvictionPolicy)evictPlc;

                break;
            }
        }

        topic = F.isEmpty(name()) ? TOPIC_GGFS : TOPIC_GGFS.topic(name());

        ggfsCtx.kernalContext().io().addMessageListener(topic, delMsgLsnr);
        ggfsCtx.kernalContext().event().addLocalEventListener(delDiscoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /**
     * @return Local node.
     */
    private ClusterNode localNode() {
        if (locNode == null)
            locNode = ggfsCtx.kernalContext().discovery().localNode();

        return locNode;
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        busyLock.block();

        // Clear interrupted flag temporarily.
        boolean interrupted = Thread.interrupted();

        // Force all workers to finish their batches.
        for (GridGgfsFileWorker w : workerMap.values())
            w.cancel();

        // Wait for all writers to finish their execution.
        for (GridGgfsFileWorker w : workerMap.values()) {
            try {
                w.join();
            }
            catch (InterruptedException e) {
                U.error(log, e.getMessage(), e);
            }
        }

        workerMap.clear();

        if (secondaryFs instanceof AutoCloseable)
            U.closeQuiet((AutoCloseable)secondaryFs);

        ggfsCtx.kernalContext().io().removeMessageListener(topic, delMsgLsnr);
        ggfsCtx.kernalContext().event().removeLocalEventListener(delDiscoLsnr);

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
    private GridGgfsFileWorkerBatch newBatch(final IgniteFsPath path, OutputStream out) throws IgniteCheckedException {
        assert path != null;
        assert out != null;

        if (enterBusy()) {
            try {
                GridGgfsFileWorkerBatch batch = new GridGgfsFileWorkerBatch(path, out);

                while (true) {
                    GridGgfsFileWorker worker = workerMap.get(path);

                    if (worker != null) {
                        if (worker.addBatch(batch)) // Added batch to active worker.
                            break;
                        else
                            workerMap.remove(path, worker); // Worker is stopping. Remove it from map.
                    }
                    else {
                        worker = new GridGgfsFileWorker("ggfs-file-worker-" + path) {
                            @Override protected void onFinish() {
                                workerMap.remove(path, this);
                            }
                        };

                        boolean b = worker.addBatch(batch);

                        assert b;

                        if (workerMap.putIfAbsent(path, worker) == null) {
                            worker.start();

                            break;
                        }
                    }
                }

                return batch;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IgniteCheckedException("Cannot create new output stream to the secondary file system because GGFS is " +
                "stopping: " + path);
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

    /**
     * Await for any pending finished writes on the children paths.
     *
     * @param paths Paths to check.
     */
    void await(IgniteFsPath... paths) {
        assert paths != null;

        for (Map.Entry<IgniteFsPath, GridGgfsFileWorker> workerEntry : workerMap.entrySet()) {
            IgniteFsPath workerPath = workerEntry.getKey();

            boolean await = false;

            for (IgniteFsPath path : paths) {
                if (workerPath.isSubDirectoryOf(path) || workerPath.isSame(path))  {
                    await = true;

                    break;
                }
            }

            if (await) {
                GridGgfsFileWorkerBatch batch = workerEntry.getValue().currentBatch();

                if (batch != null) {
                    try {
                        batch.awaitIfFinished();
                    }
                    catch (IgniteCheckedException ignore) {
                        // No-op.
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public GridGgfsContext context() {
        return ggfsCtx;
    }

    /**
     * @return Mode resolver.
     */
    GridGgfsModeResolver modeResolver() {
        return modeRslvr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String name() {
        return cfg.getName();
    }

    /** {@inheritDoc} */
    @Override public IgniteFsConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public GridGgfsPaths proxyPaths() {
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
    @Override public GridGgfsStatus globalSpace() throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                IgniteBiTuple<Long, Long> space = ggfsCtx.kernalContext().grid().compute().execute(
                    new GgfsGlobalSpaceTask(name()), null);

                return new GridGgfsStatus(space.get1(), space.get2());
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get global space because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public void globalSampling(@Nullable Boolean val) throws IgniteCheckedException {
        if (enterBusy()) {
            try {
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
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to set global sampling flag because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override @Nullable public Boolean globalSampling() {
        if (enterBusy()) {
            try {
                try {
                    return meta.sampling();
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to get sampling state.", e);

                    return false;
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get global sampling flag because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public GridGgfsLocalMetrics localMetrics() {
        return metrics;
    }

    /** {@inheritDoc} */
    @Override public long groupBlockSize() {
        return data.groupBlockSize();
    }

    /** {@inheritDoc} */
    @Override public boolean exists(IgniteFsPath path) throws IgniteCheckedException {
        A.notNull(path, "path");

        if (log.isDebugEnabled())
            log.debug("Check file exists: " + path);

        IgniteFsMode mode = modeRslvr.resolveMode(path);

        if (mode == PROXY)
            throw new IgniteCheckedException("PROXY mode cannot be used in GGFS directly: " + path);

        boolean res = false;

        switch (mode) {
            case PRIMARY:
                res = meta.fileId(path) != null;

                break;

            case DUAL_SYNC:
            case DUAL_ASYNC:
                res = meta.fileId(path) != null;

                if (!res)
                    res = secondaryFs.exists(path);

                break;

            default:
                assert false : "Unknown mode.";
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgniteFsFile info(IgniteFsPath path) throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");

                if (log.isDebugEnabled())
                    log.debug("Get file info: " + path);

                IgniteFsMode mode = modeRslvr.resolveMode(path);

                if (mode == PROXY)
                    throw new IgniteCheckedException("PROXY mode cannot be used in GGFS directly: " + path);

                GridGgfsFileInfo info = resolveFileInfo(path, mode);

                if (info == null)
                    return null;

                return new IgniteFsFileImpl(path, info, data.groupBlockSize());
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get path info because grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFsPathSummary summary(IgniteFsPath path) throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");

                if (log.isDebugEnabled())
                    log.debug("Calculating path summary: " + path);

                IgniteUuid fileId = meta.fileId(path);

                if (fileId == null)
                    throw new IgniteFsFileNotFoundException("Failed to get path summary (path not found): " + path);

                IgniteFsPathSummary sum = new IgniteFsPathSummary(path);

                summary0(fileId, sum);

                return sum;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get path summary because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFsFile update(IgniteFsPath path, Map<String, String> props) throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");
                A.notNull(props, "props");
                A.ensure(!props.isEmpty(), "!props.isEmpty()");

                if (log.isDebugEnabled())
                    log.debug("Set file properties [path=" + path + ", props=" + props + ']');

                IgniteFsMode mode = modeRslvr.resolveMode(path);

                if (mode == PROXY)
                    throw new IgniteCheckedException("PROXY mode cannot be used in GGFS directly: " + path);
                else if (mode != PRIMARY) {
                    assert mode == DUAL_SYNC || mode == DUAL_ASYNC;

                    await(path);

                    GridGgfsFileInfo info = meta.updateDual(secondaryFs, path, props);

                    if (info == null)
                        return null;

                    return new IgniteFsFileImpl(path, info, data.groupBlockSize());
                }

                List<IgniteUuid> fileIds = meta.fileIds(path);

                IgniteUuid fileId = fileIds.get(fileIds.size() - 1);

                if (fileId == null)
                    return null;

                IgniteUuid parentId = fileIds.size() > 1 ? fileIds.get(fileIds.size() - 2) : null;

                GridGgfsFileInfo info = meta.updateProperties(parentId, fileId, path.name(), props);

                if (info != null) {
                    if (evts.isRecordable(EVT_GGFS_META_UPDATED))
                        evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_META_UPDATED, props));

                    return new IgniteFsFileImpl(path, info, data.groupBlockSize());
                }
                else
                    return null;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to update file because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public void rename(IgniteFsPath src, IgniteFsPath dest) throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(src, "src");
                A.notNull(dest, "dest");

                if (log.isDebugEnabled())
                    log.debug("Rename file [src=" + src + ", dest=" + dest + ']');

                IgniteFsMode mode = modeRslvr.resolveMode(src);
                Set<IgniteFsMode> childrenModes = modeRslvr.resolveChildrenModes(src);

                if (mode == PROXY)
                    throw new IgniteCheckedException("PROXY mode cannot be used in GGFS directly: " + src);

                if (src.equals(dest))
                    return; // Rename to itself is a no-op.

                // Cannot rename root directory.
                if (src.parent() == null)
                    throw new IgniteFsInvalidPathException("Failed to rename root directory.");

                // Cannot move directory of upper level to self sub-dir.
                if (dest.isSubDirectoryOf(src))
                    throw new IgniteFsInvalidPathException("Failed to rename directory (cannot move directory of " +
                        "upper level to self sub-dir) [src=" + src + ", dest=" + dest + ']');

                if (evictExclude(src, mode == PRIMARY) != evictExclude(dest, modeRslvr.resolveMode(dest) == PRIMARY))
                    throw new IgniteFsInvalidPathException("Cannot move file to a path with different eviction " +
                        "exclude setting (need to copy and remove)");

                if (!childrenModes.equals(Collections.singleton(PRIMARY))) {
                    assert mode == DUAL_SYNC || mode == DUAL_ASYNC;

                    await(src, dest);

                    meta.renameDual(secondaryFs, src, dest);

                    return;
                }

                IgniteFsPath destParent = dest.parent();

                // Resolve source file info.
                FileDescriptor srcDesc = getFileDescriptor(src);

                // File not found.
                if (srcDesc == null || srcDesc.parentId == null) {
                    if (mode == PRIMARY)
                        checkConflictWithPrimary(src);

                    throw new IgniteFsFileNotFoundException("Failed to rename (source path not found): " + src);
                }

                String srcFileName = src.name();

                // Resolve destination file info.
                FileDescriptor destDesc = getFileDescriptor(dest);

                String destFileName;

                boolean newDest = destDesc == null;

                if (newDest) {
                    assert destParent != null;

                    // Use parent directory for destination parent and destination path name as destination name.
                    destDesc = getFileDescriptor(destParent);

                    // Destination directory doesn't exist.
                    if (destDesc == null)
                        throw new IgniteFsFileNotFoundException("Failed to rename (destination directory does not " +
                            "exist): " + dest);

                    destFileName = dest.name();
                }
                else
                    // Use destination directory for destination parent and source path name as destination name.
                    destFileName = srcFileName;

                // Can move only into directory, but not into file.
                if (destDesc.isFile)
                    throw new IgniteFsParentNotDirectoryException("Failed to rename (destination is not a directory): "
                        + dest);

                meta.move(srcDesc.fileId, srcFileName, srcDesc.parentId, destFileName, destDesc.fileId);

                if (srcDesc.isFile) { // Renamed a file.
                    if (evts.isRecordable(EVT_GGFS_FILE_RENAMED))
                        evts.record(new IgniteFsEvent(
                            src,
                            newDest ? dest : new IgniteFsPath(dest, destFileName),
                            localNode(),
                            EVT_GGFS_FILE_RENAMED));
                }
                else { // Renamed a directory.
                    if (evts.isRecordable(EVT_GGFS_DIR_RENAMED))
                        evts.record(new IgniteFsEvent(src, dest, localNode(), EVT_GGFS_DIR_RENAMED));
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to set rename path because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public boolean delete(IgniteFsPath path, boolean recursive) throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");

                if (log.isDebugEnabled())
                    log.debug("Deleting file [path=" + path + ", recursive=" + recursive + ']');

                IgniteFsMode mode = modeRslvr.resolveMode(path);
                Set<IgniteFsMode> childrenModes = modeRslvr.resolveChildrenModes(path);

                if (mode == PROXY)
                    throw new IgniteCheckedException("PROXY mode cannot be used in GGFS directly: " + path);

                boolean res = false;

                FileDescriptor desc = getFileDescriptor(path);

                if (childrenModes.contains(PRIMARY)) {
                    if (desc != null)
                        res = delete0(desc, path.parent(), recursive);
                    else if (mode == PRIMARY)
                        checkConflictWithPrimary(path);
                }

                if (childrenModes.contains(DUAL_SYNC) || childrenModes.contains(DUAL_ASYNC)) {
                    assert secondaryFs != null;

                    await(path);

                    res |= meta.deleteDual(secondaryFs, path, recursive);
                }

                // Record event if needed.
                if (res && desc != null) {
                    if (desc.isFile) {
                        if (evts.isRecordable(EVT_GGFS_FILE_DELETED))
                            evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_FILE_DELETED));
                    }
                    else if (evts.isRecordable(EVT_GGFS_DIR_DELETED))
                        evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_DIR_DELETED));
                }

                return res;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to set file times because Grid is stopping.");
    }

    /**
     * Internal procedure for (optionally) recursive file and directory deletion.
     *
     * @param desc File descriptor of file or directory to delete.
     * @param parentPath Parent path. If specified, events will be fired for each deleted file
     *      or directory. If not specified, events will not be fired.
     * @param recursive Recursive deletion flag.
     * @return {@code True} if file was successfully deleted. If directory is not empty and
     *      {@code recursive} flag is false, will return {@code false}.
     * @throws IgniteCheckedException In case of error.
     */
    private boolean delete0(FileDescriptor desc, @Nullable IgniteFsPath parentPath, boolean recursive)
        throws IgniteCheckedException {
        IgniteFsPath curPath = parentPath == null ? new IgniteFsPath() : new IgniteFsPath(parentPath, desc.fileName);

        if (desc.isFile) {
            deleteFile(curPath, desc, true);

            return true;
        }
        else {
            if (recursive) {
                meta.softDelete(desc.parentId, desc.fileName, desc.fileId);

                return true;
            }
            else {
                Map<String, GridGgfsListingEntry> infoMap = meta.directoryListing(desc.fileId);

                if (F.isEmpty(infoMap)) {
                    deleteFile(curPath, desc, true);

                    return true;
                }
                else
                    // Throw exception if not empty and not recursive.
                    throw new GridGgfsDirectoryNotEmptyException("Failed to remove directory (directory is not empty " +
                        "and recursive flag is not set)");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgniteFsPath path) throws IgniteCheckedException {
        mkdirs(path, null);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgniteFsPath path, @Nullable Map<String, String> props) throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");

                if (log.isDebugEnabled())
                    log.debug("Make directories: " + path);

                if (props == null)
                    props = DFLT_DIR_META;

                IgniteFsMode mode = modeRslvr.resolveMode(path);

                if (mode == PROXY)
                    throw new IgniteCheckedException("PROXY mode cannot be used in GGFS directly: " + path);
                else if (mode != PRIMARY) {
                    assert mode == DUAL_SYNC || mode == DUAL_ASYNC;

                    await(path);

                    meta.mkdirsDual(secondaryFs, path, props);

                    return;
                }

                List<IgniteUuid> ids = meta.fileIds(path);
                List<String> components = path.components();

                assert ids.size() == components.size() + 1 : "Components doesn't contain ROOT element" +
                    " [ids=" + ids + ", components=" + components + ']';

                IgniteUuid parentId = ROOT_ID;

                IgniteFsPath curPath = path.root();

                for (int step = 0, size = components.size(); step < size; step++) {
                    IgniteUuid fileId = ids.get(step + 1); // Skip the first ROOT element.

                    if (fileId == null) {
                        GridGgfsFileInfo fileInfo = new GridGgfsFileInfo(true, props); // Create new directory.

                        String fileName = components.get(step); // Get current component name.

                        curPath = new IgniteFsPath(curPath, fileName);

                        try {
                            // Fails only if parent is not a directory or if modified concurrently.
                            IgniteUuid oldId = meta.putIfAbsent(parentId, fileName, fileInfo);

                            fileId = oldId == null ? fileInfo.id() : oldId; // Update node ID.

                            if (oldId == null && evts.isRecordable(EVT_GGFS_DIR_CREATED))
                                evts.record(new IgniteFsEvent(curPath, localNode(), EVT_GGFS_DIR_CREATED));
                        }
                        catch (IgniteCheckedException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to create directory [path=" + path + ", parentId=" + parentId +
                                    ", fileName=" + fileName + ", step=" + step + ", e=" + e.getMessage() + ']');

                            // Check directory with such name already exists.
                            GridGgfsFileInfo stored = meta.info(meta.fileId(parentId, fileName));

                            if (stored == null)
                                throw new IgniteFsException(e);

                            if (!stored.isDirectory())
                                throw new IgniteFsParentNotDirectoryException("Failed to create directory (parent " +
                                    "element is not a directory)");

                            fileId = stored.id(); // Update node ID.
                        }
                    }

                    assert fileId != null;

                    parentId = fileId;
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to set file times because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsPath> listPaths(final IgniteFsPath path) throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");

                if (log.isDebugEnabled())
                    log.debug("List directory: " + path);

                IgniteFsMode mode = modeRslvr.resolveMode(path);

                if (mode == PROXY)
                    throw new IgniteCheckedException("PROXY mode cannot be used in GGFS directly: " + path);

                Set<IgniteFsMode> childrenModes = modeRslvr.resolveChildrenModes(path);

                Collection<String> files = new HashSet<>();

                if (childrenModes.contains(DUAL_SYNC) || childrenModes.contains(DUAL_ASYNC)) {
                    assert secondaryFs != null;

                    Collection<IgniteFsPath> children = secondaryFs.listPaths(path);

                    for (IgniteFsPath child : children)
                        files.add(child.name());
                }

                IgniteUuid fileId = meta.fileId(path);

                if (fileId != null)
                    files.addAll(meta.directoryListing(fileId).keySet());
                else if (mode == PRIMARY) {
                    checkConflictWithPrimary(path);

                    throw new IgniteFsFileNotFoundException("Failed to list files (path not found): " + path);
                }

                return F.viewReadOnly(files, new C1<String, IgniteFsPath>() {
                    @Override
                    public IgniteFsPath apply(String e) {
                        return new IgniteFsPath(path, e);
                    }
                });
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to set file times because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsFile> listFiles(final IgniteFsPath path) throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");

                if (log.isDebugEnabled())
                    log.debug("List directory details: " + path);

                IgniteFsMode mode = modeRslvr.resolveMode(path);

                if (mode == PROXY)
                    throw new IgniteCheckedException("PROXY mode cannot be used in GGFS directly: " + path);

                Set<IgniteFsMode> childrenModes = modeRslvr.resolveChildrenModes(path);

                Collection<IgniteFsFile> files = new HashSet<>();

                if (childrenModes.contains(DUAL_SYNC) || childrenModes.contains(DUAL_ASYNC)) {
                    assert secondaryFs != null;

                    Collection<IgniteFsFile> children = secondaryFs.listFiles(path);

                    for (IgniteFsFile child : children) {
                        GridGgfsFileInfo fsInfo = new GridGgfsFileInfo(cfg.getBlockSize(), child.length(),
                            evictExclude(path, false), child.properties());

                        files.add(new IgniteFsFileImpl(child.path(), fsInfo, data.groupBlockSize()));
                    }
                }

                IgniteUuid fileId = meta.fileId(path);

                if (fileId != null) {
                    GridGgfsFileInfo info = meta.info(fileId);

                    // Handle concurrent deletion.
                    if (info != null) {
                        if (info.isFile())
                            // If this is a file, return its description.
                            return Collections.<IgniteFsFile>singleton(new IgniteFsFileImpl(path, info,
                                data.groupBlockSize()));

                        // Perform the listing.
                        for (Map.Entry<String, GridGgfsListingEntry> e : info.listing().entrySet()) {
                            IgniteFsPath p = new IgniteFsPath(path, e.getKey());

                            files.add(new IgniteFsFileImpl(p, e.getValue(), data.groupBlockSize()));
                        }
                    }
                }
                else if (mode == PRIMARY) {
                    checkConflictWithPrimary(path);

                    throw new IgniteFsFileNotFoundException("Failed to list files (path not found): " + path);
                }

                return files;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to set file times because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() throws IgniteCheckedException {
        return metrics().localSpaceSize();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsInputStreamAdapter open(IgniteFsPath path) throws IgniteCheckedException {
        return open(path, cfg.getStreamBufferSize(), cfg.getSequentialReadsBeforePrefetch());
    }

    /** {@inheritDoc} */
    @Override public GridGgfsInputStreamAdapter open(IgniteFsPath path, int bufSize) throws IgniteCheckedException {
        return open(path, bufSize, cfg.getSequentialReadsBeforePrefetch());
    }

    /** {@inheritDoc} */
    @Override public GridGgfsInputStreamAdapter open(IgniteFsPath path, int bufSize, int seqReadsBeforePrefetch)
        throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");
                A.ensure(bufSize >= 0, "bufSize >= 0");
                A.ensure(seqReadsBeforePrefetch >= 0, "seqReadsBeforePrefetch >= 0");

                if (log.isDebugEnabled())
                    log.debug("Open file for reading [path=" + path + ", bufSize=" + bufSize + ']');

                if (bufSize == 0)
                    bufSize = cfg.getStreamBufferSize();

                IgniteFsMode mode = modeRslvr.resolveMode(path);

                if (mode == PROXY)
                    throw new IgniteCheckedException("PROXY mode cannot be used in GGFS directly: " + path);
                else if (mode != PRIMARY) {
                    assert mode == DUAL_SYNC || mode == DUAL_ASYNC;

                    GridGgfsSecondaryInputStreamDescriptor desc = meta.openDual(secondaryFs, path, bufSize);

                    GgfsEventAwareInputStream os = new GgfsEventAwareInputStream(ggfsCtx, path, desc.info(),
                        cfg.getPrefetchBlocks(), seqReadsBeforePrefetch, desc.reader(), metrics);

                    if (evts.isRecordable(EVT_GGFS_FILE_OPENED_READ))
                        evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_FILE_OPENED_READ));

                    return os;
                }

                GridGgfsFileInfo info = meta.info(meta.fileId(path));

                if (info == null) {
                    checkConflictWithPrimary(path);

                    throw new IgniteFsFileNotFoundException("File not found: " + path);
                }

                if (!info.isFile())
                    throw new IgniteFsInvalidPathException("Failed to open file (not a file): " + path);

                // Input stream to read data from grid cache with separate blocks.
                GgfsEventAwareInputStream os = new GgfsEventAwareInputStream(ggfsCtx, path, info,
                    cfg.getPrefetchBlocks(), seqReadsBeforePrefetch, null, metrics);

                if (evts.isRecordable(EVT_GGFS_FILE_OPENED_READ))
                    evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_FILE_OPENED_READ));

                return os;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to open file because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream create(IgniteFsPath path, boolean overwrite) throws IgniteCheckedException {
        return create0(path, cfg.getStreamBufferSize(), overwrite, null, 0, null, true);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream create(IgniteFsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) throws IgniteCheckedException {
        return create0(path, bufSize, overwrite, null, replication, props, false);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream create(IgniteFsPath path, int bufSize, boolean overwrite,
        @Nullable IgniteUuid affKey, int replication, long blockSize, @Nullable Map<String, String> props)
        throws IgniteCheckedException {
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
     * @throws IgniteCheckedException If file creation failed.
     */
    private IgniteFsOutputStream create0(
        final IgniteFsPath path,
        final int bufSize,
        final boolean overwrite,
        @Nullable IgniteUuid affKey,
        final int replication,
        @Nullable Map<String, String> props,
        final boolean simpleCreate
    ) throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");
                A.ensure(bufSize >= 0, "bufSize >= 0");

                if (log.isDebugEnabled())
                    log.debug("Open file for writing [path=" + path + ", bufSize=" + bufSize + ", overwrite=" +
                        overwrite + ", props=" + props + ']');

                IgniteFsMode mode = modeRslvr.resolveMode(path);

                GridGgfsFileWorkerBatch batch = null;

                if (mode == PROXY)
                    throw new IgniteCheckedException("PROXY mode cannot be used in GGFS directly: " + path);
                else if (mode != PRIMARY) {
                    assert mode == DUAL_SYNC || mode == DUAL_ASYNC;

                    await(path);

                    GridGgfsSecondaryOutputStreamDescriptor desc = meta.createDual(secondaryFs, path, simpleCreate,
                        props, overwrite, bufSize, (short)replication, groupBlockSize(), affKey);

                    batch = newBatch(path, desc.out());

                    GgfsEventAwareOutputStream os = new GgfsEventAwareOutputStream(path, desc.info(), desc.parentId(),
                        bufSize == 0 ? cfg.getStreamBufferSize() : bufSize, mode, batch);

                    if (evts.isRecordable(EVT_GGFS_FILE_OPENED_WRITE))
                        evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_FILE_OPENED_WRITE));

                    return os;
                }

                // Re-create parents when working in PRIMARY mode. In DUAL mode this is done by MetaManager.
                IgniteFsPath parent = path.parent();

                // Create missing parent directories if necessary.
                if (parent != null)
                    mkdirs(parent, props);

                List<IgniteUuid> ids = meta.fileIds(path);

                // Resolve parent ID for file.
                IgniteUuid parentId = ids.size() >= 2 ? ids.get(ids.size() - 2) : null;

                if (parentId == null)
                    throw new IgniteFsInvalidPathException("Failed to resolve parent directory: " + path);

                String fileName = path.name();

                // Constructs new file info.
                GridGgfsFileInfo info = new GridGgfsFileInfo(cfg.getBlockSize(), affKey, evictExclude(path, true),
                    props);

                // Add new file into tree structure.
                while (true) {
                    IgniteUuid oldId = meta.putIfAbsent(parentId, fileName, info);

                    if (oldId == null)
                        break;

                    if (!overwrite)
                        throw new IgniteFsPathAlreadyExistsException("Failed to create file (file already exists): " +
                            path);

                    GridGgfsFileInfo oldInfo = meta.info(oldId);

                    if (oldInfo.isDirectory())
                        throw new IgniteFsPathAlreadyExistsException("Failed to create file (path points to a " +
                            "directory): " + path);

                    // Remove old file from the tree.
                    // Only one file is deleted, so we use internal data loader.
                    deleteFile(path, new FileDescriptor(parentId, fileName, oldId, oldInfo.isFile()), false);

                    if (evts.isRecordable(EVT_GGFS_FILE_DELETED))
                        evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_FILE_DELETED));
                }

                if (evts.isRecordable(EVT_GGFS_FILE_CREATED))
                    evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_FILE_CREATED));

                info = meta.lock(info.id());

                GgfsEventAwareOutputStream os = new GgfsEventAwareOutputStream(path, info, parentId,
                    bufSize == 0 ? cfg.getStreamBufferSize() : bufSize, mode, batch);

                if (evts.isRecordable(EVT_GGFS_FILE_OPENED_WRITE))
                    evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_FILE_OPENED_WRITE));

                return os;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to create file times because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream append(IgniteFsPath path, boolean create) throws IgniteCheckedException {
        return append(path, cfg.getStreamBufferSize(), create, null);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream append(final IgniteFsPath path, final int bufSize, boolean create,
        @Nullable Map<String, String> props) throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");
                A.ensure(bufSize >= 0, "bufSize >= 0");

                if (log.isDebugEnabled())
                    log.debug("Open file for appending [path=" + path + ", bufSize=" + bufSize + ", create=" + create +
                        ", props=" + props + ']');

                IgniteFsMode mode = modeRslvr.resolveMode(path);

                GridGgfsFileWorkerBatch batch = null;

                if (mode == PROXY)
                    throw new IgniteCheckedException("PROXY mode cannot be used in GGFS directly: " + path);
                else if (mode != PRIMARY) {
                    assert mode == DUAL_SYNC || mode == DUAL_ASYNC;

                    await(path);

                    GridGgfsSecondaryOutputStreamDescriptor desc = meta.appendDual(secondaryFs, path, bufSize);

                    batch = newBatch(path, desc.out());

                    return new GgfsEventAwareOutputStream(path, desc.info(), desc.parentId(),
                        bufSize == 0 ? cfg.getStreamBufferSize() : bufSize, mode, batch);
                }

                List<IgniteUuid> ids = meta.fileIds(path);

                GridGgfsFileInfo info = meta.info(ids.get(ids.size() - 1));

                // Resolve parent ID for the file.
                IgniteUuid parentId = ids.size() >= 2 ? ids.get(ids.size() - 2) : null;

                if (info == null) {
                    if (!create) {
                        checkConflictWithPrimary(path);

                        throw new IgniteFsFileNotFoundException("File not found: " + path);
                    }

                    if (parentId == null)
                        throw new IgniteFsInvalidPathException("Failed to resolve parent directory: " + path);

                    info = new GridGgfsFileInfo(cfg.getBlockSize(), /**affinity key*/null, evictExclude(path,
                        mode == PRIMARY), props);

                    IgniteUuid oldId = meta.putIfAbsent(parentId, path.name(), info);

                    if (oldId != null)
                        info = meta.info(oldId);

                    if (evts.isRecordable(EVT_GGFS_FILE_CREATED))
                        evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_FILE_CREATED));
                }

                if (!info.isFile())
                    throw new IgniteFsInvalidPathException("Failed to open file (not a file): " + path);

                info = meta.lock(info.id());

                if (evts.isRecordable(EVT_GGFS_FILE_OPENED_WRITE))
                    evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_FILE_OPENED_WRITE));

                return new GgfsEventAwareOutputStream(path, info, parentId, bufSize == 0 ?
                    cfg.getStreamBufferSize() : bufSize, mode, batch);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to append file times because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public void setTimes(IgniteFsPath path, long accessTime, long modificationTime)
        throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");

                if (accessTime == -1 && modificationTime == -1)
                    return;

                FileDescriptor desc = getFileDescriptor(path);

                if (desc == null) {
                    checkConflictWithPrimary(path);

                    throw new IgniteFsFileNotFoundException("Failed to update times (path not found): " + path);
                }

                // Cannot update times for root.
                if (desc.parentId == null)
                    return;

                meta.updateTimes(desc.parentId, desc.fileId, desc.fileName, accessTime, modificationTime);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to set file times because Grid is stopping.");
    }

    /**
     * Checks if given path exists in secondary file system and throws exception if so.
     *
     * @param path Path to check.
     * @throws IgniteCheckedException If path exists.
     */
    private void checkConflictWithPrimary(IgniteFsPath path) throws IgniteCheckedException {
        if (secondaryFs != null) {
            if (secondaryFs.info(path) != null) {
                throw new IgniteCheckedException("Path mapped to a PRIMARY mode found in secondary file " +
                     "system. Remove path from secondary file system or change path mapping: " + path);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsBlockLocation> affinity(IgniteFsPath path, long start, long len)
        throws IgniteCheckedException {
        return affinity(path, start, len, 0L);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsBlockLocation> affinity(IgniteFsPath path, long start, long len, long maxLen)
        throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");
                A.ensure(start >= 0, "start >= 0");
                A.ensure(len >= 0, "len >= 0");

                if (log.isDebugEnabled())
                    log.debug("Get affinity for file block [path=" + path + ", start=" + start + ", len=" + len + ']');

                IgniteFsMode mode = modeRslvr.resolveMode(path);

                if (mode == PROXY)
                    throw new IgniteCheckedException("PROXY mode cannot be used in GGFS directly: " + path);

                // Check memory first.
                IgniteUuid fileId = meta.fileId(path);
                GridGgfsFileInfo info = meta.info(fileId);

                if (info == null && mode != PRIMARY) {
                    assert mode == DUAL_SYNC || mode == DUAL_ASYNC;
                    assert secondaryFs != null;

                    // Synchronize
                    info = meta.synchronizeFileDual(secondaryFs, path);
                }

                if (info == null)
                    throw new IgniteFsFileNotFoundException("File not found: " + path);

                if (!info.isFile())
                    throw new IgniteFsInvalidPathException("Failed to get affinity info for file (not a file): " +
                        path);

                return data.affinity(info, start, len, maxLen);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get affinity because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFsMetrics metrics() throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                IgniteFsPathSummary sum = new IgniteFsPathSummary();

                summary0(ROOT_ID, sum);

                long secondarySpaceSize = 0;

                if (secondaryFs != null) {
                    try {
                        secondarySpaceSize = secondaryFs.usedSpaceSize();
                    }
                    catch (IgniteCheckedException e) {
                        LT.warn(log, e, "Failed to get secondary file system consumed space size.");

                        secondarySpaceSize = -1;
                    }
                }

                return new IgniteFsMetricsAdapter(
                    ggfsCtx.data().spaceSize(),
                    ggfsCtx.data().maxSpaceSize(),
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
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get metrics because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        metrics.reset();
    }

    /** {@inheritDoc} */
    @Override public long size(IgniteFsPath path) throws IgniteCheckedException {
        if (enterBusy()) {
            try {
                A.notNull(path, "path");

                IgniteUuid nextId = meta.fileId(path);

                if (nextId == null)
                    return 0;

                IgniteFsPathSummary sum = new IgniteFsPathSummary(path);

                summary0(nextId, sum);

                return sum.totalLength();
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get path size because Grid is stopping.");
    }

    /**
     * Calculates size of directory or file for given ID.
     *
     * @param fileId File ID.
     * @param sum Summary object that will collect information.
     * @throws IgniteCheckedException If failed.
     */
    private void summary0(IgniteUuid fileId, IgniteFsPathSummary sum) throws IgniteCheckedException {
        assert sum != null;

        GridGgfsFileInfo info = meta.info(fileId);

        if (info != null) {
            if (info.isDirectory()) {
                if (!ROOT_ID.equals(info.id()))
                    sum.directoriesCount(sum.directoriesCount() + 1);

                for (GridGgfsListingEntry entry : info.listing().values())
                    summary0(entry.fileId(), sum);
            }
            else {
                sum.filesCount(sum.filesCount() + 1);
                sum.totalLength(sum.totalLength() + info.length());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void format() throws IgniteCheckedException {
        formatAsync().get();
    }

    /**
     * Formats the file system removing all existing entries from it.
     *
     * @return Future.
     */
    IgniteFuture<?> formatAsync() throws IgniteCheckedException {
        IgniteUuid id = meta.softDelete(null, null, ROOT_ID);

        if (id == null)
            return new GridFinishedFuture<Object>(ggfsCtx.kernalContext());
        else {
            GridFutureAdapter<Object> fut = new GridFutureAdapter<>(ggfsCtx.kernalContext());

            GridFutureAdapter<Object> oldFut = delFuts.putIfAbsent(id, fut);

            if (oldFut != null)
                return oldFut;
            else {
                if (!meta.exists(id)) {
                    // Safety in case response message was received before we put future into collection.
                    fut.onDone();

                    delFuts.remove(id, fut);
                }

                return fut;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> awaitDeletesAsync() throws IgniteCheckedException {
        Collection<IgniteUuid> ids = meta.pendingDeletes();

        if (!ids.isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("Constructing delete future for trash entries: " + ids);

            GridCompoundFuture<Object, Object> resFut = new GridCompoundFuture<>(ggfsCtx.kernalContext());

            for (IgniteUuid id : ids) {
                GridFutureAdapter<Object> fut = new GridFutureAdapter<>(ggfsCtx.kernalContext());

                IgniteFuture<Object> oldFut = delFuts.putIfAbsent(id, fut);

                if (oldFut != null)
                    resFut.add(oldFut);
                else {
                    if (meta.exists(id))
                        resFut.add(fut);
                    else {
                        fut.onDone();

                        delFuts.remove(id, fut);
                    }
                }
            }

            resFut.markInitialized();

            return resFut;
        }
        else
            return new GridFinishedFuture<>(ggfsCtx.kernalContext());
    }

    /**
     * Get file descriptor for specified path.
     *
     * @param path Path to file.
     * @return Detailed file descriptor or {@code null}, if file does not exist.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private FileDescriptor getFileDescriptor(IgniteFsPath path) throws IgniteCheckedException {
        List<IgniteUuid> ids = meta.fileIds(path);
        GridGgfsFileInfo fileInfo = meta.info(ids.get(ids.size() - 1));

        if (fileInfo == null)
            return null; // File does not exist.

        // Resolve parent ID for removed file.
        IgniteUuid parentId = ids.size() >= 2 ? ids.get(ids.size() - 2) : null;

        return new FileDescriptor(parentId, path.name(), fileInfo.id(), fileInfo.isFile());
    }

    /**
     * Remove file from the file system (structure and data).
     *
     * @param path Path of the deleted file.
     * @param desc Detailed file descriptor to remove.
     * @param rmvLocked Whether to remove this entry in case it is has explicit lock.
     * @throws IgniteCheckedException If failed.
     */
    private void deleteFile(IgniteFsPath path, FileDescriptor desc, boolean rmvLocked) throws IgniteCheckedException {
        IgniteUuid parentId = desc.parentId;
        IgniteUuid fileId = desc.fileId;

        if (parentId == null || ROOT_ID.equals(fileId)) {
            assert parentId == null && ROOT_ID.equals(fileId) : "Invalid file descriptor: " + desc;

            return; // Never remove the root directory!
        }

        if (TRASH_ID.equals(fileId))
            return; // Never remove trash directory.

        meta.removeIfEmpty(parentId, desc.fileName, fileId, path, rmvLocked);
    }

    /**
     * Check whether GGFS with the same name exists among provided attributes.
     *
     * @param attrs Attributes.
     * @return {@code True} in case GGFS with the same name exists among provided attributes
     */
    private boolean sameGgfs(GridGgfsAttributes[] attrs) {
        if (attrs != null) {
            String ggfsName = name();

            for (GridGgfsAttributes attr : attrs) {
                if (F.eq(ggfsName, attr.ggfsName()))
                    return true;
            }
        }
        return false;
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(IgniteFsTask<T, R> task, @Nullable IgniteFsRecordResolver rslvr,
        Collection<IgniteFsPath> paths, @Nullable T arg) throws IgniteCheckedException {
        return executeAsync(task, rslvr, paths, arg).get();
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(IgniteFsTask<T, R> task, @Nullable IgniteFsRecordResolver rslvr,
        Collection<IgniteFsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg)
        throws IgniteCheckedException {
        return executeAsync(task, rslvr, paths, skipNonExistentFiles, maxRangeLen, arg).get();
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends IgniteFsTask<T, R>> taskCls,
        @Nullable IgniteFsRecordResolver rslvr, Collection<IgniteFsPath> paths, @Nullable T arg) throws IgniteCheckedException {
        return executeAsync(taskCls, rslvr, paths, arg).get();
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends IgniteFsTask<T, R>> taskCls,
        @Nullable IgniteFsRecordResolver rslvr, Collection<IgniteFsPath> paths, boolean skipNonExistentFiles,
        long maxRangeSize, @Nullable T arg) throws IgniteCheckedException {
        return executeAsync(taskCls, rslvr, paths, skipNonExistentFiles, maxRangeSize, arg).get();
    }

    /**
     * Executes GGFS task asynchronously.
     *
     * @param task Task to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param arg Optional task argument.
     * @return Execution future.
     */
    <T, R> IgniteFuture<R> executeAsync(IgniteFsTask<T, R> task, @Nullable IgniteFsRecordResolver rslvr,
        Collection<IgniteFsPath> paths, @Nullable T arg) {
        return executeAsync(task, rslvr, paths, true, cfg.getMaximumTaskRangeLength(), arg);
    }

    /**
     * Executes GGFS task with overridden maximum range length (see
     * {@link org.apache.ignite.fs.IgniteFsConfiguration#getMaximumTaskRangeLength()} for more information).
     *
     * @param task Task to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param skipNonExistentFiles Whether to skip non existent files. If set to {@code true} non-existent files will
     *     be ignored. Otherwise an exception will be thrown.
     * @param maxRangeLen Optional maximum range length. If {@code 0}, then by default all consecutive
     *      GGFS blocks will be included.
     * @param arg Optional task argument.
     * @return Execution future.
     */
    <T, R> IgniteFuture<R> executeAsync(IgniteFsTask<T, R> task, @Nullable IgniteFsRecordResolver rslvr,
        Collection<IgniteFsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg) {
        return ggfsCtx.kernalContext().task().execute(task, new IgniteFsTaskArgsImpl<>(cfg.getName(), paths, rslvr,
            skipNonExistentFiles, maxRangeLen, arg));
    }

    /**
     * Executes GGFS task asynchronously.
     *
     * @param taskCls Task class to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param arg Optional task argument.
     * @return Execution future.
     */
    <T, R> IgniteFuture<R> executeAsync(Class<? extends IgniteFsTask<T, R>> taskCls,
        @Nullable IgniteFsRecordResolver rslvr, Collection<IgniteFsPath> paths, @Nullable T arg) {
        return executeAsync(taskCls, rslvr, paths, true, cfg.getMaximumTaskRangeLength(), arg);
    }

    /**
     * Executes GGFS task asynchronously with overridden maximum range length (see
     * {@link org.apache.ignite.fs.IgniteFsConfiguration#getMaximumTaskRangeLength()} for more information).
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
    <T, R> IgniteFuture<R> executeAsync(Class<? extends IgniteFsTask<T, R>> taskCls,
        @Nullable IgniteFsRecordResolver rslvr, Collection<IgniteFsPath> paths, boolean skipNonExistentFiles,
        long maxRangeLen, @Nullable T arg) {
        return ggfsCtx.kernalContext().task().execute((Class<IgniteFsTask<T, R>>)taskCls,
            new IgniteFsTaskArgsImpl<>(cfg.getName(), paths, rslvr, skipNonExistentFiles, maxRangeLen, arg));
    }

    /** {@inheritDoc} */
    @Override public boolean evictExclude(IgniteFsPath path, boolean primary) {
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
     * @throws IgniteCheckedException If failed.
     */
    private GridGgfsFileInfo resolveFileInfo(IgniteFsPath path, IgniteFsMode mode) throws IgniteCheckedException {
        assert path != null;
        assert mode != null;

        GridGgfsFileInfo info = null;

        switch (mode) {
            case PRIMARY:
                info = meta.info(meta.fileId(path));

                break;

            case DUAL_SYNC:
            case DUAL_ASYNC:
                info = meta.info(meta.fileId(path));

                if (info == null) {
                    IgniteFsFile status = secondaryFs.info(path);

                    if (status != null)
                        info = status.isDirectory() ? new GridGgfsFileInfo(true, status.properties()) :
                            new GridGgfsFileInfo(status.blockSize(), status.length(), null, null, false,
                            status.properties());
                }

                break;

            default:
                assert false : "Unknown mode: " + mode;
        }

        return info;
    }

    /** {@inheritDoc} */
    @Override public IgniteFs withAsync() {
        return new GridGgfsAsyncImpl(this);
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        throw new IllegalStateException("Asynchronous mode is not enabled.");
    }

    /** Detailed file descriptor. */
    private static final class FileDescriptor {
        /** Parent file ID. */
        @Nullable
        private final IgniteUuid parentId;

        /** File name. */
        private final String fileName;

        /** File ID. */
        private final IgniteUuid fileId;

        /** File is plain data file or directory. */
        private final boolean isFile;

        /**
         * Constructs detailed file descriptor.
         *
         * @param parentId Parent file ID.
         * @param fileName File name.
         * @param fileId File ID.
         * @param isFile {@code True} if file.
         */
        private FileDescriptor(@Nullable IgniteUuid parentId, String fileName, IgniteUuid fileId, boolean isFile) {
            assert fileName != null;

            this.parentId = parentId;
            this.fileName = fileName;

            this.fileId = fileId;
            this.isFile = isFile;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = parentId != null ? parentId.hashCode() : 0;

            res = 31 * res + fileName.hashCode();
            res = 31 * res + fileId.hashCode();
            res = 31 * res + (isFile ? 1231 : 1237);

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (o == this)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            FileDescriptor that = (FileDescriptor)o;

            return fileId.equals(that.fileId) && isFile == that.isFile && fileName.equals(that.fileName) &&
                (parentId == null ? that.parentId == null : parentId.equals(that.parentId));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FileDescriptor.class, this);
        }
    }

    /**
     * GGFS output stream extension that fires events.
     */
    private class GgfsEventAwareOutputStream extends IgniteFsOutputStreamImpl {
        /** Close guard. */
        private final AtomicBoolean closeGuard = new AtomicBoolean(false);

        /**
         * Constructs file output stream.
         *
         * @param path Path to stored file.
         * @param fileInfo File info.
         * @param parentId Parent ID.
         * @param bufSize The size of the buffer to be used.
         * @param mode GGFS mode.
         * @param batch Optional secondary file system batch.
         * @throws IgniteCheckedException In case of error.
         */
        GgfsEventAwareOutputStream(IgniteFsPath path, GridGgfsFileInfo fileInfo,
            IgniteUuid parentId, int bufSize, IgniteFsMode mode, @Nullable GridGgfsFileWorkerBatch batch)
            throws IgniteCheckedException {
            super(ggfsCtx, path, fileInfo, parentId, bufSize, mode, batch, metrics);

            metrics.incrementFilesOpenedForWrite();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
        @Override protected void onClose() throws IOException {
            if (closeGuard.compareAndSet(false, true)) {
                super.onClose();

                metrics.decrementFilesOpenedForWrite();

                if (evts.isRecordable(EVT_GGFS_FILE_CLOSED_WRITE))
                    evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_FILE_CLOSED_WRITE, bytes()));
            }
        }
    }

    /**
     * GGFS input stream extension that fires events.
     */
    private class GgfsEventAwareInputStream extends GridGgfsInputStreamImpl {
        /** Close guard. */
        private final AtomicBoolean closeGuard = new AtomicBoolean(false);

        /**
         * Constructor.
         *
         * @param ggfsCtx GGFS context.
         * @param path Path to stored file.
         * @param fileInfo File info.
         * @param prefetchBlocks Prefetch blocks.
         * @param seqReadsBeforePrefetch Amount of sequential reads before prefetch is triggered.
         * @param secReader Optional secondary file system reader.
         * @param metrics Metrics.
         */
        GgfsEventAwareInputStream(GridGgfsContext ggfsCtx, IgniteFsPath path, GridGgfsFileInfo fileInfo,
            int prefetchBlocks, int seqReadsBeforePrefetch, @Nullable IgniteFsReader secReader,
            GridGgfsLocalMetrics metrics) {
            super(ggfsCtx, path, fileInfo, prefetchBlocks, seqReadsBeforePrefetch, secReader, metrics);

            metrics.incrementFilesOpenedForRead();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
        @Override public void close() throws IOException {
            if (closeGuard.compareAndSet(false, true)) {
                super.close();

                metrics.decrementFilesOpenedForRead();

                if (evts.isRecordable(EVT_GGFS_FILE_CLOSED_READ))
                    evts.record(new IgniteFsEvent(path, localNode(), EVT_GGFS_FILE_CLOSED_READ, bytes()));
            }
        }
    }

    /**
     * Space calculation task.
     */
    @GridInternal
    private static class GgfsGlobalSpaceTask extends ComputeTaskSplitAdapter<Object, IgniteBiTuple<Long, Long>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** GGFS name. */
        private String ggfsName;

        /**
         * @param ggfsName GGFS name.
         */
        private GgfsGlobalSpaceTask(@Nullable String ggfsName) {
            this.ggfsName = ggfsName;
        }

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg)
            throws IgniteCheckedException {
            Collection<ComputeJob> res = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                res.add(new ComputeJobAdapter() {
                    /** Injected grid. */
                    @IgniteInstanceResource
                    private Ignite g;

                    @Nullable @Override public IgniteBiTuple<Long, Long> execute() throws IgniteCheckedException {
                        IgniteFs ggfs = ((GridKernal)g).context().ggfs().ggfs(ggfsName);

                        if (ggfs == null)
                            return F.t(0L, 0L);

                        IgniteFsMetrics metrics = ggfs.metrics();

                        long loc = metrics.localSpaceSize();

                        return F.t(loc, metrics.maxSpaceSize());
                    }
                });
            }

            return res;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Long, Long> reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
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
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteCheckedException {
            // Never failover.
            return ComputeJobResultPolicy.WAIT;
        }
    }

    /**
     * Format message listener required for format action completion.
     */
    private class FormatMessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            if (msg instanceof GridGgfsDeleteMessage) {
                ClusterNode node = ggfsCtx.kernalContext().discovery().node(nodeId);

                if (node != null) {
                    if (sameGgfs((GridGgfsAttributes[])node.attribute(ATTR_GGFS))) {
                        GridGgfsDeleteMessage msg0 = (GridGgfsDeleteMessage)msg;

                        try {
                            msg0.finishUnmarshal(ggfsCtx.kernalContext().config().getMarshaller(), null);
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to unmarshal message (will ignore): " + msg0, e);

                            return;
                        }

                        assert msg0.id() != null;

                        GridFutureAdapter<?> fut = delFuts.remove(msg0.id());

                        if (fut != null) {
                            if (msg0.error() == null)
                                fut.onDone();
                            else
                                fut.onDone(msg0.error());
                        }
                    }
                }
            }
        }
    }

    /**
     * Discovery listener required for format actions completion.
     */
    private class FormatDiscoveryListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(IgniteEvent evt) {
            assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

            IgniteDiscoveryEvent evt0 = (IgniteDiscoveryEvent)evt;

            if (evt0.eventNode() != null) {
                if (sameGgfs((GridGgfsAttributes[])evt0.eventNode().attribute(ATTR_GGFS))) {
                    Collection<IgniteUuid> rmv = new HashSet<>();

                    for (Map.Entry<IgniteUuid, GridFutureAdapter<Object>> fut : delFuts.entrySet()) {
                        IgniteUuid id = fut.getKey();

                        try {
                            if (!meta.exists(id)) {
                                fut.getValue().onDone();

                                rmv.add(id);
                            }
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to check file existence: " + id, e);
                        }
                    }

                    for (IgniteUuid id : rmv)
                        delFuts.remove(id);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid nextAffinityKey() {
        if (enterBusy()) {
            try {
                return data.nextAffinityKey(null);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get next affinity key because Grid is stopping.");
    }

    /** {@inheritDoc} */
    @Override public boolean isProxy(URI path) {
        IgniteFsMode mode = F.isEmpty(cfg.getPathModes()) ? cfg.getDefaultMode() :
            modeRslvr.resolveMode(new IgniteFsPath(path));

        return mode == PROXY;
    }
}
