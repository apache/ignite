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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.events.IgfsEvent;
import org.apache.ignite.igfs.IgfsConcurrentModificationException;
import org.apache.ignite.igfs.IgfsDirectoryNotEmptyException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathAlreadyExistsException;
import org.apache.ignite.igfs.IgfsPathIsDirectoryException;
import org.apache.ignite.igfs.IgfsPathIsNotDirectoryException;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_IGFS_DIR_CREATED;
import static org.apache.ignite.events.EventType.EVT_IGFS_DIR_RENAMED;
import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_CREATED;
import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_DELETED;
import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_PURGED;
import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_RENAMED;
import static org.apache.ignite.internal.processors.igfs.IgfsFileInfo.ROOT_ID;
import static org.apache.ignite.internal.processors.igfs.IgfsFileInfo.TRASH_ID;
import static org.apache.ignite.internal.processors.igfs.IgfsFileInfo.builder;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache based structure (meta data) manager.
 */
@SuppressWarnings("all")
public class IgfsMetaManager extends IgfsManager {
    /** IGFS configuration. */
    private FileSystemConfiguration cfg;

    /** Metadata cache. */
    private IgniteInternalCache<Object, Object> metaCache;

    /** */
    private CountDownLatch metaCacheStartLatch;

    /** File ID to file info projection. */
    private IgniteInternalCache<IgniteUuid, IgfsFileInfo> id2InfoPrj;

    /** Predefined key for sampling mode value. */
    private GridCacheInternal sampling;

    /** Logger. */
    private IgniteLogger log;

    /** Delete worker. */
    private volatile IgfsDeleteWorker delWorker;

    /** Events manager. */
    private GridEventStorageManager evts;

    /** Local node. */
    private ClusterNode locNode;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /**
     *
     */
    void awaitInit() {
        try {
            metaCacheStartLatch.await();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        metaCacheStartLatch = new CountDownLatch(1);

        cfg = igfsCtx.configuration();

        evts = igfsCtx.kernalContext().event();

        sampling = new IgfsSamplingKey(cfg.getName());

        log = igfsCtx.kernalContext().log(IgfsMetaManager.class);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        metaCache = igfsCtx.kernalContext().cache().getOrStartCache(cfg.getMetaCacheName());

        assert metaCache != null;

        igfsCtx.kernalContext().cache().internalCache(cfg.getMetaCacheName()).preloader().startFuture()
            .listen(new CI1<IgniteInternalFuture<Object>>() {
                @Override public void apply(IgniteInternalFuture<Object> f) {
                    metaCacheStartLatch.countDown();
                }
            });

        id2InfoPrj = (IgniteInternalCache<IgniteUuid, IgfsFileInfo>)metaCache.<IgniteUuid, IgfsFileInfo>cache();

        locNode = igfsCtx.kernalContext().discovery().localNode();

        // Start background delete worker.
        delWorker = new IgfsDeleteWorker(igfsCtx);

        delWorker.start();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        IgfsDeleteWorker delWorker0 = delWorker;

        if (delWorker0 != null)
            delWorker0.cancel();

        if (delWorker0 != null) {
            try {
                U.join(delWorker0);
            }
            catch (IgniteInterruptedCheckedException ignored) {
                // No-op.
            }
        }

        busyLock.block();
    }

    /**
     * Return nodes where meta cache is defined.
     *
     * @return Nodes where meta cache is defined.
     */
    Collection<ClusterNode> metaCacheNodes() {
        if (busyLock.enterBusy()) {
            try {
                return igfsCtx.kernalContext().discovery().cacheNodes(metaCache.name(), AffinityTopologyVersion.NONE);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get meta cache nodes because Grid is stopping.");
    }

    /**
     * Gets file ID for specified path.
     *
     * @param path Path.
     * @return File ID for specified path or {@code null} if such file doesn't exist.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgniteUuid fileId(IgfsPath path) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                return fileId(path, false);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get file ID because Grid is stopping: " + path);
    }

    /**
     * Gets file ID for specified path possibly skipping existing transaction.
     *
     * @param path Path.
     * @param skipTx Whether to skip existing transaction.
     * @return File ID for specified path or {@code null} if such file doesn't exist.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private IgniteUuid fileId(IgfsPath path, boolean skipTx) throws IgniteCheckedException {
        List<IgniteUuid> ids = fileIds(path, skipTx);

        assert ids != null && !ids.isEmpty() : "Invalid file IDs [path=" + path + ", ids=" + ids + ']';

        return ids.get(ids.size() - 1);
    }

    /**
     * Gets file ID by its name from parent directory listing.
     *
     * @param parentId Parent directory ID to get child ID for.
     * @param fileName File name in parent listing to get file ID for.
     * @return File ID.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgniteUuid fileId(IgniteUuid parentId, String fileName) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                return fileId(parentId, fileName, false);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get file ID because Grid is stopping [parentId=" + parentId +
                ", fileName=" + fileName + ']');
    }

    /**
     * Gets file ID by its name from parent directory listing possibly skipping existing transaction.
     *
     * @param parentId Parent directory ID to get child ID for.
     * @param fileName File name in parent listing to get file ID for.
     * @param skipTx Whether to skip existing transaction.
     * @return File ID.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private IgniteUuid fileId(IgniteUuid parentId, String fileName, boolean skipTx) throws IgniteCheckedException {
        IgfsListingEntry entry = directoryListing(parentId, skipTx).get(fileName);

        if (entry == null) {
            if (log.isDebugEnabled())
                log.debug("Missing file ID [parentId=" + parentId + ", fileName=" + fileName + ']');

            return null;
        }

        return entry.fileId();
    }

    /**
     * Gets all file IDs for components of specified path. Result cannot be empty - there is at least root element.
     * But each element (except the first) can be {@code null} if such files don't exist.
     *
     * @param path Path.
     * @return Collection of file IDs for components of specified path.
     * @throws IgniteCheckedException If failed.
     */
    public List<IgniteUuid> fileIds(IgfsPath path) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                return fileIds(path, false);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get file IDS because Grid is stopping: " + path);
    }

    /**
     * Gets all file IDs for components of specified path possibly skipping existing transaction. Result cannot
     * be empty - there is at least root element. But each element (except the first) can be {@code null} if such
     * files don't exist.
     *
     * @param path Path.
     * @param skipTx Whether to skip existing transaction.
     * @return Collection of file IDs for components of specified path.
     * @throws IgniteCheckedException If failed.
     */
    private List<IgniteUuid> fileIds(IgfsPath path, boolean skipTx) throws IgniteCheckedException {
        assert path != null;

        // Path components.
        Collection<String> components = path.components();

        // Collection of file IDs for components of specified path.
        List<IgniteUuid> ids = new ArrayList<>(components.size() + 1);

        ids.add(ROOT_ID); // Always add root ID.

        IgniteUuid fileId = ROOT_ID;

        for (String s : components) {
            assert !s.isEmpty();

            if (fileId != null)
                fileId = fileId(fileId, s, skipTx);

            ids.add(fileId);
        }

        return ids;
    }

    /**
     * Ensure that entry with the given ID exists in meta cache.
     *
     * @param fileId File id.
     * @return {@code True} in case such entry exists.
     * @throws IgniteCheckedException IF failed.
     */
    public boolean exists(IgniteUuid fileId) throws IgniteCheckedException{
        if (busyLock.enterBusy()) {
            try {
                assert fileId != null;

                // containsKey() doesn't work here since meta cache can be PARTITIONED (we do not restrict if!).
                return info(fileId) != null;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to check file system entry existence because Grid is stopping: " +
                fileId);
    }

    /**
     * Gets file info by its ID.
     *
     * @param fileId File ID to get details for.
     * @return File info.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgfsFileInfo info(@Nullable IgniteUuid fileId) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                if (fileId == null)
                    return null;

                IgfsFileInfo info = id2InfoPrj.get(fileId);

                // Force root ID always exist in cache.
                if (info == null && ROOT_ID.equals(fileId))
                    id2InfoPrj.putIfAbsent(ROOT_ID, info = new IgfsFileInfo());

                return info;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get file info because Grid is stopping: " + fileId);
    }

    /**
     * Gets files details by their IDs.
     *
     * @param fileIds file IDs to get details for.
     * @return Files details.
     * @throws IgniteCheckedException If failed.
     */
    public Map<IgniteUuid, IgfsFileInfo> infos(Collection<IgniteUuid> fileIds) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);
                assert fileIds != null;

                if (F.isEmpty(fileIds))
                    return Collections.emptyMap();

                Map<IgniteUuid, IgfsFileInfo> map = id2InfoPrj.getAll(fileIds);

                // Force root ID always exist in cache.
                if (fileIds.contains(ROOT_ID) && !map.containsKey(ROOT_ID)) {
                    IgfsFileInfo info = new IgfsFileInfo();

                    id2InfoPrj.putIfAbsent(ROOT_ID, info);

                    map = new GridLeanMap<>(map);

                    map.put(ROOT_ID, info);
                }

                return map;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get file infos because Grid is stopping: " + fileIds);
    }

    /**
     * Lock the file explicitly outside of transaction.
     *
     * @param fileId File ID to lock.
     * @return Locked file info or {@code null} if file cannot be locked or doesn't exist.
     * @throws IgniteCheckedException If failed.
     */
    public IgfsFileInfo lock(IgniteUuid fileId) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);
                assert fileId != null;

                IgniteInternalTx tx = metaCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);

                try {
                    // Lock file ID for this transaction.
                    IgfsFileInfo oldInfo = info(fileId);

                    if (oldInfo == null)
                        throw new IgniteCheckedException("Failed to lock file (file not found): " + fileId);

                    IgfsFileInfo newInfo = lockInfo(oldInfo);

                    boolean put = metaCache.put(fileId, newInfo);

                    assert put : "Value was not stored in cache [fileId=" + fileId + ", newInfo=" + newInfo + ']';

                    tx.commit();

                    return newInfo;
                }
                catch (GridClosureException e) {
                    throw U.cast(e);
                }
                finally {
                    tx.close();
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to obtain lock because Grid is stopping: " + fileId);
    }

    /**
     * Set lock on file info.
     *
     * @param info File info.
     * @return New file info with lock set.
     * @throws IgniteCheckedException In case lock is already set on that file.
     */
    public IgfsFileInfo lockInfo(IgfsFileInfo info) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert info != null;

                if (info.lockId() != null)
                    throw new IgniteCheckedException("Failed to lock file (file is being concurrently written) [fileId=" +
                        info.id() + ", lockId=" + info.lockId() + ']');

                return new IgfsFileInfo(info, IgniteUuid.randomUuid(), info.modificationTime());
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get lock info because Grid is stopping: " + info);
    }

    /**
     * Remove explicit lock on file held by the current thread.
     *
     * @param info File info to unlock.
     * @param modificationTime Modification time to write to file info.
     * @throws IgniteCheckedException If failed.
     */
    public void unlock(final IgfsFileInfo info, final long modificationTime) throws IgniteCheckedException {
        assert validTxState(false);
        assert info != null;

        if (busyLock.enterBusy()) {
            try {
                final IgniteUuid lockId = info.lockId();

                if (lockId == null)
                    return;

                // Temporary clear interrupted state for unlocking.
                final boolean interrupted = Thread.interrupted();

                try {
                    IgfsUtils.doInTransactionWithRetries(metaCache, new IgniteOutClosureX<Void>() {
                        @Override public Void applyx() throws IgniteCheckedException {
                            IgniteUuid fileId = info.id();

                            // Lock file ID for this transaction.
                            IgfsFileInfo oldInfo = info(fileId);

                            if (oldInfo == null)
                                throw fsException(new IgfsPathNotFoundException("Failed to unlock file (file not found): " + fileId));

                            if (!info.lockId().equals(oldInfo.lockId()))
                                throw new IgniteCheckedException("Failed to unlock file (inconsistent file lock ID) [fileId=" + fileId +
                                    ", lockId=" + info.lockId() + ", actualLockId=" + oldInfo.lockId() + ']');

                            IgfsFileInfo newInfo = new IgfsFileInfo(oldInfo, null, modificationTime);

                            boolean put = metaCache.put(fileId, newInfo);

                            assert put : "Value was not stored in cache [fileId=" + fileId + ", newInfo=" + newInfo + ']';

                            return null;
                        }
                    });
                }
                finally {
                    assert validTxState(false);

                    if (interrupted)
                        Thread.currentThread().interrupt();
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to unlock file system entry because Grid is stopping: " + info);
    }

    /**
     * Lock file IDs participating in the transaction.<br/>
     *
     * @param fileIds file IDs to lock.
     * @return Locked file details. Resulting map doesn't contain details for not-existent files.
     * @throws IgniteCheckedException If failed.
     */
    private Map<IgniteUuid, IgfsFileInfo> lockIds(IgniteUuid... fileIds) throws IgniteCheckedException {
        assert validTxState(true);
        assert fileIds != null && fileIds.length > 0;

        // Always sort file IDs participating in transaction to escape cache transaction deadlocks.
        Arrays.sort(fileIds);

        // Wrap array as collection (1) to escape superfluous check in projection and (2) to check assertions.
        Collection<IgniteUuid> keys = Arrays.asList(fileIds);

        if (log.isDebugEnabled())
            log.debug("Locking file ids: " + keys);

        // Lock files and get their infos.
        Map<IgniteUuid, IgfsFileInfo> map = id2InfoPrj.getAll(keys);

        if (log.isDebugEnabled())
            log.debug("Locked file ids: " + keys);

        // Force root ID always exist in cache.
        if (keys.contains(ROOT_ID) && !map.containsKey(ROOT_ID)) {
            IgfsFileInfo info = new IgfsFileInfo();

            id2InfoPrj.putIfAbsent(ROOT_ID, info);

            map = new GridLeanMap<>(map);

            map.put(ROOT_ID, info);
        }

        // Returns detail's map for locked IDs.
        return map;
    }

    /**
     * List child files for specified file ID.
     *
     * @param fileId File to list child files for.
     * @return Directory listing for the specified file.
     * @throws IgniteCheckedException If failed.
     */
    public Map<String, IgfsListingEntry> directoryListing(IgniteUuid fileId) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                return directoryListing(fileId, false);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get directory listing because Grid is stopping: " + fileId);
    }

    /**
     * Gets first available file info for fragmentizer.
     *
     * @param exclude File IDs to exclude from result.
     * @return First qualified file info.
     * @throws IgniteCheckedException If failed to get file for fragmentizer.
     */
    public IgfsFileInfo fileForFragmentizer(Collection<IgniteUuid> exclude) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                return fileForFragmentizer0(ROOT_ID, exclude);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get file for framentizer because Grid is stopping.");
    }

    /**
     * Gets first available file info for fragmentizer.
     *
     * @param parentId Parent ID to scan.
     * @param exclude File IDs to exclude from result.
     * @return First qualified file info.
     * @throws IgniteCheckedException If failed to get file for fragmentizer.
     */
    private IgfsFileInfo fileForFragmentizer0(IgniteUuid parentId, Collection<IgniteUuid> exclude)
        throws IgniteCheckedException {
        IgfsFileInfo info = info(parentId);

        // Check if file was concurrently deleted.
        if (info == null)
            return null;

        assert info.isDirectory();

        Map<String, IgfsListingEntry> listing = info.listing();

        for (IgfsListingEntry entry : listing.values()) {
            if (entry.isFile()) {
                IgfsFileInfo fileInfo = info(entry.fileId());

                if (fileInfo != null) {
                    if (!exclude.contains(fileInfo.id()) &&
                        fileInfo.fileMap() != null &&
                        !fileInfo.fileMap().ranges().isEmpty())
                        return fileInfo;
                }
            }
            else {
                IgfsFileInfo fileInfo = fileForFragmentizer0(entry.fileId(), exclude);

                if (fileInfo != null)
                    return fileInfo;
            }
        }

        return null;
    }

    /**
     * List child files for specified file ID possibly skipping existing transaction.
     *
     * @param fileId File to list child files for.
     * @param skipTx Whether to skip existing transaction.
     * @return Directory listing for the specified file.*
     * @throws IgniteCheckedException If failed.
     */
    private Map<String, IgfsListingEntry> directoryListing(IgniteUuid fileId, boolean skipTx) throws IgniteCheckedException {
        assert fileId != null;

        IgfsFileInfo info = skipTx ? id2InfoPrj.getAllOutTx(Collections.singleton(fileId)).get(fileId) :
            id2InfoPrj.get(fileId);

        return info == null ? Collections.<String, IgfsListingEntry>emptyMap() : info.listing();
    }

    /**
     * Add file into file system structure.
     *
     * @param parentId Parent file ID.
     * @param fileName File name in the parent's listing.
     * @param newFileInfo File info to store in the parent's listing.
     * @return File id already stored in meta cache or {@code null} if passed file info was stored.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteUuid putIfAbsent(IgniteUuid parentId, String fileName, IgfsFileInfo newFileInfo)
        throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);
                assert parentId != null;
                assert fileName != null;
                assert newFileInfo != null;

                IgniteUuid res = null;

                IgniteInternalTx tx = metaCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);

                try {
                    res = putIfAbsentNonTx(parentId, fileName, newFileInfo);

                    tx.commit();
                }
                finally {
                    tx.close();
                }

                return res;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to put file because Grid is stopping [parentId=" + parentId +
                ", fileName=" + fileName + ", newFileInfo=" + newFileInfo + ']');
    }

    /**
     * Add file into file system structure. Do not create new transaction expecting that the one already exists.
     *
     * @param parentId Parent file ID.
     * @param fileName File name in the parent's listing.
     * @param newFileInfo File info to store in the parent's listing.
     * @return File id already stored in meta cache or {@code null} if passed file info was stored.
     * @throws IgniteCheckedException If failed.
     */
    private IgniteUuid putIfAbsentNonTx(IgniteUuid parentId, String fileName, IgfsFileInfo newFileInfo)
        throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Locking parent id [parentId=" + parentId + ", fileName=" + fileName + ", newFileInfo=" +
                newFileInfo + ']');

        validTxState(true);

        // Lock only parent file ID.
        IgfsFileInfo parentInfo = info(parentId);

        assert validTxState(true);

        if (parentInfo == null)
            throw fsException(new IgfsPathNotFoundException("Failed to lock parent directory (not found): " + parentId));

        if (!parentInfo.isDirectory())
            throw fsException(new IgfsPathIsNotDirectoryException("Parent file is not a directory: " + parentInfo));

        Map<String, IgfsListingEntry> parentListing = parentInfo.listing();

        assert parentListing != null;

        IgfsListingEntry entry = parentListing.get(fileName);

        assert validTxState(true);

        if (entry != null)
            return entry.fileId();

        IgniteUuid fileId = newFileInfo.id();

        if (!id2InfoPrj.putIfAbsent(fileId, newFileInfo))
            throw fsException("Failed to add file details into cache: " + newFileInfo);

        assert metaCache.get(parentId) != null;

        id2InfoPrj.invoke(parentId, new UpdateListing(fileName, new IgfsListingEntry(newFileInfo), false));

        return null;
    }

    /**
     * Move or rename file.
     *
     * @param fileId File ID to move or rename.
     * @param srcFileName Original file name in the parent's listing.
     * @param srcParentId Parent directory ID.
     * @param destFileName New file name in the parent's listing after moving.
     * @param destParentId New parent directory ID.
     * @throws IgniteCheckedException If failed.
     */
    public void move(IgniteUuid fileId, String srcFileName, IgniteUuid srcParentId, String destFileName,
        IgniteUuid destParentId) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                IgniteInternalTx tx = metaCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);

                try {
                    moveNonTx(fileId, srcFileName, srcParentId, destFileName, destParentId);

                    tx.commit();
                }
                finally {
                    tx.close();
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to move file system entry because Grid is stopping [fileId=" +
                fileId + ", srcFileName=" + srcFileName + ", srcParentId=" + srcParentId + ", destFileName=" +
                destFileName + ", destParentId=" + destParentId + ']');
    }

    /**
     * Move or rename file in existing transaction.
     *
     * @param fileId File ID to move or rename.
     * @param srcFileName Original file name in the parent's listing.
     * @param srcParentId Parent directory ID.
     * @param destFileName New file name in the parent's listing after moving.
     * @param destParentId New parent directory ID.
     * @throws IgniteCheckedException If failed.
     */
    private void moveNonTx(IgniteUuid fileId, @Nullable String srcFileName, IgniteUuid srcParentId, String destFileName,
        IgniteUuid destParentId) throws IgniteCheckedException {
        assert validTxState(true);
        assert fileId != null;
        assert srcFileName != null;
        assert srcParentId != null;
        assert destFileName != null;
        assert destParentId != null;

        if (srcParentId.equals(destParentId) && srcFileName.equals(destFileName)) {
            if (log.isDebugEnabled())
                log.debug("File is moved to itself [fileId=" + fileId +
                    ", fileName=" + srcFileName + ", parentId=" + srcParentId + ']');

            return; // File is moved to itself.
        }

        // Lock file ID and parent IDs for this transaction.
        Map<IgniteUuid, IgfsFileInfo> infoMap = lockIds(srcParentId, fileId, destParentId);

        validTxState(true);

        IgfsFileInfo srcInfo = infoMap.get(srcParentId);

        if (srcInfo == null)
            throw fsException(new IgfsPathNotFoundException("Failed to lock source directory (not found?)" +
                " [srcParentId=" + srcParentId + ']'));

        if (!srcInfo.isDirectory())
            throw fsException(new IgfsPathIsNotDirectoryException("Source is not a directory: " + srcInfo));

        IgfsFileInfo destInfo = infoMap.get(destParentId);

        if (destInfo == null)
            throw fsException(new IgfsPathNotFoundException("Failed to lock destination directory (not found?)" +
                " [destParentId=" + destParentId + ']'));

        if (!destInfo.isDirectory())
            throw fsException(new IgfsPathIsNotDirectoryException("Destination is not a directory: " + destInfo));

        IgfsFileInfo fileInfo = infoMap.get(fileId);

        if (fileInfo == null)
            throw fsException(new IgfsPathNotFoundException("Failed to lock target file (not found?) [fileId=" +
                fileId + ']'));

        IgfsListingEntry srcEntry = srcInfo.listing().get(srcFileName);
        IgfsListingEntry destEntry = destInfo.listing().get(destFileName);

        // If source file does not exist or was re-created.
        if (srcEntry == null || !srcEntry.fileId().equals(fileId))
            throw fsException(new IgfsPathNotFoundException("Failed to remove file name from the source directory" +
                " (file not found) [fileId=" + fileId + ", srcFileName=" + srcFileName +
                ", srcParentId=" + srcParentId + ", srcEntry=" + srcEntry + ']'));

        // If stored file already exist.
        if (destEntry != null)
            throw fsException(new IgfsPathAlreadyExistsException("Failed to add file name into the destination " +
                " directory (file already exists) [fileId=" + fileId + ", destFileName=" + destFileName +
                ", destParentId=" + destParentId + ", destEntry=" + destEntry + ']'));

        assert metaCache.get(srcParentId) != null;
        assert metaCache.get(destParentId) != null;

        // Remove listing entry from the source parent listing.
        id2InfoPrj.invoke(srcParentId, new UpdateListing(srcFileName, srcEntry, true));

        // Add listing entry into the destination parent listing.
        id2InfoPrj.invoke(destParentId, new UpdateListing(destFileName, srcEntry, false));
    }

    /**
     * Remove file from the file system structure.
     *
     * @param parentId Parent file ID.
     * @param fileName New file name in the parent's listing.
     * @param fileId File ID to remove.
     * @param path Path of the deleted file.
     * @param rmvLocked Whether to remove this entry in case it is has explicit lock.
     * @return The last actual file info or {@code null} if such file no more exist.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgfsFileInfo removeIfEmpty(IgniteUuid parentId, String fileName, IgniteUuid fileId,
        IgfsPath path, boolean rmvLocked)
        throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                IgniteInternalTx tx = metaCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);

                try {
                    if (parentId != null)
                        lockIds(parentId, fileId, TRASH_ID);
                    else
                        lockIds(fileId, TRASH_ID);

                    IgfsFileInfo fileInfo = removeIfEmptyNonTx(parentId, fileName, fileId, path, rmvLocked);

                    tx.commit();

                    delWorker.signal();

                    return fileInfo;
                }
                finally {
                    tx.close();
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to remove file system entry because Grid is stopping [parentId=" +
                parentId + ", fileName=" + fileName + ", fileId=" + fileId + ", path=" + path + ']');
    }

    /**
     * Remove file from the file system structure in existing transaction.
     *
     * @param parentId Parent file ID.
     * @param fileName New file name in the parent's listing.
     * @param fileId File ID to remove.
     * @param path Path of the deleted file.
     * @param rmvLocked Whether to remove this entry in case it has explicit lock.
     * @return The last actual file info or {@code null} if such file no more exist.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private IgfsFileInfo removeIfEmptyNonTx(@Nullable IgniteUuid parentId, String fileName, IgniteUuid fileId,
        IgfsPath path, boolean rmvLocked)
        throws IgniteCheckedException {
        assert validTxState(true);
        assert parentId != null;
        assert fileName != null;
        assert fileId != null;
        assert !ROOT_ID.equals(fileId);

        if (log.isDebugEnabled())
            log.debug("Remove file: [parentId=" + parentId + ", fileName= " + fileName + ", fileId=" + fileId + ']');

        // Safe gets because locks are obtained in removeIfEmpty.
        IgfsFileInfo fileInfo = id2InfoPrj.get(fileId);
        IgfsFileInfo parentInfo = id2InfoPrj.get(parentId);

        if (fileInfo == null || parentInfo == null) {
            if (parentInfo != null) { // fileInfo == null
                IgfsListingEntry entry = parentInfo.listing().get(fileName);

                // If file info does not exists but listing entry exists, throw inconsistent exception.
                if (entry != null && entry.fileId().equals(fileId))
                    throw new IgniteCheckedException("Failed to remove file (file system is in inconsistent state) " +
                        "[fileInfo=" + fileInfo + ", fileName=" + fileName + ", fileId=" + fileId + ']');
            }

            return null; // Parent directory or removed file cannot be locked (not found?).
        }

        assert parentInfo.isDirectory();

        if (!rmvLocked && fileInfo.lockId() != null)
            throw fsException("Failed to remove file (file is opened for writing) [fileName=" +
                fileName + ", fileId=" + fileId + ", lockId=" + fileInfo.lockId() + ']');

        // Validate own directory listing.
        if (fileInfo.isDirectory()) {
            Map<String, IgfsListingEntry> listing = fileInfo.listing();

            if (!F.isEmpty(listing))
                throw fsException(new IgfsDirectoryNotEmptyException("Failed to remove file (directory is not empty)" +
                    " [fileId=" + fileId + ", listing=" + listing + ']'));
        }

        // Validate file in the parent listing.
        IgfsListingEntry listingEntry = parentInfo.listing().get(fileName);

        if (listingEntry == null || !listingEntry.fileId().equals(fileId))
            return null;

        // Actual remove.
        softDeleteNonTx(parentId, fileName, fileId);

        // Update a file info of the removed file with a file path,
        // which will be used by delete worker for event notifications.
        id2InfoPrj.invoke(fileId, new UpdatePath(path));

        return builder(fileInfo).path(path).build();
    }

    /**
     * Move path to the trash directory.
     *
     * @param parentId Parent ID.
     * @param pathName Path name.
     * @param pathId Path ID.
     * @return ID of an entry located directly under the trash directory.
     * @throws IgniteCheckedException If failed.
     */
    IgniteUuid softDelete(@Nullable IgniteUuid parentId, @Nullable String pathName, IgniteUuid pathId) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                IgniteInternalTx tx = metaCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);

                try {
                    if (parentId == null)
                        lockIds(pathId, TRASH_ID);
                    else
                        lockIds(parentId, pathId, TRASH_ID);

                    IgniteUuid resId = softDeleteNonTx(parentId, pathName, pathId);

                    tx.commit();

                    delWorker.signal();

                    return resId;
                }
                finally {
                    tx.close();
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to perform soft delete because Grid is stopping [parentId=" +
                parentId + ", pathName=" + pathName + ", pathId=" + pathId + ']');
    }

    /**
     * Move path to the trash directory in existing transaction.
     *
     * @param parentId Parent ID.
     * @param name Path name.
     * @param id Path ID.
     * @return ID of an entry located directly under the trash directory.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private IgniteUuid softDeleteNonTx(@Nullable IgniteUuid parentId, @Nullable String name, IgniteUuid id)
        throws IgniteCheckedException {
        assert validTxState(true);

        IgniteUuid resId;

        if (parentId == null) {
            // Handle special case when we deleting root directory.
            assert ROOT_ID.equals(id);

            IgfsFileInfo rootInfo = id2InfoPrj.get(ROOT_ID);

            if (rootInfo == null)
                return null; // Root was never created.

            // Ensure trash directory existence.
            if (id2InfoPrj.get(TRASH_ID) == null)
                id2InfoPrj.getAndPut(TRASH_ID, new IgfsFileInfo(TRASH_ID));

            Map<String, IgfsListingEntry> rootListing = rootInfo.listing();

            if (!rootListing.isEmpty()) {
                IgniteUuid[] lockIds = new IgniteUuid[rootInfo.listing().size()];

                int i = 0;

                for (IgfsListingEntry entry : rootInfo.listing().values())
                    lockIds[i++] = entry.fileId();

                // Lock children IDs in correct order.
                lockIds(lockIds);

                // Construct new info and move locked entries from root to it.
                Map<String, IgfsListingEntry> transferListing = new HashMap<>();

                transferListing.putAll(rootListing);

                IgfsFileInfo newInfo = new IgfsFileInfo(transferListing);

                id2InfoPrj.getAndPut(newInfo.id(), newInfo);

                // Add new info to trash listing.
                id2InfoPrj.invoke(TRASH_ID, new UpdateListing(newInfo.id().toString(),
                    new IgfsListingEntry(newInfo), false));

                // Remove listing entries from root.
                for (Map.Entry<String, IgfsListingEntry> entry : transferListing.entrySet())
                    id2InfoPrj.invoke(ROOT_ID, new UpdateListing(entry.getKey(), entry.getValue(), true));

                resId = newInfo.id();
            }
            else
                resId = null;
        }
        else {
            // Ensure trash directory existence.
            if (id2InfoPrj.get(TRASH_ID) == null)
                id2InfoPrj.getAndPut(TRASH_ID, new IgfsFileInfo(TRASH_ID));

            moveNonTx(id, name, parentId, id.toString(), TRASH_ID);

            resId = id;
        }

        return resId;
    }

    /**
     * Remove listing entries of the given parent.
     *
     * @param parentId Parent ID.
     * @param listing Listing entries.
     * @return Collection of really deleted entries.
     * @throws IgniteCheckedException If failed.
     */
    Collection<IgniteUuid> delete(IgniteUuid parentId, Map<String, IgfsListingEntry> listing)
        throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert parentId != null;
                assert listing != null;
                assert validTxState(false);

                IgniteInternalTx tx = metaCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);

                try {
                    Collection<IgniteUuid> res = new HashSet<>();

                    // Obtain all necessary locks in one hop.
                    IgniteUuid[] allIds = new IgniteUuid[listing.size() + 1];

                    allIds[0] = parentId;

                    int i = 1;

                    for (IgfsListingEntry entry : listing.values())
                        allIds[i++] = entry.fileId();

                    Map<IgniteUuid, IgfsFileInfo> locks = lockIds(allIds);

                    IgfsFileInfo parentInfo = locks.get(parentId);

                    // Ensure parent is still in place.
                    if (parentInfo != null) {
                        Map<String, IgfsListingEntry> newListing =
                            new HashMap<>(parentInfo.listing().size(), 1.0f);

                        newListing.putAll(parentInfo.listing());

                        // Remove child entries if possible.
                        for (Map.Entry<String, IgfsListingEntry> entry : listing.entrySet()) {
                            IgniteUuid entryId = entry.getValue().fileId();

                            IgfsFileInfo entryInfo = locks.get(entryId);

                            if (entryInfo != null) {
                                // Delete only files or empty folders.
                                if (entryInfo.isFile() || entryInfo.isDirectory() && entryInfo.listing().isEmpty()) {
                                    id2InfoPrj.getAndRemove(entryId);

                                    newListing.remove(entry.getKey());

                                    res.add(entryId);
                                }
                            }
                            else {
                                // Entry was deleted concurrently.
                                newListing.remove(entry.getKey());

                                res.add(entryId);
                            }
                        }

                        // Update parent listing.
                        id2InfoPrj.put(parentId, new IgfsFileInfo(newListing, parentInfo));
                    }

                    tx.commit();

                    return res;
                }
                finally {
                    tx.close();
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to perform delete because Grid is stopping [parentId=" +
                parentId + ", listing=" + listing + ']');
    }

    /**
     * Remove entry from the metadata listing.
     *
     * @param parentId Parent ID.
     * @param name Name.
     * @param id ID.
     * @return {@code True} in case the entry really was removed from the cache by this call.
     * @throws IgniteCheckedException If failed.
     */
    boolean delete(IgniteUuid parentId, String name, IgniteUuid id) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                IgniteInternalTx tx = metaCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);

                try {
                    boolean res = false;

                    Map<IgniteUuid, IgfsFileInfo> infos = lockIds(parentId, id);

                    // Proceed only in case both parent and child exist.
                    if (infos.containsKey(parentId) && infos.containsKey(id)) {
                        IgfsFileInfo parentInfo = infos.get(parentId);

                        assert parentInfo != null;

                        IgfsListingEntry listingEntry = parentInfo.listing().get(name);

                        if (listingEntry != null)
                            id2InfoPrj.invoke(parentId, new UpdateListing(name, listingEntry, true));

                        id2InfoPrj.getAndRemove(id);

                        res = true;
                    }

                    tx.commit();

                    return res;
                }
                finally {
                    tx.close();
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to perform delete because Grid is stopping [parentId=" +
                parentId + ", name=" + name + ", id=" + id + ']');
    }

    /**
     * Check whether there are any pending deletes and return collection of pending delete entry IDs.
     *
     * @return Collection of entry IDs to be deleted.
     * @throws IgniteCheckedException If operation failed.
     */
    public Collection<IgniteUuid> pendingDeletes() throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                IgfsFileInfo trashInfo = id2InfoPrj.get(TRASH_ID);

                if (trashInfo != null) {
                    Map<String, IgfsListingEntry> listing = trashInfo.listing();

                    if (listing != null && !listing.isEmpty()) {
                        return F.viewReadOnly(listing.values(), new IgniteClosure<IgfsListingEntry, IgniteUuid>() {
                            @Override public IgniteUuid apply(IgfsListingEntry e) {
                                return e.fileId();
                            }
                        });
                    }
                }

                return Collections.emptySet();
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get pending deletes because Grid is stopping.");
    }

    /**
     * Update file info (file properties) in cache in existing transaction.
     *
     * @param parentId Parent ID ({@code null} if file is root).
     * @param fileName To update listing.
     * @param fileId File ID to update information for.
     * @param props Properties to set for the file.
     * @return Updated file info or {@code null} if such file ID not found.
     * @throws IgniteCheckedException If operation failed.
     */
    @Nullable private IgfsFileInfo updatePropertiesNonTx(@Nullable IgniteUuid parentId, IgniteUuid fileId,
        String fileName, Map<String, String> props) throws IgniteCheckedException {
        assert fileId != null;
        assert !F.isEmpty(props) : "Expects not-empty file's properties";
        assert validTxState(true);

        if (log.isDebugEnabled())
            log.debug("Update file properties [fileId=" + fileId + ", props=" + props + ']');

        try {
            IgfsFileInfo oldInfo;
            IgfsFileInfo parentInfo;

            // Lock file ID for this transaction.
            if (parentId == null) {
                oldInfo = info(fileId);
                parentInfo = null;
            }
            else {
                Map<IgniteUuid, IgfsFileInfo> locked = lockIds(parentId, fileId);

                oldInfo = locked.get(fileId);
                parentInfo = locked.get(parentId);

                if (parentInfo == null)
                    return null; // Parent not found.
            }

            assert validTxState(true);

            if (oldInfo == null)
                return null; // File not found.

            if (parentInfo != null) {
                Map<String, IgfsListingEntry> listing = parentInfo.listing();

                IgfsListingEntry entry = listing.get(fileName);

                if (entry == null || !entry.fileId().equals(fileId)) // File was removed or recreated.
                    return null;
            }

            Map<String, String> tmp = oldInfo.properties();

            tmp = tmp == null ? new GridLeanMap<String, String>(props.size()) : new GridLeanMap<>(tmp);

            for (Map.Entry<String, String> e : props.entrySet()) {
                if (e.getValue() == null)
                    // Remove properties with 'null' values.
                    tmp.remove(e.getKey());
                else
                    // Add/overwrite property.
                    tmp.put(e.getKey(), e.getValue());
            }

            IgfsFileInfo newInfo = new IgfsFileInfo(oldInfo, tmp);

            id2InfoPrj.put(fileId, newInfo);

            if (parentId != null) {
                IgfsListingEntry entry = new IgfsListingEntry(newInfo);

                assert metaCache.get(parentId) != null;

                id2InfoPrj.invoke(parentId, new UpdateListing(fileName, entry, false));
            }

            return newInfo;
        }
        catch (GridClosureException e) {
            throw U.cast(e);
        }
    }

    /**
     * Update file info (file properties) in cache.
     *
     * @param parentId Parent ID ({@code null} if file is root).
     * @param fileName To update listing.
     * @param fileId File ID to update information for.
     * @param props Properties to set for the file.
     * @return Updated file info or {@code null} if such file ID not found.
     * @throws IgniteCheckedException If operation failed.
     */
    @Nullable public IgfsFileInfo updateProperties(@Nullable IgniteUuid parentId, IgniteUuid fileId, String fileName,
        Map<String, String> props) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                IgniteInternalTx tx = metaCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);

                try {
                    IgfsFileInfo info = updatePropertiesNonTx(parentId, fileId, fileName, props);

                    tx.commit();

                    return info;
                }
                finally {
                    tx.close();
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to update properties because Grid is stopping [parentId=" +
                parentId + ", fileId=" + fileId + ", fileName=" + fileName + ", props=" + props + ']');
    }

    /**
     * Asynchronously updates record in parent listing.
     *
     * @param parentId Parent ID.
     * @param fileId File ID.
     * @param fileName File name.
     * @param lenDelta Length delta.
     * @param modificationTime Last modification time.
     */
    public void updateParentListingAsync(IgniteUuid parentId, IgniteUuid fileId, String fileName, long lenDelta,
        long modificationTime) {
        if (busyLock.enterBusy()) {
            try {
                assert parentId != null;

                assert validTxState(false);

                id2InfoPrj.invokeAsync(parentId, new UpdateListingEntry(fileId, fileName, lenDelta, 0,
                    modificationTime));
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to update parent listing because Grid is stopping [parentId=" +
                parentId + ", fileId=" + fileId + ", fileName=" + fileName + ']');
    }

    /**
     * Update file info in cache.
     *
     * @param fileId File ID to update information for.
     * @param c Closure to update file's info inside transaction.
     * @return Updated file info or {@code null} if such file ID not found.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgfsFileInfo updateInfo(IgniteUuid fileId, IgniteClosure<IgfsFileInfo, IgfsFileInfo> c)
        throws IgniteCheckedException {
        assert validTxState(false);
        assert fileId != null;
        assert c != null;

        if (busyLock.enterBusy()) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Update file info [fileId=" + fileId + ", c=" + c + ']');

                IgniteInternalTx tx = metaCache.isLockedByThread(fileId) ? null : metaCache.txStartEx(PESSIMISTIC,
                    REPEATABLE_READ);

                try {
                    // Lock file ID for this transaction.
                    IgfsFileInfo oldInfo = info(fileId);

                    if (oldInfo == null)
                        return null; // File not found.

                    IgfsFileInfo newInfo = c.apply(oldInfo);

                    if (newInfo == null)
                        throw fsException("Failed to update file info with null value" +
                            " [oldInfo=" + oldInfo + ", newInfo=" + newInfo + ", c=" + c + ']');

                    if (!oldInfo.id().equals(newInfo.id()))
                        throw fsException("Failed to update file info (file IDs differ)" +
                            " [oldInfo=" + oldInfo + ", newInfo=" + newInfo + ", c=" + c + ']');

                    if (oldInfo.isDirectory() != newInfo.isDirectory())
                        throw fsException("Failed to update file info (file types differ)" +
                            " [oldInfo=" + oldInfo + ", newInfo=" + newInfo + ", c=" + c + ']');

                    boolean b = metaCache.replace(fileId, oldInfo, newInfo);

                    assert b : "Inconsistent transaction state [oldInfo=" + oldInfo + ", newInfo=" + newInfo +
                        ", c=" + c + ']';

                    if (tx != null)
                        tx.commit();

                    return newInfo;
                }
                catch (GridClosureException e) {
                    throw U.cast(e);
                }
                finally {
                    if (tx != null)
                        tx.close();
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to update file system entry info because Grid is stopping: " +
                fileId);
    }

    /**
     * Set sampling flag.
     *
     * @param val Sampling flag state or {@code null} to clear sampling state and mark it as "not set".
     * @return {@code True} if sampling mode was actually changed by this call.
     * @throws IgniteCheckedException If failed.
     */
    public boolean sampling(Boolean val) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                IgniteInternalTx tx = metaCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);

                try {
                    Object prev = val != null ? metaCache.getAndPut(sampling, val) : metaCache.getAndRemove(sampling);

                    tx.commit();

                    return !F.eq(prev, val);
                }
                finally {
                    tx.close();
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to set sampling flag because Grid is stopping.");
    }

    /**
     * Get sampling flag state.
     *
     * @return {@code True} in case sampling is enabled, {@code false} otherwise or {@code null} in case sampling
     * is not set.
     * @throws IgniteCheckedException If failed.
     */
    public Boolean sampling() throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                Object val = metaCache.get(sampling);

                return (val == null || !(val instanceof Boolean)) ? null : (Boolean)val;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get sampling flag because Grid is stopping.");
    }

    /**
     * Create the file in DUAL mode.
     *
     * @param fs File system.
     * @param path Path.
     * @param simpleCreate "Simple create" flag.
     * @param props Properties..
     * @param overwrite Overwrite flag.
     * @param bufSize Buffer size.
     * @param replication Replication factor.
     * @param blockSize Block size.
     * @param affKey Affinity key.
     * @return Output stream descriptor.
     * @throws IgniteCheckedException If file creation failed.
     */
    public IgfsSecondaryOutputStreamDescriptor createDual(final IgfsSecondaryFileSystem fs,
        final IgfsPath path,
        final boolean simpleCreate,
        @Nullable final Map<String, String> props,
        final boolean overwrite,
        final int bufSize,
        final short replication,
        final long blockSize,
        final IgniteUuid affKey)
        throws IgniteCheckedException
    {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert path != null;

                // Events to fire (can be done outside of a transaction).
                final Deque<IgfsEvent> pendingEvts = new LinkedList<>();

                SynchronizationTask<IgfsSecondaryOutputStreamDescriptor> task =
                    new SynchronizationTask<IgfsSecondaryOutputStreamDescriptor>() {
                        /** Output stream to the secondary file system. */
                        private OutputStream out;

                        @Override public IgfsSecondaryOutputStreamDescriptor onSuccess(Map<IgfsPath,
                            IgfsFileInfo> infos) throws Exception {
                            assert !infos.isEmpty();

                            // Determine the first existing parent.
                            IgfsPath parentPath = null;

                            for (IgfsPath curPath : infos.keySet()) {
                                if (parentPath == null || curPath.isSubDirectoryOf(parentPath))
                                    parentPath = curPath;
                            }

                            assert parentPath != null;

                            IgfsFileInfo parentInfo = infos.get(parentPath);

                            // Delegate to the secondary file system.
                            out = simpleCreate ? fs.create(path, overwrite) :
                                fs.create(path, bufSize, overwrite, replication, blockSize, props);

                            IgfsPath parent0 = path.parent();

                            assert parent0 != null : "path.parent() is null (are we creating ROOT?): " + path;

                            // If some of the parent directories were missing, synchronize again.
                            if (!parentPath.equals(parent0)) {
                                parentInfo = synchronize(fs, parentPath, parentInfo, parent0, true, null);

                                // Fire notification about missing directories creation.
                                if (evts.isRecordable(EVT_IGFS_DIR_CREATED)) {
                                    IgfsPath evtPath = parent0;

                                    while (!parentPath.equals(evtPath)) {
                                        pendingEvts.addFirst(new IgfsEvent(evtPath, locNode, EVT_IGFS_DIR_CREATED));

                                        evtPath = evtPath.parent();

                                        assert evtPath != null; // If this fails, then ROOT does not exist.
                                    }
                                }
                            }

                            // Get created file info.
                            IgfsFile status = fs.info(path);

                            if (status == null)
                                throw fsException("Failed to open output stream to the file created in " +
                                    "the secondary file system because it no longer exists: " + path);
                            else if (status.isDirectory())
                                throw fsException("Failed to open output stream to the file created in " +
                                    "the secondary file system because the path points to a directory: " + path);

                            IgfsFileInfo newInfo = new IgfsFileInfo(status.blockSize(), status.length(), affKey,
                                IgniteUuid.randomUuid(), igfsCtx.igfs().evictExclude(path, false), status.properties());

                            // Add new file info to the listing optionally removing the previous one.
                            IgniteUuid oldId = putIfAbsentNonTx(parentInfo.id(), path.name(), newInfo);

                            if (oldId != null) {
                                IgfsFileInfo oldInfo = info(oldId);

                                id2InfoPrj.remove(oldId); // Remove the old one.
                                id2InfoPrj.put(newInfo.id(), newInfo); // Put the new one.

                                id2InfoPrj.invoke(parentInfo.id(),
                                    new UpdateListing(path.name(), parentInfo.listing().get(path.name()), true));
                                id2InfoPrj.invoke(parentInfo.id(),
                                    new UpdateListing(path.name(), new IgfsListingEntry(newInfo), false));

                                IgniteInternalFuture<?> delFut = igfsCtx.data().delete(oldInfo);

                                // Record PURGE event if needed.
                                if (evts.isRecordable(EVT_IGFS_FILE_PURGED)) {
                                    delFut.listen(new CI1<IgniteInternalFuture<?>>() {
                                        @Override public void apply(IgniteInternalFuture<?> t) {
                                            try {
                                                t.get(); // Ensure delete succeeded.

                                                evts.record(new IgfsEvent(path, locNode, EVT_IGFS_FILE_PURGED));
                                            }
                                            catch (IgniteCheckedException e) {
                                                LT.warn(log, e, "Old file deletion failed in DUAL mode [path=" + path +
                                                    ", simpleCreate=" + simpleCreate + ", props=" + props +
                                                    ", overwrite=" + overwrite + ", bufferSize=" + bufSize +
                                                    ", replication=" + replication + ", blockSize=" + blockSize + ']');
                                            }
                                        }
                                    });
                                }

                                // Record DELETE event if needed.
                                if (evts.isRecordable(EVT_IGFS_FILE_DELETED))
                                    pendingEvts.add(new IgfsEvent(path, locNode, EVT_IGFS_FILE_DELETED));
                            }

                            // Record CREATE event if needed.
                            if (evts.isRecordable(EVT_IGFS_FILE_CREATED))
                                pendingEvts.add(new IgfsEvent(path, locNode, EVT_IGFS_FILE_CREATED));

                            return new IgfsSecondaryOutputStreamDescriptor(parentInfo.id(), newInfo, out);
                        }

                        @Override public IgfsSecondaryOutputStreamDescriptor onFailure(Exception err)
                            throws IgniteCheckedException {
                            U.closeQuiet(out);

                            U.error(log, "File create in DUAL mode failed [path=" + path + ", simpleCreate=" +
                                simpleCreate + ", props=" + props + ", overwrite=" + overwrite + ", bufferSize=" +
                                bufSize + ", replication=" + replication + ", blockSize=" + blockSize + ']', err);

                            throw new IgniteCheckedException("Failed to create the file due to secondary file system " +
                                "exception: " + path, err);
                        }
                    };

                try {
                    return synchronizeAndExecute(task, fs, false, path.parent());
                }
                finally {
                    for (IgfsEvent evt : pendingEvts)
                        evts.record(evt);
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to create file in DUAL mode because Grid is stopping: " + path);
    }

    /**
     * Append to a file in DUAL mode.
     *
     * @param fs File system.
     * @param path Path.
     * @param bufSize Buffer size.
     * @return Output stream descriptor.
     * @throws IgniteCheckedException If output stream open for append has failed.
     */
    public IgfsSecondaryOutputStreamDescriptor appendDual(final IgfsSecondaryFileSystem fs, final IgfsPath path,
        final int bufSize) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert path != null;

                SynchronizationTask<IgfsSecondaryOutputStreamDescriptor> task =
                    new SynchronizationTask<IgfsSecondaryOutputStreamDescriptor>() {
                        /** Output stream to the secondary file system. */
                        private OutputStream out;

                        @Override public IgfsSecondaryOutputStreamDescriptor onSuccess(Map<IgfsPath,
                            IgfsFileInfo> infos) throws Exception {
                            IgfsFileInfo info = infos.get(path);

                            if (info.isDirectory())
                                throw fsException("Failed to open output stream to the file in the " +
                                    "secondary file system because the path points to a directory: " + path);

                            out = fs.append(path, bufSize, false, null);

                            // Synchronize file ending.
                            long len = info.length();
                            int blockSize = info.blockSize();

                            int remainder = (int)(len % blockSize);

                            if (remainder > 0) {
                                int blockIdx = (int)(len / blockSize);

                                IgfsSecondaryFileSystemPositionedReadable reader = fs.open(path, bufSize);

                                try {
                                    igfsCtx.data().dataBlock(info, path, blockIdx, reader).get();
                                }
                                finally {
                                    reader.close();
                                }
                            }

                            // Set lock and return.
                            info = lockInfo(info);

                            metaCache.put(info.id(), info);

                            return new IgfsSecondaryOutputStreamDescriptor(infos.get(path.parent()).id(), info, out);
                        }

                        @Override public IgfsSecondaryOutputStreamDescriptor onFailure(@Nullable Exception err)
                            throws IgniteCheckedException {
                            U.closeQuiet(out);

                            U.error(log, "File append in DUAL mode failed [path=" + path + ", bufferSize=" + bufSize +
                                ']', err);

                            throw new IgniteCheckedException("Failed to append to the file due to secondary file system " +
                                "exception: " + path, err);
                        }
                    };

                return synchronizeAndExecute(task, fs, true, path);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to append to file in DUAL mode because Grid is stopping: " + path);
    }

    /**
     * Open file in DUAL mode.
     *
     * @param fs Secondary file system.
     * @param path Path to open.
     * @param bufSize Buffer size.
     * @return Input stream descriptor.
     * @throws IgniteCheckedException If input stream open has failed.
     */
    public IgfsSecondaryInputStreamDescriptor openDual(final IgfsSecondaryFileSystem fs, final IgfsPath path,
        final int bufSize)
        throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert path != null;

                // First, try getting file info without any transactions and synchronization.
                IgfsFileInfo info = info(fileId(path));

                if (info != null) {
                    if (!info.isFile())
                        throw fsException(new IgfsPathIsDirectoryException("Failed to open file (not a file): " +
                            path));

                    return new IgfsSecondaryInputStreamDescriptor(info, fs.open(path, bufSize));
                }

                // If failed, try synchronize.
                SynchronizationTask<IgfsSecondaryInputStreamDescriptor> task =
                    new SynchronizationTask<IgfsSecondaryInputStreamDescriptor>() {
                        @Override public IgfsSecondaryInputStreamDescriptor onSuccess(
                            Map<IgfsPath, IgfsFileInfo> infos) throws Exception {
                            IgfsFileInfo info = infos.get(path);

                            if (info == null)
                                throw fsException(new IgfsPathNotFoundException("File not found: " + path));
                            if (!info.isFile())
                                throw fsException(new IgfsPathIsDirectoryException("Failed to open file " +
                                    "(not a file): " + path));

                            return new IgfsSecondaryInputStreamDescriptor(infos.get(path), fs.open(path, bufSize));
                        }

                        @Override public IgfsSecondaryInputStreamDescriptor onFailure(@Nullable Exception err)
                            throws IgniteCheckedException {
                            U.error(log, "File open in DUAL mode failed [path=" + path + ", bufferSize=" + bufSize +
                                ']', err);

                            throw new IgniteCheckedException("Failed to open the path due to secondary file system " +
                                "exception: " + path, err);
                        }
                    };

                return synchronizeAndExecute(task, fs, false, path);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to open file in DUAL mode because Grid is stopping: " + path);
    }

    /**
     * Synchronizes with secondary file system.
     *
     * @param fs File system.
     * @param path Path.
     * @return File info or {@code null} if file not found.
     * @throws IgniteCheckedException If sync task failed.
     */
    @Nullable public IgfsFileInfo synchronizeFileDual(final IgfsSecondaryFileSystem fs, final IgfsPath path)
        throws IgniteCheckedException {
        assert fs != null;
        assert path != null;

        if (busyLock.enterBusy()) {
            try {
                // First, try getting file info without any transactions and synchronization.
                IgfsFileInfo info = info(fileId(path));

                if (info != null)
                    return info;

                // If failed, try synchronize.
                SynchronizationTask<IgfsFileInfo> task =
                    new SynchronizationTask<IgfsFileInfo>() {
                        @Override public IgfsFileInfo onSuccess(Map<IgfsPath, IgfsFileInfo> infos)
                            throws Exception {
                            return infos.get(path);
                        }

                        @Override public IgfsFileInfo onFailure(@Nullable Exception err) throws IgniteCheckedException {
                            throw new IgniteCheckedException("Failed to synchronize path due to secondary file system " +
                                "exception: " + path, err);
                        }
                    };

                return synchronizeAndExecute(task, fs, false, path);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to synchronize file because Grid is stopping: " + path);


    }

    /**
     * Create directory in DUAL mode.
     *
     * @param fs Secondary file system.
     * @param path Path to create.
     * @param props Properties to be applied.
     * @return {@code True} in case rename was successful.
     * @throws IgniteCheckedException If directory creation failed.
     */
    public boolean mkdirsDual(final IgfsSecondaryFileSystem fs, final IgfsPath path, final Map<String, String> props)
        throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert path != null;
                assert props != null;

                if (path.parent() == null)
                    return true; // No additional handling for root directory is needed.

                // Events to fire (can be done outside of a transaction).
                final Deque<IgfsEvent> pendingEvts = new LinkedList<>();

                SynchronizationTask<Boolean> task = new SynchronizationTask<Boolean>() {
                    @Override public Boolean onSuccess(Map<IgfsPath, IgfsFileInfo> infos) throws Exception {
                        fs.mkdirs(path, props);

                        assert !infos.isEmpty();

                        // Now perform synchronization again starting with the last created parent.
                        IgfsPath parentPath = null;

                        for (IgfsPath curPath : infos.keySet()) {
                            if (parentPath == null || curPath.isSubDirectoryOf(parentPath))
                                parentPath = curPath;
                        }

                        assert parentPath != null;

                        IgfsFileInfo parentPathInfo = infos.get(parentPath);

                        synchronize(fs, parentPath, parentPathInfo, path, true, null);

                        if (evts.isRecordable(EVT_IGFS_DIR_CREATED)) {
                            IgfsPath evtPath = path;

                            while (!parentPath.equals(evtPath)) {
                                pendingEvts.addFirst(new IgfsEvent(evtPath, locNode, EVT_IGFS_DIR_CREATED));

                                evtPath = evtPath.parent();

                                assert evtPath != null; // If this fails, then ROOT does not exist.
                            }
                        }

                        return true;
                    }

                    @Override public Boolean onFailure(@Nullable Exception err) throws IgniteCheckedException {
                        U.error(log, "Directory creation in DUAL mode failed [path=" + path + ", properties=" + props +
                            ']', err);

                        throw new IgniteCheckedException("Failed to create the path due to secondary file system exception: " +
                            path, err);
                    }
                };

                try {
                    return synchronizeAndExecute(task, fs, false, path.parent());
                }
                finally {
                    for (IgfsEvent evt : pendingEvts)
                        evts.record(evt);
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to create directory in DUAL mode because Grid is stopping: " +
                path);
    }

    /**
     * Rename path in DUAL mode.
     *
     * @param fs Secondary file system.
     * @param src Source path.
     * @param dest Destination path.
     * @return Operation result.
     * @throws IgniteCheckedException If failed.
     */
    public boolean renameDual(final IgfsSecondaryFileSystem fs, final IgfsPath src, final IgfsPath dest) throws
        IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert src != null;
                assert dest != null;

                if (src.parent() == null)
                    return false; // Root directory cannot be renamed.

                // Events to fire (can be done outside of a transaction).
                final Collection<IgfsEvent> pendingEvts = new LinkedList<>();

                SynchronizationTask<Boolean> task = new SynchronizationTask<Boolean>() {
                    @Override public Boolean onSuccess(Map<IgfsPath, IgfsFileInfo> infos) throws Exception {
                        IgfsFileInfo srcInfo = infos.get(src);
                        IgfsFileInfo srcParentInfo = infos.get(src.parent());
                        IgfsFileInfo destInfo = infos.get(dest);
                        IgfsFileInfo destParentInfo = dest.parent() != null ? infos.get(dest.parent()) : null;

                        // Source path and destination (or destination parent) must exist.
                        if (srcInfo == null)
                            throw fsException(new IgfsPathNotFoundException("Failed to rename " +
                                    "(source path not found): " + src));

                        if (destInfo == null && destParentInfo == null)
                            throw fsException(new IgfsPathNotFoundException("Failed to rename " +
                                "(destination path not found): " + dest));

                        // Delegate to the secondary file system.
                        fs.rename(src, dest);

                        // Rename was successful, perform compensation in the local file system.
                        if (destInfo == null) {
                            // Move and rename.
                            assert destParentInfo != null;

                            moveNonTx(srcInfo.id(), src.name(), srcParentInfo.id(), dest.name(), destParentInfo.id());
                        }
                        else {
                            // Move.
                            if (destInfo.isFile())
                                throw fsException("Failed to rename the path in the local file system " +
                                    "because destination path already exists and it is a file: " + dest);
                            else
                                moveNonTx(srcInfo.id(), src.name(), srcParentInfo.id(), src.name(), destInfo.id());
                        }

                        // Record event if needed.
                        if (srcInfo.isFile()) {
                            if (evts.isRecordable(EVT_IGFS_FILE_RENAMED))
                                pendingEvts.add(new IgfsEvent(
                                    src,
                                    destInfo == null ? dest : new IgfsPath(dest, src.name()),
                                    locNode,
                                    EVT_IGFS_FILE_RENAMED));
                        }
                        else if (evts.isRecordable(EVT_IGFS_DIR_RENAMED))
                            pendingEvts.add(new IgfsEvent(src, dest, locNode, EVT_IGFS_DIR_RENAMED));

                        return true;
                    }

                    @Override public Boolean onFailure(@Nullable Exception err) throws IgniteCheckedException {
                        U.error(log, "Path rename in DUAL mode failed [source=" + src + ", destination=" + dest + ']',
                            err);

                        throw new IgniteCheckedException("Failed to rename the path due to secondary file system " +
                            "exception: " + src, err);
                    }
                };

                try {
                    return synchronizeAndExecute(task, fs, false, src, dest);
                }
                finally {
                    for (IgfsEvent evt : pendingEvts)
                        evts.record(evt);
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to rename in DUAL mode because Grid is stopping [src=" + src +
                ", dest=" + dest + ']');
    }

    /**
     * Delete path in DUAL mode.
     *
     * @param fs Secondary file system.
     * @param path Path to update.
     * @param recursive Recursive flag.
     * @return Operation result.
     * @throws IgniteCheckedException If delete failed.
     */
    public boolean deleteDual(final IgfsSecondaryFileSystem fs, final IgfsPath path, final boolean recursive)
        throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert path != null;

                SynchronizationTask<Boolean> task = new SynchronizationTask<Boolean>() {
                    @Override public Boolean onSuccess(Map<IgfsPath, IgfsFileInfo> infos) throws Exception {
                        IgfsFileInfo info = infos.get(path);

                        if (info == null)
                            return false; // File doesn't exist in the secondary file system.

                        if (!fs.delete(path, recursive))
                            return false; // Delete failed remotely.

                        if (path.parent() != null) {
                            assert infos.containsKey(path.parent());

                            softDeleteNonTx(infos.get(path.parent()).id(), path.name(), info.id());
                        }
                        else {
                            assert ROOT_ID.equals(info.id());

                            softDeleteNonTx(null, path.name(), info.id());
                        }

                        // Update the deleted file info with path information for delete worker.
                        id2InfoPrj.invoke(info.id(), new UpdatePath(path));

                        return true; // No additional handling is required.
                    }

                    @Override public Boolean onFailure(@Nullable Exception err) throws IgniteCheckedException {
                        U.error(log, "Path delete in DUAL mode failed [path=" + path + ", recursive=" + recursive + ']',
                            err);

                        throw new IgniteCheckedException("Failed to delete the path due to secondary file system exception: ",
                            err);
                    }
                };

                Boolean res = synchronizeAndExecute(task, fs, false, Collections.singleton(TRASH_ID), path);

                delWorker.signal();

                return res;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to delete in DUAL mode because Grid is stopping: " + path);
    }

    /**
     * Update path in DUAL mode.
     *
     * @param fs Secondary file system.
     * @param path Path to update.
     * @param props Properties to be applied.
     * @return Update file info.
     * @throws IgniteCheckedException If update failed.
     */
    public IgfsFileInfo updateDual(final IgfsSecondaryFileSystem fs, final IgfsPath path, final Map<String, String> props)
        throws IgniteCheckedException {
        assert fs != null;
        assert path != null;
        assert props != null && !props.isEmpty();

        if (busyLock.enterBusy()) {
            try {
                SynchronizationTask<IgfsFileInfo> task = new SynchronizationTask<IgfsFileInfo>() {
                    @Override public IgfsFileInfo onSuccess(Map<IgfsPath, IgfsFileInfo> infos)
                        throws Exception {
                        if (infos.get(path) == null)
                            return null;

                        fs.update(path, props);

                        assert path.parent() == null || infos.get(path.parent()) != null;

                        return updatePropertiesNonTx(infos.get(path.parent()).id(), infos.get(path).id(), path.name(),
                            props);
                    }

                    @Override public IgfsFileInfo onFailure(@Nullable Exception err) throws IgniteCheckedException {
                        U.error(log, "Path update in DUAL mode failed [path=" + path + ", properties=" + props + ']',
                            err);

                        throw new IgniteCheckedException("Failed to update the path due to secondary file system exception: " +
                            path, err);
                    }
                };

                return synchronizeAndExecute(task, fs, false, path);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to update in DUAL mode because Grid is stopping: " + path);
    }

    /**
     * Synchronize directory structure with the secondary file system.
     *
     * @param fs Secondary file system.
     * @param startPath Start path.
     * @param startPathInfo Start path info.
     * @param endPath End path.
     * @param strict Whether all paths must exist in the secondary file system.
     * @param created Optional map where data about all created values is put.
     * @return File info of the end path.
     * @throws IgniteCheckedException If failed.
     */
    private IgfsFileInfo synchronize(IgfsSecondaryFileSystem fs,
        IgfsPath startPath,
        IgfsFileInfo startPathInfo,
        IgfsPath endPath,
        boolean strict,
        @Nullable Map<IgfsPath, IgfsFileInfo> created)
        throws IgniteCheckedException
    {
        assert fs != null;
        assert startPath != null && startPathInfo != null && endPath != null;

        validTxState(true);

        IgfsFileInfo parentInfo = startPathInfo;

        List<String> components = endPath.components();

        IgfsPath curPath = startPath;

        for (int i = startPath.components().size(); i < components.size(); i++) {
            curPath = new IgfsPath(curPath, components.get(i));

            if (created != null && created.containsKey(curPath))
                // Re-use already created info.
                parentInfo = created.get(curPath);
            else {
                // Get file status from the secondary file system.
                IgfsFile status;

                try {
                    status = fs.info(curPath);
                }
                catch (IgniteException e) {
                    throw new IgniteCheckedException("Failed to get path information: " + e, e);
                }

                if (status != null) {
                    if (!status.isDirectory() && !curPath.equals(endPath))
                        throw new IgniteCheckedException("Failed to create path the locally because secondary file system " +
                            "directory structure was modified concurrently and the path is not a directory as " +
                            "expected: " + curPath);
                }
                else {
                    if (strict) {
                        throw new IgniteCheckedException("Failed to create path locally due to secondary file system " +
                            "exception: " + curPath);
                    }
                    else if (created != null)
                        created.put(curPath.parent(), parentInfo);

                    return null;
                }

                // Recreate the path locally.
                IgfsFileInfo curInfo = status.isDirectory() ? new IgfsFileInfo(true, status.properties()) :
                    new IgfsFileInfo(igfsCtx.configuration().getBlockSize(), status.length(),
                        igfsCtx.igfs().evictExclude(curPath, false), status.properties());

                IgniteUuid oldId = putIfAbsentNonTx(parentInfo.id(), components.get(i), curInfo);

                if (oldId != null)
                    curInfo = info(oldId);

                if (created != null)
                    created.put(curPath, curInfo);

                parentInfo = curInfo;
            }
        }

        return parentInfo;
    }

    /**
     * Synchronize file system structure and then execute provided task. All these actions are performed withing
     * the transaction.
     *
     * @param task Task to execute.
     * @param fs File system.
     * @param strict Whether paths must be re-created strictly.
     * @param paths Paths to synchronize.
     * @return Result of task execution.
     * @throws IgniteCheckedException If failed.
     */
    private <T> T synchronizeAndExecute(SynchronizationTask<T> task,
        IgfsSecondaryFileSystem fs,
        boolean strict,
        IgfsPath... paths)
        throws IgniteCheckedException
    {
        return synchronizeAndExecute(task, fs, strict, null, paths);
    }

    /**
     * Synchronize file system structure and then execute provided task. All these actions are performed withing
     * the transaction.
     *
     * @param task Task to execute.
     * @param fs File system.
     * @param strict Whether paths must be re-created strictly.
     * @param extraLockIds Additional IDs to lock (optional).
     * @param paths Paths to synchronize.
     * @return Result of task execution.
     * @throws IgniteCheckedException If failed.
     */
    private <T> T synchronizeAndExecute(SynchronizationTask<T> task,
        IgfsSecondaryFileSystem fs,
        boolean strict,
        @Nullable Collection<IgniteUuid> extraLockIds,
        IgfsPath... paths)
        throws IgniteCheckedException
    {
        assert task != null;
        assert fs != null;
        assert paths != null && paths.length > 0;

        // Sort paths so that we know in which order to synchronize them.
        if (paths.length > 1)
            Arrays.sort(paths);

        boolean finished = false;

        T res = null;

        while (!finished) {
            // Obtain existing IDs outside the transaction.
            List<List<IgniteUuid>> pathIds = new ArrayList<>(paths.length);

            for (IgfsPath path : paths)
                pathIds.add(fileIds(path));

            // Start pessimistic.
            IgniteInternalTx tx = metaCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);

            try {
                // Lock the very first existing parents and possibly the leaf as well.
                Map<IgfsPath, IgfsPath> pathToParent = new HashMap<>();

                Map<IgfsPath, IgniteUuid> pathToId = new HashMap<>();

                for (int i = 0; i < paths.length; i++) {
                    IgfsPath path = paths[i];

                    // Determine the very first existing parent
                    List<IgniteUuid> ids = pathIds.get(i);

                    if (ids.size() > 1) {
                        // The path is not root.
                        IgfsPath parentPath = path.parent();
                        IgniteUuid parentId = ids.get(ids.size() - 2);

                        for (int j = ids.size() - 3; j >= 0; j--) {
                            if (parentId != null)
                                break;
                            else {
                                parentPath = parentPath.parent();
                                parentId = ids.get(j);
                            }
                        }

                        assert parentPath != null && parentId != null;

                        pathToParent.put(path, parentPath);

                        pathToId.put(parentPath, parentId);
                    }

                    IgniteUuid pathId = ids.get(ids.size() - 1);

                    if (pathId != null)
                        pathToId.put(path, pathId);
                }

                IgniteUuid[] lockArr = new IgniteUuid[extraLockIds == null ? pathToId.size() : pathToId.size() +
                    extraLockIds.size()];

                int idx = 0;

                for (IgniteUuid id : pathToId.values())
                    lockArr[idx++] = id;

                if (extraLockIds != null) {
                    for (IgniteUuid id : extraLockIds)
                        lockArr[idx++] = id;
                }

                Map<IgniteUuid, IgfsFileInfo> idToInfo = lockIds(lockArr);

                if (extraLockIds != null) {
                    for (IgniteUuid id : extraLockIds)
                        idToInfo.remove(id);
                }

                // Ensure that locked IDs still point to expected paths.
                IgfsPath changed = null;

                for (Map.Entry<IgfsPath, IgniteUuid> entry : pathToId.entrySet()) {
                    if (!idToInfo.containsKey(entry.getValue()) ||
                        !F.eq(entry.getValue(), fileId(entry.getKey(), true))) {
                        changed = entry.getKey();

                        break;
                    }
                }

                if (changed != null) {
                    finished = true;

                    throw fsException(new IgfsConcurrentModificationException("File system entry has been " +
                        "modified concurrently: " + changed));
                }
                else {
                    boolean newParents = false;

                    // Check whether any new parents appeared before we have obtained the locks.
                    for (int i = 0; i < paths.length; i++) {
                        List<IgniteUuid> newIds = fileIds(paths[i], true);

                        if (!pathIds.get(i).equals(newIds)) {
                            newParents = true;

                            break;
                        }
                    }

                    if (newParents)
                        continue; // Release all locks and try again.
                    else {
                        // Perform synchronization.
                        Map<IgfsPath, IgfsFileInfo> infos = new HashMap<>();

                        TreeMap<IgfsPath, IgfsFileInfo> created = new TreeMap<>();

                        for (IgfsPath path : paths) {
                            IgfsPath parentPath = path.parent();

                            if (pathToId.containsKey(path)) {
                                infos.put(path, info(pathToId.get(path)));

                                if (parentPath != null)
                                    infos.put(parentPath, info(pathToId.get(parentPath)));
                            }
                            else {
                                IgfsPath firstParentPath = pathToParent.get(path);

                                assert firstParentPath != null;
                                assert pathToId.get(firstParentPath) != null;

                                IgfsFileInfo info = synchronize(fs,
                                    firstParentPath,
                                    idToInfo.get(pathToId.get(firstParentPath)),
                                    path,
                                    strict,
                                    created);

                                assert strict && info != null || !strict;

                                if (info != null)
                                    infos.put(path, info);

                                if (parentPath != null) {
                                    if (parentPath.equals(firstParentPath))
                                        infos.put(firstParentPath, idToInfo.get(pathToId.get(firstParentPath)));
                                    else {
                                        assert strict && created.get(parentPath) != null || !strict;

                                        if (created.get(parentPath) != null)
                                            infos.put(parentPath, created.get(parentPath));
                                        else {
                                            // Put the last created path.
                                            infos.put(created.lastKey(), created.get(created.lastKey()));
                                        }
                                    }
                                }
                            }
                        }

                        // Finally, execute the task.
                        finished = true;

                        try {
                            res = task.onSuccess(infos);
                        }
                        catch (Exception e) {
                            res = task.onFailure(e);
                        }
                    }
                }

                tx.commit();
            }
            catch (IgniteCheckedException e) {
                if (!finished) {
                    finished = true;

                    res = task.onFailure(e);
                }
                else
                    throw e;
            }
            finally {
                tx.close();
            }
        }

        return res;
    }

    /**
     * Update cached value with closure.
     *
     * @param cache Cache projection to work with.
     * @param key Key to retrieve/update the value for.
     * @param c Closure to apply to cached value.
     * @return {@code True} if value was stored in cache, {@code false} otherwise.
     * @throws IgniteCheckedException If operation failed.
     */
    private <K, V> boolean putx(IgniteInternalCache<K, V> cache, K key, IgniteClosure<V, V> c) throws IgniteCheckedException {
        assert validTxState(true);

        V oldVal = cache.get(key);
        V newVal = c.apply(oldVal);

        return newVal == null ? cache.remove(key) : cache.put(key, newVal);
    }

    /**
     * Check transaction is (not) started.
     *
     * @param inTx Expected transaction state.
     * @return Transaction state is correct.
     */
    private boolean validTxState(boolean inTx) {
        boolean txState = inTx == (metaCache.tx() != null);

        assert txState : (inTx ? "Method cannot be called outside transaction " :
            "Method cannot be called in transaction ") + "[tx=" + metaCache.tx() + ", threadId=" +
            Thread.currentThread().getId() + ']';

        return txState;
    }

    /**
     * Updates last access and last modification times.
     *
     * @param parentId File parent ID.
     * @param fileId File ID to update.
     * @param fileName File name to update. Must match file ID.
     * @param accessTime Access time to set. If {@code -1}, will not be updated.
     * @param modificationTime Modification time to set. If {@code -1}, will not be updated.
     * @throws IgniteCheckedException If update failed.
     */
    public void updateTimes(IgniteUuid parentId, IgniteUuid fileId, String fileName, long accessTime,
        long modificationTime) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                // Start pessimistic transaction.
                IgniteInternalTx tx = metaCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);

                try {
                    Map<IgniteUuid, IgfsFileInfo> infoMap = lockIds(fileId, parentId);

                    IgfsFileInfo fileInfo = infoMap.get(fileId);

                    if (fileInfo == null)
                        throw fsException(new IgfsPathNotFoundException("Failed to update times " +
                                "(path was not found): " + fileName));

                    IgfsFileInfo parentInfo = infoMap.get(parentId);

                    if (parentInfo == null)
                        throw fsException(new IgfsPathNotFoundException("Failed to update times " +
                            "(parent was not found): " + fileName));

                    IgfsListingEntry entry = parentInfo.listing().get(fileName);

                    // Validate listing.
                    if (entry == null || !entry.fileId().equals(fileId))
                        throw fsException(new IgfsConcurrentModificationException("Failed to update times " +
                                "(file concurrently modified): " + fileName));

                    assert parentInfo.isDirectory();

                    IgfsFileInfo updated = new IgfsFileInfo(fileInfo,
                        accessTime == -1 ? fileInfo.accessTime() : accessTime,
                        modificationTime == -1 ? fileInfo.modificationTime() : modificationTime);

                    id2InfoPrj.put(fileId, updated);

                    id2InfoPrj.invoke(parentId, new UpdateListingEntry(fileId, fileName, 0, accessTime,
                        modificationTime));

                    tx.commit();
                }
                finally {
                    tx.close();
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to update times because Grid is stopping [parentId=" + parentId +
                ", fileId=" + fileId + ", fileName=" + fileName + ']');
    }

    /**
     * @param msg Error message.
     * @return Checked exception.
     */
    private static IgniteCheckedException fsException(String msg) {
        return new IgniteCheckedException(new IgfsException(msg));
    }

    /**
     * @param msg Error message.
     * @return Checked exception.
     */
    private static IgniteCheckedException fsException(IgfsException err) {
        return new IgniteCheckedException(err);
    }

    /**
     * Synchronization task interface.
     */
    private static interface SynchronizationTask<T> {
        /**
         * Callback handler in case synchronization was successful.
         *
         * @param infos Map from paths to corresponding infos.
         * @return Task result.
         * @throws Exception If failed.
         */
        public T onSuccess(Map<IgfsPath, IgfsFileInfo> infos) throws Exception;

        /**
         * Callback handler in case synchronization failed.
         *
         * @param err Optional exception.
         * @return Task result.
         * @throws IgniteCheckedException In case exception is to be thrown in that case.
         */
        public T onFailure(Exception err) throws IgniteCheckedException;
    }

    /**
     * Path descriptor.
     */
    private static class PathDescriptor {
        /** Path. */
        private final IgfsPath path;

        /** Resolved IDs. */
        private final List<IgniteUuid> ids;

        /** Parent path. */
        private IgfsPath parentPath;

        /** Parent path info. */
        private IgfsFileInfo parentInfo;

        /**
         * Constructor.
         *
         * @param path Path.
         * @param ids Resolved path IDs.
         * @param parentPath Parent path.
         * @param parentInfo Parent info.
         */
        PathDescriptor(IgfsPath path, List<IgniteUuid> ids, IgfsPath parentPath, IgfsFileInfo parentInfo) {
            assert path != null;
            assert ids != null && !ids.isEmpty();
            assert parentPath == null && parentInfo == null || parentPath != null && parentInfo != null;
            assert parentPath == null || parentPath != null && path.isSubDirectoryOf(parentPath);

            this.path = path;
            this.ids = ids;
            this.parentPath = parentPath;
            this.parentInfo = parentInfo;
        }

        /**
         * Get resolved path ids.
         *
         * @return Path ids.
         */
        private Collection<IgniteUuid> ids() {
            return ids;
        }

        /**
         * Get path ID from the end. E.g. endId(1) will return the last element.
         * @param i Element index from the end.
         *
         * @return Path ID from the end.
         */
        private IgniteUuid endId(int i) {
            return ids.get(ids.size() - i);
        }

        /**
         * Update ID with the given index.
         *
         * @param newParentPath New parent path.
         * @param newParentInfo New parent info.
         */
        private void updateParent(IgfsPath newParentPath, IgfsFileInfo newParentInfo) {
            assert newParentPath != null;
            assert newParentInfo != null;
            assert path.isSubDirectoryOf(newParentPath);

            parentPath = newParentPath;
            parentInfo = newParentInfo;

            ids.set(newParentPath.components().size(), newParentInfo.id());
        }

        /**
         * Get parent path.
         *
         * @return Parent path.
         */
        private IgfsPath parentPath() {
            return parentPath;
        }

        /**
         * Get parent path info.
         *
         * @return Parent path info.
         */
        private IgfsFileInfo parentInfo() {
            return parentInfo;
        }
    }

    /**
     * Updates file length information in parent listing.
     */
    private static final class UpdateListingEntry implements EntryProcessor<IgniteUuid, IgfsFileInfo, Void>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** File name. */
        private String fileName;

        /** File id. */
        private IgniteUuid fileId;

        /** Length delta. */
        private long lenDelta;

        /** Last access time. */
        private long accessTime;

        /** Last modification time. */
        private long modificationTime;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public UpdateListingEntry() {
            // No-op.
        }

        /**
         * @param fileId Expected file id in parent directory listing.
         * @param fileName File name.
         * @param lenDelta Length delta.
         * @param accessTime Last access time.
         * @param modificationTime Last modification time.
         */
        private UpdateListingEntry(IgniteUuid fileId,
            String fileName,
            long lenDelta,
            long accessTime,
            long modificationTime) {
            this.fileId = fileId;
            this.fileName = fileName;
            this.lenDelta = lenDelta;
            this.accessTime = accessTime;
            this.modificationTime = modificationTime;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<IgniteUuid, IgfsFileInfo> e, Object... args) {
            IgfsFileInfo fileInfo = e.getValue();

            Map<String, IgfsListingEntry> listing = fileInfo.listing();

            IgfsListingEntry entry = listing.get(fileName);

            if (entry == null || !entry.fileId().equals(fileId))
                return null;

            entry = new IgfsListingEntry(entry, entry.length() + lenDelta,
                accessTime == -1 ? entry.accessTime() : accessTime,
                modificationTime == -1 ? entry.modificationTime() : modificationTime);

            // Create new map to replace info.
            listing = new HashMap<>(listing);

            // Modify listing map in-place since map is serialization-safe.
            listing.put(fileName, entry);

            e.setValue(new IgfsFileInfo(listing, fileInfo));

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeGridUuid(out, fileId);
            out.writeUTF(fileName);
            out.writeLong(lenDelta);
            out.writeLong(accessTime);
            out.writeLong(modificationTime);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException {
            fileId = U.readGridUuid(in);
            fileName = in.readUTF();
            lenDelta = in.readLong();
            accessTime = in.readLong();
            modificationTime = in.readLong();
        }
    }

    /**
     * Update directory listing closure.
     */
    @GridInternal
    private static final class UpdateListing implements EntryProcessor<IgniteUuid, IgfsFileInfo, Void>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** File name to add into parent listing. */
        private String fileName;

        /** File ID.*/
        private IgfsListingEntry entry;

        /** Update operation: remove entry from listing if {@code true} or add entry to listing if {@code false}. */
        private boolean rmv;

        /**
         * Constructs update directory listing closure.
         *
         * @param fileName File name to add into parent listing.
         * @param entry Listing entry to add or remove.
         * @param rmv Remove entry from listing if {@code true} or add entry to listing if {@code false}.
         */
        private UpdateListing(String fileName, IgfsListingEntry entry, boolean rmv) {
            assert fileName != null;
            assert entry != null;

            this.fileName = fileName;
            this.entry = entry;
            this.rmv = rmv;
        }

        /**
         * Empty constructor required for {@link Externalizable}.
         *
         */
        public UpdateListing() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<IgniteUuid, IgfsFileInfo> e, Object... args) {
            IgfsFileInfo fileInfo = e.getValue();

            assert fileInfo != null : "File info not found for the child: " + entry.fileId();
            assert fileInfo.isDirectory();

            Map<String, IgfsListingEntry> listing =
                U.newHashMap(fileInfo.listing().size() + (rmv ? 0 : 1));

            listing.putAll(fileInfo.listing());

            if (rmv) {
                IgfsListingEntry oldEntry = listing.get(fileName);

                if (oldEntry == null || !oldEntry.fileId().equals(entry.fileId()))
                    throw new IgniteException("Directory listing doesn't contain expected file" +
                        " [listing=" + listing + ", fileName=" + fileName + ", entry=" + entry + ']');

                // Modify listing in-place.
                listing.remove(fileName);
            }
            else {
                // Modify listing in-place.
                IgfsListingEntry oldEntry = listing.put(fileName, entry);

                if (oldEntry != null && !oldEntry.fileId().equals(entry.fileId()))
                    throw new IgniteException("Directory listing contains unexpected file" +
                        " [listing=" + listing + ", fileName=" + fileName + ", entry=" + entry +
                        ", oldEntry=" + oldEntry + ']');
            }

            e.setValue(new IgfsFileInfo(listing, fileInfo));

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, fileName);
            out.writeObject(entry);
            out.writeBoolean(rmv);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            fileName = U.readString(in);
            entry = (IgfsListingEntry)in.readObject();
            rmv = in.readBoolean();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(UpdateListing.class, this);
        }
    }

    /**
     * Update path closure.
     */
    @GridInternal
    private static final class UpdatePath implements EntryProcessor<IgniteUuid, IgfsFileInfo, Void>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** New path. */
        private IgfsPath path;

        /**
         * @param path Path.
         */
        private UpdatePath(IgfsPath path) {
            this.path = path;
        }

        /**
         * Default constructor (required by Externalizable).
         */
        public UpdatePath() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<IgniteUuid, IgfsFileInfo> e, Object... args) {
            IgfsFileInfo info = e.getValue();

            e.setValue(builder(info).path(path).build());

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(path);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            path = (IgfsPath)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(UpdatePath.class, this);
        }
    }
}