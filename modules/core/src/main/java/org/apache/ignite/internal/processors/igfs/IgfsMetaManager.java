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
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.IgfsEvent;
import org.apache.ignite.igfs.IgfsConcurrentModificationException;
import org.apache.ignite.igfs.IgfsDirectoryNotEmptyException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsParentNotDirectoryException;
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
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_IGFS_DIR_CREATED;
import static org.apache.ignite.events.EventType.EVT_IGFS_DIR_RENAMED;
import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_CREATED;
import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_RENAMED;
import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_OPENED_WRITE;
import static org.apache.ignite.internal.processors.igfs.IgfsFileInfo.builder;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache based structure (meta data) manager.
 */
@SuppressWarnings("all")
public class IgfsMetaManager extends IgfsManager {
    /** Comparator for Id sorting. */
    private static final Comparator<IgniteUuid> PATH_ID_SORTING_COMPARATOR
            = new Comparator<IgniteUuid>() {
        @Override public int compare(IgniteUuid u1, IgniteUuid u2) {
            if (u1 == u2)
                return 0;

            if (u1 == null)
                return -1;

            return u1.compareTo(u2);
        }
    };

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
                validTxState(false);

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
    @Nullable private IgniteUuid fileId(IgniteUuid parentId, String fileName, boolean skipTx)
        throws IgniteCheckedException {
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
                validTxState(false);

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

        ids.add(IgfsUtils.ROOT_ID); // Always add root ID.

        IgniteUuid fileId = IgfsUtils.ROOT_ID;

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
     * NB: this method is used both in Tx and out of Tx.
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

                IgfsFileInfo info = getInfo(fileId);

                // Force root ID always exist in cache.
                if (info == null && IgfsUtils.ROOT_ID.equals(fileId))
                    info = createSystemEntryIfAbsent(fileId);

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
                validTxState(false);

                assert fileIds != null;

                if (F.isEmpty(fileIds))
                    return Collections.emptyMap();

                Map<IgniteUuid, IgfsFileInfo> map = getInfos(fileIds);

                // Force root ID always exist in cache.
                if (fileIds.contains(IgfsUtils.ROOT_ID) && !map.containsKey(IgfsUtils.ROOT_ID)) {
                    map = new GridLeanMap<>(map);

                    map.put(IgfsUtils.ROOT_ID, createSystemEntryIfAbsent(IgfsUtils.ROOT_ID));
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
     * @param delete If file is being locked for delete.
     * @return Locked file info or {@code null} if file cannot be locked or doesn't exist.
     * @throws IgniteCheckedException If the file with such id does not exist, or on another failure.
     */
    public @Nullable IgfsFileInfo lock(IgniteUuid fileId, boolean delete) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                assert fileId != null;

                IgniteInternalTx tx = startTx();

                try {
                    // Lock file ID for this transaction.
                    IgfsFileInfo oldInfo = info(fileId);

                    if (oldInfo == null)
                        return null;

                    if (oldInfo.lockId() != null)
                        return null; // The file is already locked, we cannot lock it.

                    IgfsFileInfo newInfo = invokeLock(fileId, delete);

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
     * Create file lock ID.
     *
     * @param delete If lock ID is required for file deletion.
     * @return Lock ID.
     */
    private IgniteUuid createFileLockId(boolean delete) {
        if (delete)
            return IgfsUtils.DELETE_LOCK_ID;

        return IgniteUuid.fromUuid(locNode.id());
    }

    /**
     * Remove explicit lock on file held by the current thread.
     *
     * @param info File info to unlock.
     * @param modificationTime Modification time to write to file info.
     * @throws IgniteCheckedException If failed.
     */
    public void unlock(final IgfsFileInfo info, final long modificationTime) throws IgniteCheckedException {
        validTxState(false);

        assert info != null;

        if (busyLock.enterBusy()) {
            try {
                final IgniteUuid lockId = info.lockId();

                if (lockId == null)
                    return;

                // Temporary clear interrupted state for unlocking.
                final boolean interrupted = Thread.interrupted();

                try {
                    IgfsUtils.doInTransactionWithRetries(id2InfoPrj, new IgniteOutClosureX<Void>() {
                        @Override public Void applyx() throws IgniteCheckedException {
                            validTxState(true);

                            IgniteUuid fileId = info.id();

                            // Lock file ID for this transaction.
                            IgfsFileInfo oldInfo = info(fileId);

                            if (oldInfo == null)
                                throw fsException(new IgfsPathNotFoundException("Failed to unlock file (file not " +
                                    "found): " + fileId));

                            if (!info.lockId().equals(oldInfo.lockId()))
                                throw new IgniteCheckedException("Failed to unlock file (inconsistent file lock ID) " +
                                    "[fileId=" + fileId + ", lockId=" + info.lockId() + ", actualLockId=" +
                                    oldInfo.lockId() + ']');

                            id2InfoPrj.invoke(fileId, new FileUnlockProcessor(modificationTime));

                            return null;
                        }
                    });
                }
                finally {
                    validTxState(false);

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
        validTxState(true);

        assert fileIds != null && fileIds.length > 0;

        Arrays.sort(fileIds);

        return lockIds(Arrays.asList(fileIds));
    }

    /**
     * Answers if the collection is sorted.
     *
     * @param col The collection to check.
     * @param <T> The type of the collection elements.
     * @return If the collection sorted.
     */
    private static <T extends Comparable<T>> boolean isSorted(Collection<T> col) {
        T prev = null;

        for (T t: col) {
            if (t == null)
                throw new NullPointerException("Collections should not contain nulls");

            if (prev != null && prev.compareTo(t) > 0)
                return false; // disordered.

            prev = t;
        }

        return true;
    }

    /**
     * Lock file IDs.
     *
     * @param fileIds File IDs (sorted).
     * @return Map with lock info.
     * @throws IgniteCheckedException If failed.
     */
    private Map<IgniteUuid, IgfsFileInfo> lockIds(Collection<IgniteUuid> fileIds) throws IgniteCheckedException {
        assert isSorted(fileIds);
        validTxState(true);

        if (log.isDebugEnabled())
            log.debug("Locking file ids: " + fileIds);

        // Lock files and get their infos.
        Map<IgniteUuid, IgfsFileInfo> map = getInfos(fileIds);

        if (log.isDebugEnabled())
            log.debug("Locked file ids: " + fileIds);

        for (IgniteUuid fileId : fileIds) {
            if (IgfsUtils.isRootOrTrashId(fileId)) {
                if (!map.containsKey(fileId))
                    map.put(fileId, createSystemEntryIfAbsent(fileId));
            }
        }

        // Returns detail's map for locked IDs.
        return map;
    }

    /**
     * create system entry if it is absent.
     *
     * @param id System entry ID.
     * @return Value of created or existing system entry.
     * @throws IgniteCheckedException On error.
     */
    private IgfsFileInfo createSystemEntryIfAbsent(IgniteUuid id)
        throws IgniteCheckedException {
        assert IgfsUtils.isRootOrTrashId(id);

        IgfsFileInfo info = new IgfsFileInfo(id);

        IgfsFileInfo oldInfo = id2InfoPrj.getAndPutIfAbsent(id, info);

        if (oldInfo != null)
            info = oldInfo;

        return info;
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
                return fileForFragmentizer0(IgfsUtils.ROOT_ID, exclude);
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
    private Map<String, IgfsListingEntry> directoryListing(IgniteUuid fileId, boolean skipTx)
        throws IgniteCheckedException {
        assert fileId != null;

        IgfsFileInfo info = skipTx ? id2InfoPrj.getAllOutTx(Collections.singleton(fileId)).get(fileId) :
            getInfo(fileId);

        return info == null ? Collections.<String, IgfsListingEntry>emptyMap() : info.listing();
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

        if (parentInfo == null)
            throw fsException(new IgfsPathNotFoundException("Failed to lock parent directory (not found): " +
                parentId));

        if (!parentInfo.isDirectory())
            throw fsException(new IgfsPathIsNotDirectoryException("Parent file is not a directory: " + parentInfo));

        IgfsListingEntry childEntry = parentInfo.listing().get(fileName);

        if (childEntry != null)
            return childEntry.fileId();

        createNewEntry(newFileInfo, parentId, fileName);

        return null;
    }

    /**
     * Move routine.
     *
     * @param srcPath Source path.
     * @param dstPath Destinatoin path.
     * @return File info of renamed entry.
     * @throws IgniteCheckedException In case of exception.
     */
    public IgfsFileInfo move(IgfsPath srcPath, IgfsPath dstPath) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                // 1. First get source and destination path IDs.
                List<IgniteUuid> srcPathIds = fileIds(srcPath);
                List<IgniteUuid> dstPathIds = fileIds(dstPath);

                final Set<IgniteUuid> allIds = new TreeSet<>(PATH_ID_SORTING_COMPARATOR);

                allIds.addAll(srcPathIds);

                final IgniteUuid dstLeafId = dstPathIds.get(dstPathIds.size() - 1);

                if (dstLeafId == null) {
                    // Delete null entry for the unexisting destination element:
                    dstPathIds.remove(dstPathIds.size() - 1);
                }

                allIds.addAll(dstPathIds);

                if (allIds.remove(null)) {
                    throw new IgfsPathNotFoundException("Failed to perform move because some path component was " +
                            "not found. [src=" + srcPath + ", dst=" + dstPath + ']');
                }

                // 2. Start transaction.
                IgniteInternalTx tx = startTx();

                try {
                    // 3. Obtain the locks.
                    final Map<IgniteUuid, IgfsFileInfo> allInfos = lockIds(allIds);

                    // 4. Verify integrity of source directory.
                    if (!verifyPathIntegrity(srcPath, srcPathIds, allInfos)) {
                        throw new IgfsPathNotFoundException("Failed to perform move because source directory " +
                            "structure changed concurrently [src=" + srcPath + ", dst=" + dstPath + ']');
                    }

                    // 5. Verify integrity of destination directory.
                    final IgfsPath dstDirPath = dstLeafId != null ? dstPath : dstPath.parent();

                    if (!verifyPathIntegrity(dstDirPath, dstPathIds, allInfos)) {
                        throw new IgfsPathNotFoundException("Failed to perform move because destination directory " +
                            "structure changed concurrently [src=" + srcPath + ", dst=" + dstPath + ']');
                    }

                    // 6. Calculate source and destination targets which will be changed.
                    IgniteUuid srcTargetId = srcPathIds.get(srcPathIds.size() - 2);
                    IgfsFileInfo srcTargetInfo = allInfos.get(srcTargetId);
                    String srcName = srcPath.name();

                    IgniteUuid dstTargetId;
                    IgfsFileInfo dstTargetInfo;
                    String dstName;

                    if (dstLeafId != null) {
                        // Destination leaf exists. Check if it is an empty directory.
                        IgfsFileInfo dstLeafInfo = allInfos.get(dstLeafId);

                        assert dstLeafInfo != null;

                        if (dstLeafInfo.isDirectory()) {
                            // Destination is a directory.
                            dstTargetId = dstLeafId;
                            dstTargetInfo = dstLeafInfo;
                            dstName = srcPath.name();
                        }
                        else {
                            // Error, destination is existing file.
                            throw new IgfsPathAlreadyExistsException("Failed to perform move " +
                                "because destination points to " +
                                "existing file [src=" + srcPath + ", dst=" + dstPath + ']');
                        }
                    }
                    else {
                        // Destination leaf doesn't exist, so we operate on parent.
                        dstTargetId = dstPathIds.get(dstPathIds.size() - 1);
                        dstTargetInfo = allInfos.get(dstTargetId);
                        dstName = dstPath.name();
                    }

                    assert dstTargetInfo != null;
                    assert dstTargetInfo.isDirectory();

                    // 7. Last check: does destination target already have listing entry with the same name?
                    if (dstTargetInfo.hasChild(dstName)) {
                        throw new IgfsPathAlreadyExistsException("Failed to perform move because destination already " +
                            "contains entry with the same name existing file [src=" + srcPath +
                            ", dst=" + dstPath + ']');
                    }

                    // 8. Actual move: remove from source parent and add to destination target.
                    IgfsListingEntry entry = srcTargetInfo.listing().get(srcName);

                    transferEntry(entry, srcTargetId, srcName, dstTargetId, dstName);

                    tx.commit();

                    IgfsPath realNewPath = new IgfsPath(dstDirPath, dstName);

                    IgfsFileInfo moved = allInfos.get(srcPathIds.get(srcPathIds.size() - 1));

                    // Set the new path to the info to simplify event creation:
                    return IgfsFileInfo.builder(moved).path(realNewPath).build();
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
            throw new IllegalStateException("Failed to perform move because Grid is stopping [srcPath=" +
                srcPath + ", dstPath=" + dstPath + ']');
    }

    /**
     * Verify path integrity.
     *
     * @param path Path to verify.
     * @param expIds Expected IDs for this path. Might contain additional elements, e.g. because they were created
     *     on a child path.
     * @param infos Locked infos.
     * @return verification result.
     */
    private static boolean verifyPathIntegrity(IgfsPath path, List<IgniteUuid> expIds,
        Map<IgniteUuid, IgfsFileInfo> infos) {
        List<String> pathParts = path.components();

        assert pathParts.size() < expIds.size();

        for (int i = 0; i < pathParts.size(); i++) {
            IgniteUuid parentId = expIds.get(i);

            // If parent ID is null, it doesn't exist.
            if (parentId != null) {
                IgfsFileInfo parentInfo = infos.get(parentId);

                // If parent info is null, it doesn't exist.
                if (parentInfo != null) {
                    if (parentInfo.hasChild(pathParts.get(i), expIds.get(i + 1)))
                        continue;
                }
            }

            return false;
        }

        return true;
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
        validTxState(true);

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

        // If source file does not exist or was re-created.
        if (srcEntry == null || !srcEntry.fileId().equals(fileId))
            throw fsException(new IgfsPathNotFoundException("Failed to remove file name from the source directory" +
                " (file not found) [fileId=" + fileId + ", srcFileName=" + srcFileName +
                ", srcParentId=" + srcParentId + ", srcEntry=" + srcEntry + ']'));

        // If stored file already exist.
        if (destInfo.hasChild(destFileName))
            throw fsException(new IgfsPathAlreadyExistsException("Failed to add file name into the destination " +
                " directory (file already exists) [fileId=" + fileId + ", destFileName=" + destFileName +
                ", destParentId=" + destParentId + ']'));

        transferEntry(srcEntry, srcParentId, srcFileName, destParentId, destFileName);
    }

    /**
     * Deletes (moves to TRASH) all elements under the root folder.
     *
     * @return The new Id if the artificially created folder containing all former root
     * elements moved to TRASH folder.
     * @throws IgniteCheckedException On error.
     */
    IgniteUuid format() throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                IgniteUuid trashId = IgfsUtils.randomTrashId();

                final IgniteInternalTx tx = startTx();

                try {
                    // NB: We may lock root because its id is less than any other id:
                    final IgfsFileInfo rootInfo = lockIds(IgfsUtils.ROOT_ID, trashId).get(IgfsUtils.ROOT_ID);

                    assert rootInfo != null;

                    Map<String, IgfsListingEntry> rootListingMap = rootInfo.listing();

                    assert rootListingMap != null;

                    if (rootListingMap.isEmpty())
                        return null; // Root is empty, nothing to do.

                    // Construct new info and move locked entries from root to it.
                    Map<String, IgfsListingEntry> transferListing = new HashMap<>(rootListingMap);

                    IgfsFileInfo newInfo = new IgfsFileInfo(transferListing);

                    createNewEntry(newInfo, trashId, newInfo.id().toString());

                    // Remove listing entries from root.
                    // Note that root directory properties and other attributes are preserved:
                    id2InfoPrj.put(IgfsUtils.ROOT_ID, new IgfsFileInfo(null/*listing*/, rootInfo));

                    tx.commit();

                    delWorker.signal();

                    return newInfo.id();
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
            throw new IllegalStateException("Failed to perform format because Grid is stopping.");
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
    IgniteUuid softDelete(final IgfsPath path, final boolean recursive) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                final SortedSet<IgniteUuid> allIds = new TreeSet<>(PATH_ID_SORTING_COMPARATOR);

                List<IgniteUuid> pathIdList = fileIds(path);

                assert pathIdList.size() > 1;

                final IgniteUuid victimId = pathIdList.get(pathIdList.size() - 1);

                assert !IgfsUtils.isRootOrTrashId(victimId) : "Cannot delete root or trash directories.";

                allIds.addAll(pathIdList);

                if (allIds.remove(null))
                    return null; // A fragment of the path no longer exists.

                IgniteUuid trashId = IgfsUtils.randomTrashId();

                boolean added = allIds.add(trashId);
                assert added;

                final IgniteInternalTx tx = startTx();

                try {
                    final Map<IgniteUuid, IgfsFileInfo> infoMap = lockIds(allIds);

                    // Directory starure was changed concurrently, so the original path no longer exists:
                    if (!verifyPathIntegrity(path, pathIdList, infoMap))
                        return null;

                    final IgfsFileInfo victimInfo = infoMap.get(victimId);

                    if (!recursive && victimInfo.hasChildren())
                        // Throw exception if not empty and not recursive.
                        throw new IgfsDirectoryNotEmptyException("Failed to remove directory (directory is not " +
                            "empty and recursive flag is not set).");

                    IgfsFileInfo destInfo = infoMap.get(trashId);

                    assert destInfo != null;

                    final String srcFileName = path.name();

                    final String destFileName = victimId.toString();

                    assert !destInfo.hasChild(destFileName) : "Failed to add file name into the " +
                        "destination directory (file already exists) [destName=" + destFileName + ']';

                    IgfsFileInfo srcParentInfo = infoMap.get(pathIdList.get(pathIdList.size() - 2));

                    assert srcParentInfo != null;

                    IgniteUuid srcParentId = srcParentInfo.id();
                    assert srcParentId.equals(pathIdList.get(pathIdList.size() - 2));

                    IgfsListingEntry srcEntry = srcParentInfo.listing().get(srcFileName);

                    assert srcEntry != null : "Deletion victim not found in parent listing [path=" + path +
                        ", name=" + srcFileName + ", listing=" + srcParentInfo.listing() + ']';

                    assert victimId.equals(srcEntry.fileId());

                    transferEntry(srcEntry, srcParentId, srcFileName, trashId, destFileName);

                    if (victimInfo.isFile())
                        // Update a file info of the removed file with a file path,
                        // which will be used by delete worker for event notifications.
                        invokeUpdatePath(victimId, path);

                    tx.commit();

                    delWorker.signal();

                    return victimId;
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
            throw new IllegalStateException("Failed to perform soft delete because Grid is " +
                "stopping [path=" + path + ']');
    }

    /**
     * Move path to the trash directory in existing transaction.
     *
     * @param parentId Parent ID.
     * @param name Path name.
     * @param id Path ID.
     * @param trashId Trash ID.
     * @return ID of an entry located directly under the trash directory.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private IgniteUuid softDeleteNonTx(@Nullable IgniteUuid parentId, @Nullable String name, IgniteUuid id,
        IgniteUuid trashId)
        throws IgniteCheckedException {
        validTxState(true);

        IgniteUuid resId;

        if (parentId == null) {
            // Handle special case when we deleting root directory.
            assert IgfsUtils.ROOT_ID.equals(id);

            IgfsFileInfo rootInfo = getInfo(IgfsUtils.ROOT_ID);

            if (rootInfo == null)
                return null; // Root was never created.

            // Ensure trash directory existence.
            createSystemEntryIfAbsent(trashId);

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

                createNewEntry(newInfo, trashId, newInfo.id().toString());

                // Remove listing entries from root.
                for (Map.Entry<String, IgfsListingEntry> entry : transferListing.entrySet())
                    id2InfoPrj.invoke(IgfsUtils.ROOT_ID, new ListingRemoveProcessor(entry.getKey(), entry.getValue().fileId()));

                resId = newInfo.id();
            }
            else
                resId = null;
        }
        else {
            // Ensure trash directory existence.
            createSystemEntryIfAbsent(trashId);

            moveNonTx(id, name, parentId, id.toString(), trashId);

            resId = id;
        }

        return resId;
    }

    /**
     * Remove listing entries of the given parent.
     * This operation actually deletes directories from TRASH, is used solely by IgfsDeleteWorker.
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
                validTxState(false);

                IgniteInternalTx tx = startTx();

                try {
                    Collection<IgniteUuid> res = new HashSet<>();

                    // Obtain all necessary locks in one hop.
                    IgniteUuid[] allIds = new IgniteUuid[listing.size() + 1];

                    allIds[0] = parentId;

                    int i = 1;

                    for (IgfsListingEntry childEntry : listing.values())
                        allIds[i++] = childEntry.fileId();

                    Map<IgniteUuid, IgfsFileInfo> locks = lockIds(allIds);

                    IgfsFileInfo parentInfo = locks.get(parentId);

                    // Ensure parent is still in place.
                    if (parentInfo != null) {
                        Map<String, IgfsListingEntry> parentListing = parentInfo.listing();

                        Map<String, IgfsListingEntry> newListing = new HashMap<>(parentListing.size(), 1.0f);

                        newListing.putAll(parentListing);

                        // Remove child entries if possible.
                        for (Map.Entry<String, IgfsListingEntry> entry : listing.entrySet()) {
                            String childName = entry.getKey();
                            IgniteUuid childId = entry.getValue().fileId();

                            IgfsFileInfo entryInfo = locks.get(childId);

                            if (entryInfo != null) {
                                // File must be locked for deletion:
                                assert entryInfo.isDirectory() || IgfsUtils.DELETE_LOCK_ID.equals(entryInfo.lockId());

                                // Delete only files or empty folders.
                                if (!entryInfo.hasChildren()) {
                                    id2InfoPrj.remove(childId);

                                    newListing.remove(childName);

                                    res.add(childId);
                                }
                            }
                            else {
                                // Entry was deleted concurrently.
                                newListing.remove(childName);

                                res.add(childId);
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
     * Used solely by IgfsDeleteWorker.
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
                validTxState(false);

                IgniteInternalTx tx = startTx();

                try {
                    boolean res = false;

                    Map<IgniteUuid, IgfsFileInfo> infos = lockIds(parentId, id);

                    IgfsFileInfo victim = infos.get(id);

                    if (victim == null)
                        return res;

                    assert victim.isDirectory() || IgfsUtils.DELETE_LOCK_ID.equals(victim.lockId()) :
                        " isDir: " + victim.isDirectory() + ", lockId: " + victim.lockId();

                    // Proceed only in case both parent and child exist.
                    if (infos.containsKey(parentId) && infos.containsKey(id)) {
                        IgfsFileInfo parentInfo = infos.get(parentId);

                        assert parentInfo != null;

                        IgfsListingEntry childEntry = parentInfo.listing().get(name);

                        if (childEntry != null)
                            id2InfoPrj.invoke(parentId, new ListingRemoveProcessor(name, id));

                        id2InfoPrj.remove(id);

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
                Collection<IgniteUuid> ids = new HashSet<>();

                for (int i = 0; i < IgfsUtils.TRASH_CONCURRENCY; i++) {
                    IgniteUuid trashId = IgfsUtils.trashId(i);

                    IgfsFileInfo trashInfo = getInfo(trashId);

                    if (trashInfo != null && trashInfo.hasChildren()) {
                        for (IgfsListingEntry entry : trashInfo.listing().values())
                            ids.add(entry.fileId());
                    }
                }

                return ids;
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
     * @param fileId File ID to update information for.
     * @param props Properties to set for the file.
     * @return Updated file info or {@code null} if such file ID not found.
     * @throws IgniteCheckedException If operation failed.
     */
    @Nullable private IgfsFileInfo updatePropertiesNonTx(final IgniteUuid fileId, Map<String, String> props)
        throws IgniteCheckedException {
        assert fileId != null;
        assert !F.isEmpty(props) : "Expects not-empty file's properties";

        validTxState(true);

        if (log.isDebugEnabled())
            log.debug("Update file properties [fileId=" + fileId + ", props=" + props + ']');

        try {
            final IgfsFileInfo oldInfo = info(fileId);

            if (oldInfo == null)
                return null;

            return invokeAndGet(fileId, new UpdatePropertiesProcessor(props));
        }
        catch (GridClosureException e) {
            throw U.cast(e);
        }
    }

    /**
     * Update file info (file properties) in cache.
     *
     * @param fileId File ID to update information for.
     * @param props Properties to set for the file.
     * @return Updated file info or {@code null} if such file ID not found.
     * @throws IgniteCheckedException If operation failed.
     */
    @Nullable public IgfsFileInfo updateProperties(IgniteUuid fileId, Map<String, String> props)
        throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                IgniteInternalTx tx = startTx();

                try {
                    IgfsFileInfo info = updatePropertiesNonTx(fileId, props);

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
            throw new IllegalStateException("Failed to update properties because Grid is stopping [fileId=" + fileId +
                ", props=" + props + ']');
    }

    /**
     * Reserve space for file.
     *
     * @param path File path.
     * @param fileId File ID.
     * @param space Space.
     * @param affRange Affinity range.
     * @return New file info.
     */
    public IgfsFileInfo reserveSpace(IgfsPath path, IgniteUuid fileId, long space, IgfsFileAffinityRange affRange)
        throws IgniteCheckedException {
        validTxState(false);

        if (busyLock.enterBusy()) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Reserve file space [path=" + path + ", id=" + fileId + ']');

                IgniteInternalTx tx = startTx();

                try {
                    // Lock file ID for this transaction.
                    IgfsFileInfo oldInfo = info(fileId);

                    if (oldInfo == null)
                        throw fsException("File has been deleted concurrently [path=" + path + ", id=" + fileId + ']');

                    IgfsFileInfo newInfo = invokeAndGet(fileId, new FileReserveSpaceProcessor(space, affRange));

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
            throw new IllegalStateException("Failed to reseve file space because Grid is stopping [path=" + path +
                ", id=" + fileId + ']');
    }

    /**
     * Update file info in cache.
     *
     * @param fileId File ID to update information for.
     * @param proc Entry processor to invoke.
     * @return Updated file info or {@code null} if such file ID not found.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgfsFileInfo updateInfo(IgniteUuid fileId,
        EntryProcessor<IgniteUuid, IgfsFileInfo, IgfsFileInfo> proc) throws IgniteCheckedException {
        validTxState(false);
        assert fileId != null;
        assert proc != null;

        if (busyLock.enterBusy()) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Update file info [fileId=" + fileId + ", proc=" + proc + ']');

                IgniteInternalTx tx = startTx();

                try {
                    // Lock file ID for this transaction.
                    IgfsFileInfo oldInfo = info(fileId);

                    if (oldInfo == null)
                        return null; // File not found.

                    IgfsFileInfo newInfo = invokeAndGet(fileId, proc);

                    if (newInfo == null)
                        throw fsException("Failed to update file info with null value" +
                            " [oldInfo=" + oldInfo + ", newInfo=" + newInfo + ", proc=" + proc + ']');

                    if (!oldInfo.id().equals(newInfo.id()))
                        throw fsException("Failed to update file info (file IDs differ)" +
                            " [oldInfo=" + oldInfo + ", newInfo=" + newInfo + ", proc=" + proc + ']');

                    if (oldInfo.isDirectory() != newInfo.isDirectory())
                        throw fsException("Failed to update file info (file types differ)" +
                            " [oldInfo=" + oldInfo + ", newInfo=" + newInfo + ", proc=" + proc + ']');

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
            throw new IllegalStateException("Failed to update file system entry info because Grid is stopping: " +
                fileId);
    }

    /**
     * Mkdirs implementation.
     *
     * @param path The path to create.
     * @param props The properties to use for created directories.
     * @return True iff a directory was created during the operation.
     * @throws IgniteCheckedException If a non-directory file exists on the requested path, and in case of other errors.
     */
    boolean mkdirs(final IgfsPath path, final Map<String, String> props) throws IgniteCheckedException {
        assert props != null;
        validTxState(false);

        DirectoryChainBuilder b = null;

        while (true) {
            if (busyLock.enterBusy()) {
                try {
                    b = new DirectoryChainBuilder(path, props);

                    // Start TX.
                    IgniteInternalTx tx = startTx();

                    try {
                        final Map<IgniteUuid, IgfsFileInfo> lockedInfos = lockIds(b.idSet);

                        // If the path was changed, we close the current Tx and repeat the procedure again
                        // starting from taking the path ids.
                        if (verifyPathIntegrity(b.existingPath, b.idList, lockedInfos)) {
                            // Locked path okay, trying to proceed with the remainder creation.
                            IgfsFileInfo lowermostExistingInfo = lockedInfos.get(b.lowermostExistingId);

                            // Check only the lowermost directory in the existing directory chain
                            // because others are already checked in #verifyPathIntegrity() above.
                            if (!lowermostExistingInfo.isDirectory())
                                throw new IgfsParentNotDirectoryException("Failed to create directory (parent " +
                                    "element is not a directory)");

                            if (b.existingIdCnt == b.components.size() + 1) {
                                assert b.existingPath.equals(path);
                                assert lockedInfos.size() == b.existingIdCnt;

                                // The target directory already exists, nothing to do.
                                // (The fact that all the path consisns of directories is already checked above).
                                // Note that properties are not updated in this case.
                                return false;
                            }

                            Map<String, IgfsListingEntry> parentListing = lowermostExistingInfo.listing();

                            String shortName = b.components.get(b.existingIdCnt - 1);

                            IgfsListingEntry entry = parentListing.get(shortName);

                            if (entry == null) {
                                b.doBuild();

                                tx.commit();

                                break;
                            }
                            else {
                                // Another thread created file or directory with the same name.
                                if (!entry.isDirectory()) {
                                    // Entry exists, and it is not a directory:
                                    throw new IgfsParentNotDirectoryException("Failed to create directory (parent " +
                                        "element is not a directory)");
                                }

                                // If this is a directory, we continue the repeat loop,
                                // because we cannot lock this directory without
                                // lock ordering rule violation.
                            }
                        }
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
                throw new IllegalStateException("Failed to mkdir because Grid is stopping. [path=" + path + ']');
        }

        assert b != null;

        b.sendEvents();

        return true;
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

                IgniteInternalTx tx = startTx();

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
     * Put new entry to meta cache immediately linking it to parent.
     *
     * @param info Info to put.
     * @param parentId Parent ID.
     * @param name Name in parent.
     * @throws IgniteCheckedException If failed.
     */
    private void createNewEntry(IgfsFileInfo info, IgniteUuid parentId, String name) throws IgniteCheckedException {
        validTxState(true);

        if (!id2InfoPrj.putIfAbsent(info.id(), info))
            throw fsException("Failed to create new metadata entry due to ID conflict: " + info.id());

        if (parentId != null)
            id2InfoPrj.invoke(parentId, new ListingAddProcessor(name, new IgfsListingEntry(info)));
    }

    /**
     * Transfer entry from one directory to another.
     *
     * @param entry Entry to be transfered.
     * @param srcId Source ID.
     * @param srcName Source name.
     * @param destId Destination ID.
     * @param destName Destination name.
     * @throws IgniteCheckedException If failed.
     */
    private void transferEntry(IgfsListingEntry entry, IgniteUuid srcId, String srcName,
        IgniteUuid destId, String destName) throws IgniteCheckedException {
        validTxState(true);

        id2InfoPrj.invoke(srcId, new ListingRemoveProcessor(srcName, entry.fileId()));
        id2InfoPrj.invoke(destId, new ListingAddProcessor(destName, entry));
    }

    /**
     * Invoke lock processor.
     *
     * @param id File ID.
     * @param delete Whether lock is taken for delete.
     * @return Resulting file info.
     * @throws IgniteCheckedException If failed.
     */
    private IgfsFileInfo invokeLock(IgniteUuid id, boolean delete) throws IgniteCheckedException {
        return invokeAndGet(id, new FileLockProcessor(createFileLockId(delete)));
    }

    /**
     * Invoke path update processor.
     *
     * @param id File ID.
     * @param path Path to be updated.
     * @throws IgniteCheckedException If failed.
     */
    private void invokeUpdatePath(IgniteUuid id, IgfsPath path) throws IgniteCheckedException {
        validTxState(true);

        id2InfoPrj.invoke(id, new UpdatePathProcessor(path));
    }

    /**
     * Invoke some processor and return new value.
     *
     * @param id ID.
     * @param proc Processor.
     * @return New file info.
     * @throws IgniteCheckedException If failed.
     */
    private IgfsFileInfo invokeAndGet(IgniteUuid id, EntryProcessor<IgniteUuid, IgfsFileInfo, IgfsFileInfo> proc)
        throws IgniteCheckedException {
        validTxState(true);

        return id2InfoPrj.invoke(id, proc).get();
    }

    /**
     * Get info.
     *
     * @param id ID.
     * @return Info.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private IgfsFileInfo getInfo(IgniteUuid id) throws IgniteCheckedException {
        return id2InfoPrj.get(id);
    }

    /**
     * Get several infos.
     *
     * @param ids IDs.
     * @return Infos map.
     * @throws IgniteCheckedException If failed.
     */
    private Map<IgniteUuid, IgfsFileInfo> getInfos(Collection<IgniteUuid> ids) throws IgniteCheckedException {
        return id2InfoPrj.getAll(ids);
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
                            validTxState(true);

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
                                createFileLockId(false), igfsCtx.igfs().evictExclude(path, false), status.properties(),
                                status.accessTime(), status.modificationTime());

                            // Add new file info to the listing optionally removing the previous one.
                            IgniteUuid oldId = putIfAbsentNonTx(parentInfo.id(), path.name(), newInfo);

                            if (oldId != null) {
                                IgfsFileInfo oldInfo = info(oldId);

                                assert oldInfo != null; // Otherwise cache is in inconsistent state.

                                // The contact is that we cannot overwrite a file locked for writing:
                                if (oldInfo.lockId() != null)
                                    throw fsException("Failed to overwrite file (file is opened for writing) [path=" +
                                        path + ", fileId=" + oldId + ", lockId=" + oldInfo.lockId() + ']');

                                id2InfoPrj.remove(oldId); // Remove the old one.
                                id2InfoPrj.invoke(parentInfo.id(),
                                    new ListingRemoveProcessor(path.name(), parentInfo.listing().get(path.name()).fileId()));

                                createNewEntry(newInfo, parentInfo.id(), path.name()); // Put new one.

                                IgniteInternalFuture<?> delFut = igfsCtx.data().delete(oldInfo);
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
                            validTxState(true);

                            final IgfsFileInfo info = infos.get(path);

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

                            if (info.lockId() != null) {
                                throw fsException("Failed to open file (file is opened for writing) [path=" +
                                    path + ", fileId=" + info.id() + ", lockId=" + info.lockId() + ']');
                            }

                            // Set lock and return.
                            IgfsFileInfo lockedInfo = invokeLock(info.id(), false);

                            return new IgfsSecondaryOutputStreamDescriptor(infos.get(path.parent()).id(),
                                lockedInfo, out);
                        }

                        @Override public IgfsSecondaryOutputStreamDescriptor onFailure(@Nullable Exception err)
                            throws IgniteCheckedException {
                            U.closeQuiet(out);

                            U.error(log, "File append in DUAL mode failed [path=" + path + ", bufferSize=" + bufSize +
                                ']', err);

                            throw new IgniteCheckedException("Failed to append to the file due to secondary file " +
                                "system exception: " + path, err);
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
                            throw new IgniteCheckedException("Failed to synchronize path due to secondary file " +
                                "system exception: " + path, err);
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

                        throw new IgniteCheckedException("Failed to create the path due to secondary file system " +
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

                final IgniteUuid trashId = IgfsUtils.randomTrashId();

                SynchronizationTask<Boolean> task = new SynchronizationTask<Boolean>() {
                    @Override public Boolean onSuccess(Map<IgfsPath, IgfsFileInfo> infos) throws Exception {
                        IgfsFileInfo info = infos.get(path);

                        if (info == null)
                            return false; // File doesn't exist in the secondary file system.

                        if (!fs.delete(path, recursive))
                            return false; // Delete failed remotely.

                        if (path.parent() != null) {
                            assert infos.containsKey(path.parent());

                            softDeleteNonTx(infos.get(path.parent()).id(), path.name(), info.id(), trashId);
                        }
                        else {
                            assert IgfsUtils.ROOT_ID.equals(info.id());

                            softDeleteNonTx(null, path.name(), info.id(), trashId);
                        }

                        // Update the deleted file info with path information for delete worker.
                        invokeUpdatePath(info.id(), path);

                        return true; // No additional handling is required.
                    }

                    @Override public Boolean onFailure(@Nullable Exception err) throws IgniteCheckedException {
                        U.error(log, "Path delete in DUAL mode failed [path=" + path + ", recursive=" + recursive + ']',
                            err);

                        throw new IgniteCheckedException("Failed to delete the path due to secondary file system " +
                            "exception: ", err);
                    }
                };

                Boolean res = synchronizeAndExecute(task, fs, false, Collections.singleton(trashId), path);

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
    public IgfsFileInfo updateDual(final IgfsSecondaryFileSystem fs, final IgfsPath path,
        final Map<String, String> props) throws IgniteCheckedException {
        assert fs != null;
        assert path != null;
        assert props != null && !props.isEmpty();

        if (busyLock.enterBusy()) {
            try {
                SynchronizationTask<IgfsFileInfo> task = new SynchronizationTask<IgfsFileInfo>() {
                    @Override public IgfsFileInfo onSuccess(Map<IgfsPath, IgfsFileInfo> infos) throws Exception {
                        if (infos.get(path) == null)
                            return null;

                        fs.update(path, props);

                        return updatePropertiesNonTx(infos.get(path).id(), props);
                    }

                    @Override public IgfsFileInfo onFailure(@Nullable Exception err) throws IgniteCheckedException {
                        U.error(log, "Path update in DUAL mode failed [path=" + path + ", properties=" + props + ']',
                            err);

                        throw new IgniteCheckedException("Failed to update the path due to secondary file system " +
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
                        throw new IgniteCheckedException("Failed to create path the locally because secondary file " +
                            "system directory structure was modified concurrently and the path is not a directory as " +
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
                IgfsFileInfo curInfo = status.isDirectory() ?
                    new IgfsFileInfo(true, status.properties(), status.accessTime(), status.modificationTime()) :
                    new IgfsFileInfo(igfsCtx.configuration().getBlockSize(), status.length(),
                        igfsCtx.igfs().evictExclude(curPath, false), status.properties(),
                        status.accessTime(), status.modificationTime());

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
            IgniteInternalTx tx = startTx();

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
    private <K, V> boolean putx(IgniteInternalCache<K, V> cache, K key, IgniteClosure<V, V> c)
        throws IgniteCheckedException {
        validTxState(true);

        V oldVal = cache.get(key);
        V newVal = c.apply(oldVal);

        return newVal == null ? cache.remove(key) : cache.put(key, newVal);
    }

    /**
     * Check transaction is (not) started.
     *
     * @param inTx Expected transaction state.
     */
    private void validTxState(boolean inTx) {
        assert (inTx && id2InfoPrj.tx() != null) || (!inTx && id2InfoPrj.tx() == null) :
            "Invalid TX state [expected=" + inTx + ", actual=" + (id2InfoPrj.tx() != null) + ']';
    }

    /**
     * Start transaction on meta cache.
     *
     * @return Transaction.
     */
    private IgniteInternalTx startTx() {
        return metaCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);
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
                validTxState(false);

                // Start pessimistic transaction.
                IgniteInternalTx tx = startTx();

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

                    // Validate listing.
                    if (!parentInfo.hasChild(fileName, fileId))
                        throw fsException(new IgfsConcurrentModificationException("Failed to update times " +
                                "(file concurrently modified): " + fileName));

                    assert parentInfo.isDirectory();

                    id2InfoPrj.invoke(fileId, new UpdateTimesProcessor(
                        accessTime == -1 ? fileInfo.accessTime() : accessTime,
                        modificationTime == -1 ? fileInfo.modificationTime() : modificationTime)
                    );

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
     * Remove entry from directory listing.
     */
    @GridInternal
    private static final class ListingRemoveProcessor implements EntryProcessor<IgniteUuid, IgfsFileInfo, Void>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** File name. */
        private String fileName;

        /** Expected ID. */
        private IgniteUuid fileId;

        /**
         * Default constructor.
         */
        public ListingRemoveProcessor() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param fileName File name.
         * @param fileId File ID.
         */
        public ListingRemoveProcessor(String fileName, IgniteUuid fileId) {
            this.fileName = fileName;
            this.fileId = fileId;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<IgniteUuid, IgfsFileInfo> e, Object... args)
            throws EntryProcessorException {
            IgfsFileInfo fileInfo = e.getValue();

            assert fileInfo != null;
            assert fileInfo.isDirectory();

            Map<String, IgfsListingEntry> listing = new HashMap<>(fileInfo.listing());

            listing.putAll(fileInfo.listing());

            IgfsListingEntry oldEntry = listing.get(fileName);

            if (oldEntry == null || !oldEntry.fileId().equals(fileId))
                throw new IgniteException("Directory listing doesn't contain expected file" +
                    " [listing=" + listing + ", fileName=" + fileName + "]");

            // Modify listing in-place.
            listing.remove(fileName);

            e.setValue(new IgfsFileInfo(listing, fileInfo));

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, fileName);
            U.writeGridUuid(out, fileId);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            fileName = U.readString(in);
            fileId = U.readGridUuid(in);
        }
    }

    /**
     * Update directory listing closure.
     */
    @GridInternal
    private static final class ListingAddProcessor implements EntryProcessor<IgniteUuid, IgfsFileInfo, Void>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** File name to add into parent listing. */
        private String fileName;

        /** File ID.*/
        private IgfsListingEntry entry;

        /**
         * Constructs update directory listing closure.
         *
         * @param fileName File name to add into parent listing.
         * @param entry Listing entry to add or remove.
         */
        private ListingAddProcessor(String fileName, IgfsListingEntry entry) {
            assert fileName != null;
            assert entry != null;

            this.fileName = fileName;
            this.entry = entry;
        }

        /**
         * Empty constructor required for {@link Externalizable}.
         *
         */
        public ListingAddProcessor() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<IgniteUuid, IgfsFileInfo> e, Object... args) {
            IgfsFileInfo fileInfo = e.getValue();

            assert fileInfo != null : "File info not found for the child: " + entry.fileId();
            assert fileInfo.isDirectory();

            Map<String, IgfsListingEntry> listing = new HashMap<>(fileInfo.listing());

            // Modify listing in-place.
            IgfsListingEntry oldEntry = listing.put(fileName, entry);

            if (oldEntry != null && !oldEntry.fileId().equals(entry.fileId()))
                throw new IgniteException("Directory listing contains unexpected file" +
                    " [listing=" + listing + ", fileName=" + fileName + ", entry=" + entry +
                    ", oldEntry=" + oldEntry + ']');

            e.setValue(new IgfsFileInfo(listing, fileInfo));

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, fileName);
            out.writeObject(entry);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            fileName = U.readString(in);
            entry = (IgfsListingEntry)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ListingAddProcessor.class, this);
        }
    }

    /**
     * Update path closure.
     */
    @GridInternal
    private static final class UpdatePathProcessor implements EntryProcessor<IgniteUuid, IgfsFileInfo, Void>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** New path. */
        private IgfsPath path;

        /**
         * @param path Path.
         */
        private UpdatePathProcessor(IgfsPath path) {
            this.path = path;
        }

        /**
         * Default constructor (required by Externalizable).
         */
        public UpdatePathProcessor() {
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
            return S.toString(UpdatePathProcessor.class, this);
        }
    }

    /**
     * Create a new file.
     *
     * @param path Path.
     * @param bufSize Buffer size.
     * @param overwrite Overwrite flag.
     * @param affKey Affinity key.
     * @param replication Replication factor.
     * @param props Properties.
     * @param simpleCreate Whether new file should be created in secondary FS using create(Path, boolean) method.
     * @return Tuple containing the created file info and its parent id.
     */
    IgniteBiTuple<IgfsFileInfo, IgniteUuid> create(
        final IgfsPath path,
        final boolean append,
        final boolean overwrite,
        Map<String, String> dirProps,
        final int blockSize,
        final @Nullable IgniteUuid affKey,
        final boolean evictExclude,
        @Nullable Map<String, String> fileProps) throws IgniteCheckedException {
        validTxState(false);

        assert path != null;

        final String name = path.name();

        DirectoryChainBuilder b = null;

        IgniteUuid trashId = IgfsUtils.randomTrashId();

        while (true) {
            if (busyLock.enterBusy()) {
                try {
                    b = new DirectoryChainBuilder(path, dirProps, fileProps, blockSize, affKey, evictExclude);

                    // Start Tx:
                    IgniteInternalTx tx = startTx();

                    try {
                        if (overwrite)
                            // Lock also the TRASH directory because in case of overwrite we
                            // may need to delete the old file:
                            b.idSet.add(trashId);

                        final Map<IgniteUuid, IgfsFileInfo> lockedInfos = lockIds(b.idSet);

                        assert !overwrite || lockedInfos.get(trashId) != null; // TRASH must exist at this point.

                        // If the path was changed, we close the current Tx and repeat the procedure again
                        // starting from taking the path ids.
                        if (verifyPathIntegrity(b.existingPath, b.idList, lockedInfos)) {
                            // Locked path okay, trying to proceed with the remainder creation.
                            final IgfsFileInfo lowermostExistingInfo = lockedInfos.get(b.lowermostExistingId);

                            if (b.existingIdCnt == b.components.size() + 1) {
                                // Full requestd path exists.

                                assert b.existingPath.equals(path);
                                assert lockedInfos.size() ==
                                        (overwrite ? b.existingIdCnt + 1/*TRASH*/ : b.existingIdCnt);

                                if (lowermostExistingInfo.isDirectory()) {
                                    throw new IgfsPathAlreadyExistsException("Failed to "
                                            + (append ? "open" : "create") + " file (path points to an " +
                                        "existing directory): " + path);
                                }
                                else {
                                    // This is a file.
                                    assert lowermostExistingInfo.isFile();

                                    final IgniteUuid parentId = b.idList.get(b.idList.size() - 2);

                                    final IgniteUuid lockId = lowermostExistingInfo.lockId();

                                    if (append) {
                                        if (lockId != null)
                                            throw fsException("Failed to open file (file is opened for writing) "
                                                + "[fileName=" + name + ", fileId=" + lowermostExistingInfo.id()
                                                + ", lockId=" + lockId + ']');

                                        IgfsFileInfo lockedInfo = invokeLock(lowermostExistingInfo.id(), false);

                                        IgniteBiTuple<IgfsFileInfo, IgniteUuid> t2 = new T2<>(lockedInfo, parentId);

                                        tx.commit();

                                        IgfsUtils.sendEvents(igfsCtx.kernalContext(), path,
                                                EventType.EVT_IGFS_FILE_OPENED_WRITE);

                                        return t2;
                                    }
                                    else if (overwrite) {
                                        // Delete existing file, but fail if it is locked:
                                        if (lockId != null)
                                            throw fsException("Failed to overwrite file (file is opened for writing) " +
                                                    "[fileName=" + name + ", fileId=" + lowermostExistingInfo.id()
                                                    + ", lockId=" + lockId + ']');

                                        final IgfsListingEntry deletedEntry = lockedInfos.get(parentId).listing()
                                                .get(name);

                                        assert deletedEntry != null;

                                        transferEntry(deletedEntry, parentId, name, trashId,
                                            lowermostExistingInfo.id().toString());

                                        // Update a file info of the removed file with a file path,
                                        // which will be used by delete worker for event notifications.
                                        invokeUpdatePath(lowermostExistingInfo.id(), path);

                                        // Make a new locked info:
                                        long t = System.currentTimeMillis();

                                        final IgfsFileInfo newFileInfo = new IgfsFileInfo(cfg.getBlockSize(), 0L,
                                            affKey, createFileLockId(false), evictExclude, fileProps, t, t);

                                        assert newFileInfo.lockId() != null; // locked info should be created.

                                        createNewEntry(newFileInfo, parentId, name);

                                        IgniteBiTuple<IgfsFileInfo, IgniteUuid> t2 = new T2<>(newFileInfo, parentId);

                                        tx.commit();

                                        delWorker.signal();

                                        IgfsUtils.sendEvents(igfsCtx.kernalContext(), path, EVT_IGFS_FILE_OPENED_WRITE);

                                        return t2;
                                    }
                                    else {
                                        throw new IgfsPathAlreadyExistsException("Failed to create file (file " +
                                            "already exists and overwrite flag is false): " + path);
                                    }
                                }
                            }

                            // The full requested path does not exist.

                            // Check only the lowermost directory in the existing directory chain
                            // because others are already checked in #verifyPathIntegrity() above.
                            if (!lowermostExistingInfo.isDirectory())
                                throw new IgfsParentNotDirectoryException("Failed to " + (append ? "open" : "create" )
                                    + " file (parent element is not a directory)");

                            final String uppermostFileToBeCreatedName = b.components.get(b.existingIdCnt - 1);

                            if (!lowermostExistingInfo.hasChild(uppermostFileToBeCreatedName)) {
                                b.doBuild();

                                assert b.leafInfo != null;
                                assert b.leafParentId != null;

                                IgniteBiTuple<IgfsFileInfo, IgniteUuid> t2 = new T2<>(b.leafInfo, b.leafParentId);

                                tx.commit();

                                b.sendEvents();

                                return t2;
                            }

                            // Another thread concurrently created file or directory in the path with
                            // the name we need.
                        }
                    }
                    finally {
                        tx.close();
                    }
                }
                finally {
                    busyLock.leaveBusy();
                }
            } else
                throw new IllegalStateException("Failed to mkdir because Grid is stopping. [path=" + path + ']');
        }
    }

    /** File chain builder. */
    private class DirectoryChainBuilder {
        /** The requested path to be created. */
        private final IgfsPath path;

        /** Full path components. */
        private final List<String> components;

        /** The list of ids. */
        private final List<IgniteUuid> idList;

        /** The set of ids. */
        private final SortedSet<IgniteUuid> idSet = new TreeSet<IgniteUuid>(PATH_ID_SORTING_COMPARATOR);

        /** The middle node properties. */
        private final Map<String, String> props;

        /** The leaf node properties. */
        private final Map<String, String> leafProps;

        /** The lowermost exsiting path id. */
        private final IgniteUuid lowermostExistingId;

        /** The existing path. */
        private final IgfsPath existingPath;

        /** The created leaf info. */
        private IgfsFileInfo leafInfo;

        /** The leaf parent id. */
        private IgniteUuid leafParentId;

        /** The number of existing ids. */
        private final int existingIdCnt;

        /** Whether laef is directory. */
        private final boolean leafDir;

        /** Block size. */
        private final int blockSize;

        /** Affinity key. */
        private final IgniteUuid affKey;

        /** Evict exclude flag. */
        private final boolean evictExclude;

        /**
         * Constructor for directories.
         *
         * @param path Path.
         * @param props Properties.
         * @throws IgniteCheckedException If failed.
         */
        protected DirectoryChainBuilder(IgfsPath path, Map<String, String> props) throws IgniteCheckedException {
            this(path, props, props, true, 0, null, false);
        }

        /**
         * Constructor for files.
         *
         * @param path Path.
         * @param dirProps Directory properties.
         * @param fileProps File properties.
         * @param blockSize Block size.
         * @param affKey Affinity key (optional).
         * @param evictExclude Evict exclude flag.
         * @throws IgniteCheckedException If failed.
         */
        protected DirectoryChainBuilder(IgfsPath path, Map<String, String> dirProps, Map<String, String> fileProps,
            int blockSize, @Nullable IgniteUuid affKey, boolean evictExclude)
            throws IgniteCheckedException {
            this(path, dirProps, fileProps, false, blockSize, affKey, evictExclude);
        }

        /**
         * Constructor.
         *
         * @param path Path.
         * @param props Middle properties.
         * @param leafProps Leaf properties.
         * @param leafDir Whether leaf is directory or file.
         * @param blockSize Block size.
         * @param affKey Affinity key (optional).
         * @param evictExclude Evict exclude flag.
         * @throws IgniteCheckedException If failed.
         */
        private DirectoryChainBuilder(IgfsPath path, Map<String,String> props, Map<String,String> leafProps,
            boolean leafDir, int blockSize, @Nullable IgniteUuid affKey, boolean evictExclude)
            throws IgniteCheckedException {
            this.path = path;
            this.components = path.components();
            this.idList = fileIds(path);
            this.props = props;
            this.leafProps = leafProps;
            this.leafDir = leafDir;
            this.blockSize = blockSize;
            this.affKey = affKey;
            this.evictExclude = evictExclude;

            // Store all the non-null ids in the set & construct existing path in one loop:
            IgfsPath existingPath = path.root();

            assert idList.size() == components.size() + 1;

            // Find the lowermost existing id:
            IgniteUuid lowermostExistingId = null;

            int idIdx = 0;

            for (IgniteUuid id : idList) {
                if (id == null)
                    break;

                lowermostExistingId = id;

                boolean added = idSet.add(id);

                assert added : "Not added id = " + id;

                if (idIdx >= 1) // skip root.
                    existingPath = new IgfsPath(existingPath, components.get(idIdx - 1));

                idIdx++;
            }

            assert idSet.contains(IgfsUtils.ROOT_ID);

            this.lowermostExistingId = lowermostExistingId;

            this.existingPath = existingPath;

            this.existingIdCnt = idSet.size();
        }

        /**
         * Does the main portion of job building the renmaining path.
         */
        public final void doBuild() throws IgniteCheckedException {
            // Fix current time. It will be used in all created entities.
            long createTime = System.currentTimeMillis();

            IgfsListingEntry childInfo = null;
            String childName = null;

            IgniteUuid parentId = null;

            // This loop creates the missing directory chain from the bottom to the top:
            for (int i = components.size() - 1; i >= existingIdCnt - 1; i--) {
                IgniteUuid childId = IgniteUuid.randomUuid();
                boolean childDir;

                if (childName == null) {
                    assert childInfo == null;

                    if (leafDir) {
                        childDir = true;

                        leafInfo = invokeAndGet(childId, new DirectoryCreateProcessor(createTime, leafProps));
                    }
                    else {
                        childDir = false;

                        leafInfo = invokeAndGet(childId, new FileCreateProcessor(createTime, leafProps, blockSize,
                            affKey, createFileLockId(false), evictExclude));
                    }
                }
                else {
                    assert childInfo != null;

                    childDir = true;

                    id2InfoPrj.invoke(childId, new DirectoryCreateProcessor(createTime, props, childName, childInfo));

                    if (parentId == null)
                        parentId = childId;
                }

                childInfo = new IgfsListingEntry(childId, childDir);

                childName = components.get(i);
            }

            if (parentId == null)
                parentId = lowermostExistingId;

            leafParentId = parentId;

            // Now link the newly created directory chain to the lowermost existing parent:
            id2InfoPrj.invoke(lowermostExistingId, new ListingAddProcessor(childName, childInfo));
        }

        /**
         * Sends events.
         */
        public final void sendEvents() {
            if (evts.isRecordable(EVT_IGFS_DIR_CREATED)) {
                IgfsPath createdPath = existingPath;

                for (int i = existingPath.components().size(); i < components.size() - 1; i++) {
                    createdPath = new IgfsPath(createdPath, components.get(i));

                    IgfsUtils.sendEvents(igfsCtx.kernalContext(), createdPath, EVT_IGFS_DIR_CREATED);
                }
            }

            if (leafDir)
                IgfsUtils.sendEvents(igfsCtx.kernalContext(), path, EVT_IGFS_DIR_CREATED);
            else {
                IgfsUtils.sendEvents(igfsCtx.kernalContext(), path, EVT_IGFS_FILE_CREATED);
                IgfsUtils.sendEvents(igfsCtx.kernalContext(), path, EVT_IGFS_FILE_OPENED_WRITE);
            }
        }
    }

    /**
     * File create processor.
     */
    private static class FileCreateProcessor implements EntryProcessor<IgniteUuid, IgfsFileInfo, IgfsFileInfo>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Create time. */
        private long createTime;

        /** Properties. */
        private Map<String, String> props;

        /** Block size. */
        private int blockSize;

        /** Affintiy key. */
        private IgniteUuid affKey;

        /** Lcok ID. */
        private IgniteUuid lockId;

        /** Evict exclude flag. */
        private boolean evictExclude;

        /**
         * Constructor.
         */
        public FileCreateProcessor() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param createTime Create time.
         * @param props Properties.
         * @param blockSize Block size.
         * @param affKey Affinity key.
         * @param lockId Lock ID.
         * @param evictExclude Evict exclude flag.
         */
        public FileCreateProcessor(long createTime, Map<String, String> props, int blockSize,
            @Nullable IgniteUuid affKey, IgniteUuid lockId, boolean evictExclude) {
            this.createTime = createTime;
            this.props = props;
            this.blockSize = blockSize;
            this.affKey = affKey;
            this.lockId = lockId;
            this.evictExclude = evictExclude;
        }

        /** {@inheritDoc} */
        @Override public IgfsFileInfo process(MutableEntry<IgniteUuid, IgfsFileInfo> entry, Object... args)
            throws EntryProcessorException {
            IgfsFileInfo info = new IgfsFileInfo(blockSize, 0L, affKey, lockId, evictExclude, props,
                createTime, createTime);

            info.id(entry.getKey());

            entry.setValue(info);

            return info;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(createTime);
            U.writeStringMap(out, props);
            out.writeInt(blockSize);
            out.writeObject(affKey);
            out.writeObject(lockId);
            out.writeBoolean(evictExclude);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            createTime = in.readLong();
            props = U.readStringMap(in);
            blockSize = in.readInt();
            affKey = (IgniteUuid)in.readObject();
            lockId = (IgniteUuid)in.readObject();
            evictExclude = in.readBoolean();
        }
    }

    /**
     * Directory create processor.
     */
    private static class DirectoryCreateProcessor implements EntryProcessor<IgniteUuid, IgfsFileInfo, IgfsFileInfo>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Create time. */
        private long createTime;

        /** Properties. */
        private Map<String, String> props;

        /** Child name (optional). */
        private String childName;

        /** Child entry (optional. */
        private IgfsListingEntry childEntry;

        /**
         * Constructor.
         */
        public DirectoryCreateProcessor() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param createTime Create time.
         * @param props Properties.
         */
        public DirectoryCreateProcessor(long createTime, Map<String, String> props) {
            this(createTime, props, null, null);
        }

        /**
         * Constructor.
         *
         * @param createTime Create time.
         * @param props Properties.
         * @param childName Child name.
         * @param childEntry Child entry.
         */
        public DirectoryCreateProcessor(long createTime, Map<String, String> props, String childName,
            IgfsListingEntry childEntry) {
            this.createTime = createTime;
            this.props = props;
            this.childName = childName;
            this.childEntry = childEntry;
        }

        /** {@inheritDoc} */
        @Override public IgfsFileInfo process(MutableEntry<IgniteUuid, IgfsFileInfo> entry, Object... args)
            throws EntryProcessorException {

            IgfsFileInfo info = new IgfsFileInfo(true, props, createTime, createTime);

            if (childName != null)
                info = new IgfsFileInfo(Collections.singletonMap(childName, childEntry), info);

            info.id(entry.getKey());

            entry.setValue(info);

            return info;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(createTime);
            U.writeStringMap(out, props);

            if (childName != null) {
                out.writeBoolean(true);

                U.writeString(out, childName);
                out.writeObject(childEntry);
            }
            else
                out.writeBoolean(false);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            createTime = in.readLong();
            props = U.readStringMap(in);

            if (in.readBoolean()) {
                childName = U.readString(in);
                childEntry = (IgfsListingEntry)in.readObject();
            }
        }
    }

    /**
     * File lock entry processor.
     */
    private static class FileLockProcessor implements EntryProcessor<IgniteUuid, IgfsFileInfo, IgfsFileInfo>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Lock Id. */
        private IgniteUuid lockId;

        /**
         * Default constructor.
         */
        public FileLockProcessor() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param lockId Lock ID.
         */
        public FileLockProcessor(IgniteUuid lockId) {
            this.lockId = lockId;
        }

        /** {@inheritDoc} */
        @Override public IgfsFileInfo process(MutableEntry<IgniteUuid, IgfsFileInfo> entry, Object... args)
            throws EntryProcessorException {
            IgfsFileInfo oldInfo = entry.getValue();

            IgfsFileInfo newInfo =  new IgfsFileInfo(oldInfo, lockId, oldInfo.modificationTime());

            entry.setValue(newInfo);

            return newInfo;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeGridUuid(out, lockId);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            lockId = U.readGridUuid(in);
        }
    }

    /**
     * File unlock entry processor.
     */
    private static class FileUnlockProcessor implements EntryProcessor<IgniteUuid, IgfsFileInfo, Void>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Modification time. */
        private long modificationTime;

        /**
         * Default constructor.
         */
        public FileUnlockProcessor() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param modificationTime Modification time.
         */
        public FileUnlockProcessor(long modificationTime) {
            this.modificationTime = modificationTime;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<IgniteUuid, IgfsFileInfo> entry, Object... args)
            throws EntryProcessorException {
            IgfsFileInfo old = entry.getValue();

            entry.setValue(new IgfsFileInfo(old, null, modificationTime));

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(modificationTime);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            modificationTime = in.readLong();
        }
    }

    /**
     * File reserve space entry processor.
     */
    private static class FileReserveSpaceProcessor implements EntryProcessor<IgniteUuid, IgfsFileInfo, IgfsFileInfo>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Space. */
        private long space;

        /** Affinity range. */
        private IgfsFileAffinityRange affRange;

        /**
         * Default constructor.
         */
        public FileReserveSpaceProcessor() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param space Space.
         * @param affRange
         */
        public FileReserveSpaceProcessor(long space, IgfsFileAffinityRange affRange) {
            this.space = space;
            this.affRange = affRange;
        }

        /** {@inheritDoc} */
        @Override public IgfsFileInfo process(MutableEntry<IgniteUuid, IgfsFileInfo> entry, Object... args)
            throws EntryProcessorException {
            IgfsFileInfo oldInfo = entry.getValue();

            IgfsFileMap newMap = new IgfsFileMap(oldInfo.fileMap());

            newMap.addRange(affRange);

            IgfsFileInfo newInfo = new IgfsFileInfo(oldInfo, oldInfo.length() + space);

            newInfo.fileMap(newMap);

            entry.setValue(newInfo);

            return newInfo;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(space);
            out.writeObject(affRange);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            space = in.readLong();
            affRange = (IgfsFileAffinityRange)in.readObject();
        }
    }

    /**
     * Update properties processor.
     */
    private static class UpdatePropertiesProcessor implements EntryProcessor<IgniteUuid, IgfsFileInfo, IgfsFileInfo>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Properties to be updated. */
        private Map<String, String> props;

        /**
         * Constructor.
         */
        public UpdatePropertiesProcessor() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param props Properties.
         */
        public UpdatePropertiesProcessor(Map<String, String> props) {
            this.props = props;
        }

        /** {@inheritDoc} */
        @Override public IgfsFileInfo process(MutableEntry<IgniteUuid, IgfsFileInfo> entry, Object... args)
            throws EntryProcessorException {
            IgfsFileInfo oldInfo = entry.getValue();

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

            entry.setValue(newInfo);

            return newInfo;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeStringMap(out, props);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            props = U.readStringMap(in);
        }
    }

    /**
     * Update times entry processor.
     */
    private static class UpdateTimesProcessor implements EntryProcessor<IgniteUuid, IgfsFileInfo, Void>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Access time. */
        private long accessTime;

        /** Modification time. */
        private long modificationTime;

        /**
         * Default constructor.
         */
        public UpdateTimesProcessor() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param accessTime Access time.
         * @param modificationTime Modification time.
         */
        public UpdateTimesProcessor(long accessTime, long modificationTime) {
            this.accessTime = accessTime;
            this.modificationTime = modificationTime;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<IgniteUuid, IgfsFileInfo> entry, Object... args)
            throws EntryProcessorException {

            IgfsFileInfo oldInfo = entry.getValue();

            entry.setValue(new IgfsFileInfo(oldInfo, accessTime, modificationTime));

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(accessTime);
            out.writeLong(modificationTime);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            accessTime = in.readLong();
            modificationTime = in.readLong();
        }
    }
}