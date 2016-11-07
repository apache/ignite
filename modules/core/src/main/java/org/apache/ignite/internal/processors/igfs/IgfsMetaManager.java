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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.igfs.client.IgfsClientAbstractCallable;
import org.apache.ignite.internal.processors.igfs.client.meta.IgfsClientMetaIdsForPathCallable;
import org.apache.ignite.internal.processors.igfs.client.meta.IgfsClientMetaInfoForPathCallable;
import org.apache.ignite.internal.processors.igfs.client.meta.IgfsClientMetaUnlockCallable;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaDirectoryCreateProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaDirectoryListingAddProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaDirectoryListingRemoveProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaDirectoryListingRenameProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaDirectoryListingReplaceProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaFileCreateProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaFileLockProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaFileReserveSpaceProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaFileUnlockProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaUpdatePropertiesProcessor;
import org.apache.ignite.internal.processors.igfs.meta.IgfsMetaUpdateTimesProcessor;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
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

import static org.apache.ignite.events.EventType.EVT_IGFS_DIR_RENAMED;
import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_RENAMED;

/**
 * Cache based structure (meta data) manager.
 */
public class IgfsMetaManager extends IgfsManager {
    /** Comparator for Id sorting. */
    private static final Comparator<IgniteUuid> PATH_ID_SORTING_COMPARATOR = new Comparator<IgniteUuid>() {
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
    private IgniteInternalCache<IgniteUuid, IgfsEntryInfo> id2InfoPrj;

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

    /** Relaxed flag. */
    private final boolean relaxed;

    /** Client flag. */
    private final boolean client;

    /** Compute facade for client tasks. */
    private IgniteCompute cliCompute;

    /**
     * Constructor.
     *
     * @param relaxed Relaxed mode flag.
     * @param client Client flag.
     */
    public IgfsMetaManager(boolean relaxed, boolean client) {
        this.relaxed = relaxed;
        this.client = client;
    }

    /**
     * Await initialization.
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
    @SuppressWarnings("RedundantCast")
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        metaCache = igfsCtx.kernalContext().cache().getOrStartCache(cfg.getMetaCacheName());

        assert metaCache != null;

        igfsCtx.kernalContext().cache().internalCache(cfg.getMetaCacheName()).preloader().startFuture()
            .listen(new CI1<IgniteInternalFuture<Object>>() {
                @Override public void apply(IgniteInternalFuture<Object> f) {
                    metaCacheStartLatch.countDown();
                }
            });

        id2InfoPrj = (IgniteInternalCache<IgniteUuid, IgfsEntryInfo>)metaCache.<IgniteUuid, IgfsEntryInfo>cache();

        locNode = igfsCtx.kernalContext().discovery().localNode();

        // Start background delete worker.
        if (!client) {
            delWorker = new IgfsDeleteWorker(igfsCtx);

            delWorker.start();
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        IgfsDeleteWorker delWorker0 = delWorker;

        if (delWorker0 != null) {
            delWorker0.cancel();

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
     * @return Client flag.
     */
    boolean isClient() {
        return client;
    }

    /**
     * Run client task.
     *
     * @param task Task.
     * @return Result.
     */
    <T> T runClientTask(IgfsClientAbstractCallable<T> task) {
        try {
            return runClientTask(IgfsUtils.ROOT_ID, task);
        }
        catch (ClusterTopologyException e) {
            throw new IgfsException("Failed to execute operation because there are no IGFS metadata nodes." , e);
        }
    }

    /**
     * Run client task.
     *
     * @param affinityFileId Affinity fileId.
     * @param task Task.
     * @return Result.
     */
    <T> T runClientTask(IgniteUuid affinityFileId, IgfsClientAbstractCallable<T> task) {
        try {
            return (cfg.isColocateMetadata()) ?
                clientCompute().affinityCall(cfg.getMetaCacheName(), affinityFileId, task) :
                clientCompute().call(task);
        }
        catch (ClusterTopologyException e) {
            throw new IgfsException("Failed to execute operation because there are no IGFS metadata nodes." , e);
        }
    }

    /**
     * Get compute facade for client tasks.
     *
     * @return Compute facade.
     */
    private IgniteCompute clientCompute() {
        assert client;

        IgniteCompute cliCompute0 = cliCompute;

        if (cliCompute0 == null) {
            IgniteEx ignite = igfsCtx.kernalContext().grid();

            ClusterGroup cluster = ignite.cluster().forIgfsMetadataDataNodes(cfg.getName(), cfg.getMetaCacheName());

            cliCompute0 = ignite.compute(cluster);

            cliCompute = cliCompute0;
        }

        assert cliCompute0 != null;

        return cliCompute0;
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
     * Gets all file IDs for components of specified path. Result cannot be empty - there is at least root element.
     * But each element (except the first) can be {@code null} if such files don't exist.
     *
     * @param path Path.
     * @return Collection of file IDs for components of specified path.
     * @throws IgniteCheckedException If failed.
     */
    public IgfsPathIds pathIds(IgfsPath path) throws IgniteCheckedException {
        // Prepare parts.
        String[] components = path.componentsArray();

        String[] parts = new String[components.length + 1];

        System.arraycopy(components, 0, parts, 1, components.length);

        // Get IDs.
        if (client) {
            List<IgniteUuid> ids = runClientTask(new IgfsClientMetaIdsForPathCallable(cfg.getName(), path));

            return new IgfsPathIds(path, parts, ids.toArray(new IgniteUuid[ids.size()]));
        }
        else {
            if (busyLock.enterBusy()) {
                try {
                    validTxState(false);

                    IgniteUuid[] ids = new IgniteUuid[parts.length];

                    ids[0] = IgfsUtils.ROOT_ID;

                    for (int i = 1; i < ids.length; i++) {
                        IgniteUuid id = fileId(ids[i - 1], parts[i], false);

                        if (id != null)
                            ids[i] = id;
                        else
                            break;
                    }

                    // Return.
                    return new IgfsPathIds(path, parts, ids);
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
            else
                throw new IllegalStateException("Failed to get file IDS because Grid is stopping: " + path);
        }
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
    @Nullable public IgfsEntryInfo info(@Nullable IgniteUuid fileId) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                if (fileId == null)
                    return null;

                IgfsEntryInfo info = getInfo(fileId);

                // Force root ID always exist in cache.
                if (info == null && IgfsUtils.ROOT_ID.equals(fileId))
                    info = createSystemDirectoryIfAbsent(fileId);

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
    public Map<IgniteUuid, IgfsEntryInfo> infos(Collection<IgniteUuid> fileIds) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                assert fileIds != null;

                if (F.isEmpty(fileIds))
                    return Collections.emptyMap();

                Map<IgniteUuid, IgfsEntryInfo> map = getInfos(fileIds);

                // Force root ID always exist in cache.
                if (fileIds.contains(IgfsUtils.ROOT_ID) && !map.containsKey(IgfsUtils.ROOT_ID)) {
                    map = new GridLeanMap<>(map);

                    map.put(IgfsUtils.ROOT_ID, createSystemDirectoryIfAbsent(IgfsUtils.ROOT_ID));
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
     * @param del If file is being locked for delete.
     * @return Locked file info or {@code null} if file cannot be locked or doesn't exist.
     * @throws IgniteCheckedException If the file with such id does not exist, or on another failure.
     */
    public @Nullable IgfsEntryInfo lock(IgniteUuid fileId, boolean del) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                assert fileId != null;

                try (IgniteInternalTx tx = startTx()) {
                    // Lock file ID for this transaction.
                    IgfsEntryInfo oldInfo = info(fileId);

                    if (oldInfo == null)
                        return null;

                    if (oldInfo.lockId() != null)
                        return null; // The file is already locked, we cannot lock it.

                    IgfsEntryInfo newInfo = invokeLock(fileId, del);

                    tx.commit();

                    return newInfo;
                }
                catch (GridClosureException e) {
                    throw U.cast(e);
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
     * @param del If lock ID is required for file deletion.
     * @return Lock ID.
     */
    private IgniteUuid createFileLockId(boolean del) {
        if (del)
            return IgfsUtils.DELETE_LOCK_ID;

        return IgniteUuid.fromUuid(locNode.id());
    }

    /**
     * Remove explicit lock on file held by the current stream.
     *
     * @param fileId File ID.
     * @param lockId Lock ID.
     * @param modificationTime Modification time to write to file info.
     * @throws IgniteCheckedException If failed.
     */
    public void unlock(final IgniteUuid fileId, final IgniteUuid lockId, final long modificationTime)
        throws IgniteCheckedException {
        unlock(fileId, lockId, modificationTime, false, 0, null);
    }

    /**
     * Remove explicit lock on file held by the current stream.
     *
     * @param fileId File ID.
     * @param lockId Lock ID.
     * @param modificationTime Modification time to write to file info.
     * @param updateSpace Whether to update space.
     * @param space Space.
     * @param affRange Affinity range.
     * @throws IgniteCheckedException If failed.
     */
    public void unlock(final IgniteUuid fileId, final IgniteUuid lockId, final long modificationTime,
        final boolean updateSpace, final long space, @Nullable final IgfsFileAffinityRange affRange)
        throws IgniteCheckedException {

        if(client) {
            runClientTask(new IgfsClientMetaUnlockCallable(cfg.getName(), fileId, lockId, modificationTime,
                updateSpace, space, affRange));

            return;
        }

        validTxState(false);

        if (busyLock.enterBusy()) {
            try {
                if (lockId == null)
                    return;

                // Temporary clear interrupted state for unlocking.
                final boolean interrupted = Thread.interrupted();

                try {
                    IgfsUtils.doInTransactionWithRetries(id2InfoPrj, new IgniteOutClosureX<Void>() {
                        @Override public Void applyx() throws IgniteCheckedException {
                            validTxState(true);

                            // Lock file ID for this transaction.
                            IgfsEntryInfo oldInfo = info(fileId);

                            if (oldInfo == null)
                                throw fsException(new IgfsPathNotFoundException("Failed to unlock file (file not " +
                                    "found): " + fileId));

                            if (!F.eq(lockId, oldInfo.lockId()))
                                throw new IgniteCheckedException("Failed to unlock file (inconsistent file lock ID) " +
                                    "[fileId=" + fileId + ", lockId=" + lockId + ", actualLockId=" +
                                    oldInfo.lockId() + ']');

                            id2InfoPrj.invoke(fileId,
                                new IgfsMetaFileUnlockProcessor(modificationTime, updateSpace, space, affRange));

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
            throw new IllegalStateException("Failed to unlock file system entry because Grid is stopping: " + fileId);
    }

    /**
     * Lock file IDs participating in the transaction.<br/>
     *
     * @param fileIds file IDs to lock.
     * @return Locked file details. Resulting map doesn't contain details for not-existent files.
     * @throws IgniteCheckedException If failed.
     */
    private Map<IgniteUuid, IgfsEntryInfo> lockIds(IgniteUuid... fileIds) throws IgniteCheckedException {
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
    private Map<IgniteUuid, IgfsEntryInfo> lockIds(Collection<IgniteUuid> fileIds) throws IgniteCheckedException {
        assert isSorted(fileIds);
        validTxState(true);

        if (log.isDebugEnabled())
            log.debug("Locking file ids: " + fileIds);

        // Lock files and get their infos.
        Map<IgniteUuid, IgfsEntryInfo> map = getInfos(fileIds);

        if (log.isDebugEnabled())
            log.debug("Locked file ids: " + fileIds);

        for (IgniteUuid fileId : fileIds) {
            if (IgfsUtils.isRootOrTrashId(fileId)) {
                if (!map.containsKey(fileId))
                    map.put(fileId, createSystemDirectoryIfAbsent(fileId));
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
    private IgfsEntryInfo createSystemDirectoryIfAbsent(IgniteUuid id)
        throws IgniteCheckedException {
        assert IgfsUtils.isRootOrTrashId(id);

        IgfsEntryInfo info = IgfsUtils.createDirectory(id);

        IgfsEntryInfo oldInfo = id2InfoPrj.getAndPutIfAbsent(id, info);

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
    public IgfsEntryInfo fileForFragmentizer(Collection<IgniteUuid> exclude) throws IgniteCheckedException {
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
    private IgfsEntryInfo fileForFragmentizer0(IgniteUuid parentId, Collection<IgniteUuid> exclude)
        throws IgniteCheckedException {
        IgfsEntryInfo info = info(parentId);

        // Check if file was concurrently deleted.
        if (info == null)
            return null;

        assert info.isDirectory();

        Map<String, IgfsListingEntry> listing = info.listing();

        for (IgfsListingEntry entry : listing.values()) {
            if (entry.isFile()) {
                IgfsEntryInfo fileInfo = info(entry.fileId());

                if (fileInfo != null) {
                    if (!exclude.contains(fileInfo.id()) &&
                        fileInfo.fileMap() != null &&
                        !fileInfo.fileMap().ranges().isEmpty())
                        return fileInfo;
                }
            }
            else {
                IgfsEntryInfo fileInfo = fileForFragmentizer0(entry.fileId(), exclude);

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

        IgfsEntryInfo info = skipTx ? id2InfoPrj.getAllOutTx(Collections.singleton(fileId)).get(fileId) :
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
    private IgniteUuid putIfAbsentNonTx(IgniteUuid parentId, String fileName, IgfsEntryInfo newFileInfo)
        throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Locking parent id [parentId=" + parentId + ", fileName=" + fileName + ", newFileInfo=" +
                newFileInfo + ']');

        validTxState(true);

        // Lock only parent file ID.
        IgfsEntryInfo parentInfo = info(parentId);

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
     * @param dstPath Destination path.
     * @throws IgniteCheckedException In case of exception.
     */
    public void move(IgfsPath srcPath, IgfsPath dstPath) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                // Prepare path IDs.
                IgfsPathIds srcPathIds = pathIds(srcPath);
                IgfsPathIds dstPathIds = pathIds(dstPath);

                // Source path must exists.
                if (!srcPathIds.allExists())
                    throw new IgfsPathNotFoundException("Failed to perform move because source path is not " +
                        "found: " + srcPath);

                // At this point we need to understand name of resulting entry. It will be either destination leaf
                // or source leaf depending on existence.
                String dstName;

                if (dstPathIds.lastExists())
                    //  Full destination path exists -> use source name.
                    dstName = srcPathIds.lastPart();
                else {
                    if (dstPathIds.lastParentExists()) {
                        // Destination path doesn't exists -> use destination name.
                        dstName = dstPathIds.lastPart();

                        dstPathIds = dstPathIds.parent();
                    }
                    else
                        // Destination parent is not found either -> exception.
                        throw new IgfsPathNotFoundException("Failed to perform move because destination path is not " +
                            "found: " + dstPath.parent());
                }

                // Lock participating IDs.
                final Set<IgniteUuid> lockIds = new TreeSet<>(PATH_ID_SORTING_COMPARATOR);

                srcPathIds.addExistingIds(lockIds, relaxed);
                dstPathIds.addExistingIds(lockIds, relaxed);

                try (IgniteInternalTx tx = startTx()) {
                    // Obtain the locks.
                    final Map<IgniteUuid, IgfsEntryInfo> lockInfos = lockIds(lockIds);

                    // Verify integrity of source and destination paths.
                    if (!srcPathIds.verifyIntegrity(lockInfos, relaxed))
                        throw new IgfsPathNotFoundException("Failed to perform move because source directory " +
                            "structure changed concurrently [src=" + srcPath + ", dst=" + dstPath + ']');

                    if (!dstPathIds.verifyIntegrity(lockInfos, relaxed))
                        throw new IgfsPathNotFoundException("Failed to perform move because destination directory " +
                            "structure changed concurrently [src=" + srcPath + ", dst=" + dstPath + ']');

                    // Addiional check: is destination directory?
                    IgfsEntryInfo dstParentInfo = lockInfos.get(dstPathIds.lastId());

                    if (dstParentInfo.isFile())
                        throw new IgfsPathAlreadyExistsException("Failed to perform move because destination points " +
                            "to existing file [src=" + srcPath + ", dst=" + dstPath + ']');

                    // Additional check: does destination already has child with the same name?
                    if (dstParentInfo.hasChild(dstName))
                        throw new IgfsPathAlreadyExistsException("Failed to perform move because destination already " +
                            "contains entry with the same name existing file [src=" + srcPath +
                            ", dst=" + dstPath + ']');

                    // Actual move: remove from source parent and add to destination target.
                    IgfsEntryInfo srcParentInfo = lockInfos.get(srcPathIds.lastParentId());

                    IgfsEntryInfo srcInfo = lockInfos.get(srcPathIds.lastId());
                    String srcName = srcPathIds.lastPart();
                    IgfsListingEntry srcEntry = srcParentInfo.listing().get(srcName);

                    transferEntry(srcEntry, srcParentInfo.id(), srcName, dstParentInfo.id(), dstName);

                    tx.commit();

                    // Fire events.
                    IgfsPath newPath = new IgfsPath(dstPathIds.path(), dstName);

                    IgfsUtils.sendEvents(igfsCtx.kernalContext(), srcPath, newPath,
                        srcInfo.isFile() ? EVT_IGFS_FILE_RENAMED : EVT_IGFS_DIR_RENAMED);
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
     * Move or rename file in existing transaction.
     *
     * @param fileId File ID to move or rename.
     * @param srcFileName Original file name in the parent's listing.
     * @param srcParentId Parent directory ID.
     * @param destFileName New file name in the parent's listing after moving.
     * @param destParentId New parent directory ID.
     * @throws IgniteCheckedException If failed.
     */
    private void moveNonTx(IgniteUuid fileId, String srcFileName, IgniteUuid srcParentId, String destFileName,
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
        Map<IgniteUuid, IgfsEntryInfo> infoMap = lockIds(srcParentId, fileId, destParentId);

        IgfsEntryInfo srcInfo = infoMap.get(srcParentId);

        if (srcInfo == null)
            throw fsException(new IgfsPathNotFoundException("Failed to lock source directory (not found?)" +
                " [srcParentId=" + srcParentId + ']'));

        if (!srcInfo.isDirectory())
            throw fsException(new IgfsPathIsNotDirectoryException("Source is not a directory: " + srcInfo));

        IgfsEntryInfo destInfo = infoMap.get(destParentId);

        if (destInfo == null)
            throw fsException(new IgfsPathNotFoundException("Failed to lock destination directory (not found?)" +
                " [destParentId=" + destParentId + ']'));

        if (!destInfo.isDirectory())
            throw fsException(new IgfsPathIsNotDirectoryException("Destination is not a directory: " + destInfo));

        IgfsEntryInfo fileInfo = infoMap.get(fileId);

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
    @SuppressWarnings("RedundantCast")
    IgniteUuid format() throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                IgniteUuid trashId = IgfsUtils.randomTrashId();

                try (IgniteInternalTx tx = startTx()) {
                    // NB: We may lock root because its id is less than any other id:
                    final IgfsEntryInfo rootInfo = lockIds(IgfsUtils.ROOT_ID, trashId).get(IgfsUtils.ROOT_ID);

                    assert rootInfo != null;

                    Map<String, IgfsListingEntry> rootListingMap = rootInfo.listing();

                    assert rootListingMap != null;

                    if (rootListingMap.isEmpty())
                        return null; // Root is empty, nothing to do.

                    // Construct new info and move locked entries from root to it.
                    Map<String, IgfsListingEntry> transferListing = new HashMap<>(rootListingMap);

                    IgfsEntryInfo newInfo = IgfsUtils.createDirectory(
                        IgniteUuid.randomUuid(),
                        transferListing,
                        (Map<String, String>) null
                    );

                    createNewEntry(newInfo, trashId, newInfo.id().toString());

                    // Remove listing entries from root.
                    // Note that root directory properties and other attributes are preserved:
                    id2InfoPrj.put(IgfsUtils.ROOT_ID, rootInfo.listing(null));

                    tx.commit();

                    signalDeleteWorker();

                    return newInfo.id();
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
     * Whether operation must be re-tried because we have suspicious links which may broke secondary file system
     * consistency.
     *
     * @param pathIds Path IDs.
     * @param lockInfos Lock infos.
     * @return Whether to re-try.
     */
    private static boolean isRetryForSecondary(IgfsPathIds pathIds, Map<IgniteUuid, IgfsEntryInfo> lockInfos) {
        // We need to ensure that the last locked info is not linked with expected child.
        // Otherwise there was some concurrent file system update and we have to re-try.
        // That is, the following situation lead to re-try:
        // 1) We queried path /A/B/C
        // 2) Returned IDs are ROOT_ID, A_ID, B_ID, null
        // 3) But B's info contains C as child. It mean's that
        if (!pathIds.allExists()) {
            // Find the last locked index
            IgfsEntryInfo lastLockedInfo = null;
            int lastLockedIdx = -1;

            while (lastLockedIdx < pathIds.lastExistingIndex()) {
                IgfsEntryInfo nextInfo = lockInfos.get(pathIds.id(lastLockedIdx + 1));

                if (nextInfo != null) {
                    lastLockedInfo = nextInfo;
                    lastLockedIdx++;
                }
                else
                    break;
            }

            assert lastLockedIdx < pathIds.count();

            if (lastLockedInfo != null) {
                String part = pathIds.part(lastLockedIdx + 1);

                if (lastLockedInfo.listing().containsKey(part))
                    return true;
            }
        }

        return false;
    }

    /**
     * Move path to the trash directory.
     *
     * @param path Path.
     * @param recursive Recursive flag.
     * @param secondaryFs Secondary file system (optional).
     * @return ID of an entry located directly under the trash directory.
     * @throws IgniteCheckedException If failed.
     */
    IgfsDeleteResult softDelete(final IgfsPath path, final boolean recursive,
        @Nullable IgfsSecondaryFileSystem secondaryFs) throws IgniteCheckedException {
        while (true) {
            if (busyLock.enterBusy()) {
                try {
                    validTxState(false);

                    IgfsPathIds pathIds = pathIds(path);

                    if (!pathIds.allExists() && secondaryFs == null)
                        return new IgfsDeleteResult(false, null);

                    IgniteUuid victimId = pathIds.lastId();
                    String victimName = pathIds.lastPart();

                    if (IgfsUtils.isRootId(victimId))
                        throw new IgfsException("Cannot remove root directory");

                    // Prepare IDs to lock.
                    SortedSet<IgniteUuid> allIds = new TreeSet<>(PATH_ID_SORTING_COMPARATOR);

                    pathIds.addExistingIds(allIds, relaxed);

                    IgniteUuid trashId = IgfsUtils.randomTrashId();

                    allIds.add(trashId);

                    try (IgniteInternalTx tx = startTx()) {
                        // Lock participants.
                        Map<IgniteUuid, IgfsEntryInfo> lockInfos = lockIds(allIds);

                        if (secondaryFs != null && isRetryForSecondary(pathIds, lockInfos))
                            continue;

                        // Ensure that all participants are still in place.
                        if (!pathIds.allExists() || !pathIds.verifyIntegrity(lockInfos, relaxed)) {
                            // For DUAL mode we will try to update the underlying FS still. Note we do that inside TX.
                            if (secondaryFs != null) {
                                boolean res = secondaryFs.delete(path, recursive);

                                return new IgfsDeleteResult(res, null);
                            }
                            else
                                return new IgfsDeleteResult(false, null);
                        }

                        IgfsEntryInfo victimInfo = lockInfos.get(victimId);

                        // Cannot delete non-empty directory if recursive flag is not set.
                        if (!recursive && victimInfo.hasChildren())
                            throw new IgfsDirectoryNotEmptyException("Failed to remove directory (directory is not " +
                                "empty and recursive flag is not set).");

                    // Prepare trash data.
                    IgfsEntryInfo trashInfo = lockInfos.get(trashId);

                    final String trashName = IgfsUtils.composeNameForTrash(path, victimId);

                        assert !trashInfo.hasChild(trashName) : "Failed to add file name into the " +
                            "destination directory (file already exists) [destName=" + trashName + ']';

                        IgniteUuid parentId = pathIds.lastParentId();
                        IgfsEntryInfo parentInfo = lockInfos.get(parentId);

                        // Propagate call to the secondary file system.
                        if (secondaryFs != null && !secondaryFs.delete(path, recursive))
                            return new IgfsDeleteResult(false, null);

                        transferEntry(parentInfo.listing().get(victimName), parentId, victimName, trashId, trashName);

                        tx.commit();

                        signalDeleteWorker();

                        return new IgfsDeleteResult(true, victimInfo);
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

                try (IgniteInternalTx tx = startTx()) {
                    Collection<IgniteUuid> res = new HashSet<>();

                    // Obtain all necessary locks in one hop.
                    IgniteUuid[] allIds = new IgniteUuid[listing.size() + 1];

                    allIds[0] = parentId;

                    int i = 1;

                    for (IgfsListingEntry childEntry : listing.values())
                        allIds[i++] = childEntry.fileId();

                    Map<IgniteUuid, IgfsEntryInfo> locks = lockIds(allIds);

                    IgfsEntryInfo parentInfo = locks.get(parentId);

                    // Ensure parent is still in place.
                    if (parentInfo != null) {
                        Map<String, IgfsListingEntry> parentListing = parentInfo.listing();

                        Map<String, IgfsListingEntry> newListing = new HashMap<>(parentListing.size(), 1.0f);

                        newListing.putAll(parentListing);

                        // Remove child entries if possible.
                        for (Map.Entry<String, IgfsListingEntry> entry : listing.entrySet()) {
                            String childName = entry.getKey();
                            IgniteUuid childId = entry.getValue().fileId();

                            IgfsEntryInfo entryInfo = locks.get(childId);

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
                        id2InfoPrj.put(parentId, parentInfo.listing(newListing));
                    }

                    tx.commit();

                    return res;
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

                try (IgniteInternalTx tx = startTx()) {
                    Map<IgniteUuid, IgfsEntryInfo> infos = lockIds(parentId, id);

                    IgfsEntryInfo victim = infos.get(id);

                    if (victim == null)
                        return false;

                    assert victim.isDirectory() || IgfsUtils.DELETE_LOCK_ID.equals(victim.lockId()) :
                        " isDir: " + victim.isDirectory() + ", lockId: " + victim.lockId();

                    // Proceed only in case both parent and child exist.
                    if (infos.containsKey(parentId) && infos.containsKey(id)) {
                        IgfsEntryInfo parentInfo = infos.get(parentId);

                        assert parentInfo != null;

                        IgfsListingEntry childEntry = parentInfo.listing().get(name);

                        if (childEntry != null)
                            id2InfoPrj.invoke(parentId, new IgfsMetaDirectoryListingRemoveProcessor(name, id));

                        id2InfoPrj.remove(id);

                        tx.commit();

                        return true;
                    }

                    return false;
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
     * Update file info (file properties) in cache in existing transaction.
     *
     * @param fileId File ID to update information for.
     * @param props Properties to set for the file.
     * @return Updated file info or {@code null} if such file ID not found.
     * @throws IgniteCheckedException If operation failed.
     */
    @Nullable private IgfsEntryInfo updatePropertiesNonTx(final IgniteUuid fileId, Map<String, String> props)
        throws IgniteCheckedException {
        assert fileId != null;
        assert !F.isEmpty(props) : "Expects not-empty file's properties";

        validTxState(true);

        if (log.isDebugEnabled())
            log.debug("Update file properties [fileId=" + fileId + ", props=" + props + ']');

        try {
            final IgfsEntryInfo oldInfo = info(fileId);

            if (oldInfo == null)
                return null;

            return invokeAndGet(fileId, new IgfsMetaUpdatePropertiesProcessor(props));
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
    @Nullable public IgfsEntryInfo updateProperties(IgniteUuid fileId, Map<String, String> props)
        throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                try (IgniteInternalTx tx = startTx()) {
                    IgfsEntryInfo info = updatePropertiesNonTx(fileId, props);

                    tx.commit();

                    return info;
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
     * @param fileId File ID.
     * @param space Space.
     * @param affRange Affinity range.
     * @return New file info.
     */
    public IgfsEntryInfo reserveSpace(IgniteUuid fileId, long space, IgfsFileAffinityRange affRange)
        throws IgniteCheckedException {
        validTxState(false);

        if (busyLock.enterBusy()) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Reserve file space: " + fileId);

                try (IgniteInternalTx tx = startTx()) {
                    // Lock file ID for this transaction.
                    IgfsEntryInfo oldInfo = info(fileId);

                    if (oldInfo == null)
                        throw fsException("File has been deleted concurrently: " + fileId);

                    IgfsEntryInfo newInfo =
                        invokeAndGet(fileId, new IgfsMetaFileReserveSpaceProcessor(space, affRange));

                    tx.commit();

                    return newInfo;
                }
                catch (GridClosureException e) {
                    throw U.cast(e);
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to reserve file space because Grid is stopping:" + fileId);
    }

    /**
     * Update file info in cache.
     *
     * @param fileId File ID to update information for.
     * @param proc Entry processor to invoke.
     * @return Updated file info or {@code null} if such file ID not found.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgfsEntryInfo updateInfo(IgniteUuid fileId,
        EntryProcessor<IgniteUuid, IgfsEntryInfo, IgfsEntryInfo> proc) throws IgniteCheckedException {
        validTxState(false);
        assert fileId != null;
        assert proc != null;

        if (busyLock.enterBusy()) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Update file info [fileId=" + fileId + ", proc=" + proc + ']');

                try (IgniteInternalTx tx = startTx()) {
                    // Lock file ID for this transaction.
                    IgfsEntryInfo oldInfo = info(fileId);

                    if (oldInfo == null)
                        return null; // File not found.

                    IgfsEntryInfo newInfo = invokeAndGet(fileId, proc);

                    if (newInfo == null)
                        throw fsException("Failed to update file info with null value" +
                            " [oldInfo=" + oldInfo + ", newInfo=null, proc=" + proc + ']');

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
     * @return True if a directory was created during the operation.
     * @throws IgniteCheckedException If a non-directory file exists on the requested path, and in case of other errors.
     */
    boolean mkdirs(final IgfsPath path, final Map<String, String> props) throws IgniteCheckedException {
        validTxState(false);

        while (true) {
            if (busyLock.enterBusy()) {
                try {
                    // Prepare path IDs.
                    IgfsPathIds pathIds = pathIds(path);

                    // Prepare lock IDs. Essentially, they consist of two parts: existing IDs and potential new IDs.
                    Set<IgniteUuid> lockIds = new TreeSet<>(PATH_ID_SORTING_COMPARATOR);

                    pathIds.addExistingIds(lockIds, relaxed);
                    pathIds.addSurrogateIds(lockIds);

                    // Start TX.
                    try (IgniteInternalTx tx = startTx()) {
                        final Map<IgniteUuid, IgfsEntryInfo> lockInfos = lockIds(lockIds);

                        if (!pathIds.verifyIntegrity(lockInfos, relaxed))
                            // Directory structure changed concurrently. So we simply re-try.
                            continue;

                        // Check if the whole structure is already in place.
                        if (pathIds.allExists()) {
                            if (lockInfos.get(pathIds.lastExistingId()).isDirectory())
                                return false;
                            else
                                throw new IgfsParentNotDirectoryException("Failed to create directory (parent " +
                                    "element is not a directory)");
                        }

                        IgfsPathsCreateResult res = createDirectory(pathIds, lockInfos, props);

                        if (res == null)
                            continue;

                        // Commit TX.
                        tx.commit();

                        generateCreateEvents(res.createdPaths(), false);

                        // We are done.
                        return true;
                    }
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
            else
                throw new IllegalStateException("Failed to mkdir because Grid is stopping. [path=" + path + ']');
        }
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

                try (IgniteInternalTx tx = startTx()) {
                    Object prev = val != null ? metaCache.getAndPut(sampling, val) : metaCache.getAndRemove(sampling);

                    tx.commit();

                    return !F.eq(prev, val);
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
    private void createNewEntry(IgfsEntryInfo info, IgniteUuid parentId, String name) throws IgniteCheckedException {
        validTxState(true);

        if (!id2InfoPrj.putIfAbsent(info.id(), info))
            throw fsException("Failed to create new metadata entry due to ID conflict: " + info.id());

        if (parentId != null)
            id2InfoPrj.invoke(parentId, new IgfsMetaDirectoryListingAddProcessor(name, new IgfsListingEntry(info)));
    }

    /**
     * Transfer entry from one directory to another.
     *
     * @param entry Entry to be transferred.
     * @param srcId Source ID.
     * @param srcName Source name.
     * @param destId Destination ID.
     * @param destName Destination name.
     * @throws IgniteCheckedException If failed.
     */
    private void transferEntry(IgfsListingEntry entry, IgniteUuid srcId, String srcName,
        IgniteUuid destId, String destName) throws IgniteCheckedException {
        validTxState(true);

        if (F.eq(srcId, destId))
            id2InfoPrj.invoke(srcId, new IgfsMetaDirectoryListingRenameProcessor(srcName, destName));
        else {

            Map<IgniteUuid, EntryProcessor<IgniteUuid, IgfsEntryInfo, Void>> procMap = new HashMap<>();

            procMap.put(srcId, new IgfsMetaDirectoryListingRemoveProcessor(srcName, entry.fileId()));
            procMap.put(destId, new IgfsMetaDirectoryListingAddProcessor(destName, entry));

            id2InfoPrj.invokeAll(procMap);
        }
    }

    /**
     * Invoke lock processor.
     *
     * @param id File ID.
     * @param del Whether lock is taken for delete.
     * @return Resulting file info.
     * @throws IgniteCheckedException If failed.
     */
    private IgfsEntryInfo invokeLock(IgniteUuid id, boolean del) throws IgniteCheckedException {
        return invokeAndGet(id, new IgfsMetaFileLockProcessor(createFileLockId(del)));
    }

    /**
     * Invoke some processor and return new value.
     *
     * @param id ID.
     * @param proc Processor.
     * @return New file info.
     * @throws IgniteCheckedException If failed.
     */
    private IgfsEntryInfo invokeAndGet(IgniteUuid id, EntryProcessor<IgniteUuid, IgfsEntryInfo, IgfsEntryInfo> proc)
        throws IgniteCheckedException {
        validTxState(true);

        EntryProcessorResult<IgfsEntryInfo> res = id2InfoPrj.invoke(id, proc);

        assert res != null;

        return res.get();
    }

    /**
     * Get info.
     *
     * @param id ID.
     * @return Info.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private IgfsEntryInfo getInfo(IgniteUuid id) throws IgniteCheckedException {
        return id2InfoPrj.get(id);
    }

    /**
     * Get several infos.
     *
     * @param ids IDs.
     * @return Infos map.
     * @throws IgniteCheckedException If failed.
     */
    private Map<IgniteUuid, IgfsEntryInfo> getInfos(Collection<IgniteUuid> ids) throws IgniteCheckedException {
        return id2InfoPrj.getAll(ids);
    }

    /**
     * A delegate method that performs file creation in the synchronization task.
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
     * @param infos Map from paths to corresponding infos.
     * @param pendingEvts A non-null collection the events are to be accumulated in.
     * @param t1 A signle-object tuple to hold the created output stream.
     * @return Output stream descriptor.
     * @throws Exception On error.
     */
    IgfsCreateResult onSuccessCreate(IgfsSecondaryFileSystem fs, IgfsPath path,
        boolean simpleCreate, @Nullable final Map<String, String> props, boolean overwrite,
        int bufSize, short replication, long blockSize, IgniteUuid affKey, Map<IgfsPath, IgfsEntryInfo> infos,
        final Deque<IgfsEvent> pendingEvts, final T1<OutputStream> t1) throws Exception {
        validTxState(true);

        assert !infos.isEmpty();

        // Determine the first existing parent.
        IgfsPath parentPath = null;

        for (IgfsPath curPath : infos.keySet()) {
            if (parentPath == null || curPath.isSubDirectoryOf(parentPath))
                parentPath = curPath;
        }

        assert parentPath != null;

        IgfsEntryInfo parentInfo = infos.get(parentPath);

        // Delegate to the secondary file system.
        OutputStream out = simpleCreate ? fs.create(path, overwrite) :
            fs.create(path, bufSize, overwrite, replication, blockSize, props);

        t1.set(out);

        IgfsPath parent0 = path.parent();

        assert parent0 != null : "path.parent() is null (are we creating ROOT?): " + path;

        // If some of the parent directories were missing, synchronize again.
        if (!parentPath.equals(parent0)) {
            parentInfo = synchronize(fs, parentPath, parentInfo, parent0, true, null);

            // Fire notification about missing directories creation.
            if (evts.isRecordable(EventType.EVT_IGFS_DIR_CREATED)) {
                IgfsPath evtPath = parent0;

                while (!parentPath.equals(evtPath)) {
                    pendingEvts.addFirst(new IgfsEvent(evtPath, locNode,
                        EventType.EVT_IGFS_DIR_CREATED));

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

        IgfsEntryInfo newInfo = IgfsUtils.createFile(
            IgniteUuid.randomUuid(),
            igfsCtx.configuration().getBlockSize(),
            status.length(),
            affKey,
            createFileLockId(false),
            igfsCtx.igfs().evictExclude(path, false),
            status.properties(),
            status.accessTime(),
            status.modificationTime()
        );

        // Add new file info to the listing optionally removing the previous one.
        assert parentInfo != null;

        IgniteUuid oldId = putIfAbsentNonTx(parentInfo.id(), path.name(), newInfo);

        if (oldId != null) {
            IgfsEntryInfo oldInfo = info(oldId);

            assert oldInfo != null; // Otherwise cache is in inconsistent state.

            // The contact is that we cannot overwrite a file locked for writing:
            if (oldInfo.lockId() != null)
                throw fsException("Failed to overwrite file (file is opened for writing) [path=" +
                    path + ", fileId=" + oldId + ", lockId=" + oldInfo.lockId() + ']');

            id2InfoPrj.remove(oldId); // Remove the old one.
            id2InfoPrj.invoke(parentInfo.id(), new IgfsMetaDirectoryListingRemoveProcessor(
                path.name(), parentInfo.listing().get(path.name()).fileId()));

            createNewEntry(newInfo, parentInfo.id(), path.name()); // Put new one.

            igfsCtx.data().delete(oldInfo);
        }

        // Record CREATE event if needed.
        if (oldId == null && evts.isRecordable(EventType.EVT_IGFS_FILE_CREATED))
            pendingEvts.add(new IgfsEvent(path, locNode, EventType.EVT_IGFS_FILE_CREATED));

        return new IgfsCreateResult(newInfo, out);
    }

    /**
     * Append to a file in DUAL mode.
     *
     * @param fs File system.
     * @param path Path.
     * @param bufSize Buffer size.
     * @param create Create flag.
     * @return Output stream descriptor.
     * @throws IgniteCheckedException If output stream open for append has failed.
     */
    public IgfsCreateResult appendDual(final IgfsSecondaryFileSystem fs, final IgfsPath path,
        final int bufSize, final boolean create) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert path != null;

                // Events to fire (can be done outside of a transaction).
                final Deque<IgfsEvent> pendingEvts = new LinkedList<>();

                SynchronizationTask<IgfsCreateResult> task =
                    new SynchronizationTask<IgfsCreateResult>() {
                        /** Container for the secondary file system output stream. */
                        private final T1<OutputStream> outT1 = new T1<>(null);

                        @Override public IgfsCreateResult onSuccess(Map<IgfsPath,
                            IgfsEntryInfo> infos) throws Exception {
                            validTxState(true);

                            final IgfsEntryInfo info = infos.get(path);

                            final IgfsEntryInfo lockedInfo;

                            if (info == null)
                                return onSuccessCreate(fs, path, true/*simpleCreate*/, null,
                                        false/*overwrite*/, bufSize, (short)0, 0, null, infos, pendingEvts, outT1);
                            else {
                                if (info.isDirectory())
                                    throw fsException("Failed to open output stream to the file in the " +
                                        "secondary file system because the path points to a directory: " + path);

                                outT1.set(fs.append(path, bufSize, false, null));

                                // Synchronize file ending.
                                long len = info.length();
                                int blockSize = info.blockSize();

                                int remainder = (int) (len % blockSize);

                                if (remainder > 0) {
                                    int blockIdx = (int) (len / blockSize);

                                    try (IgfsSecondaryFileSystemPositionedReadable reader = fs.open(path, bufSize)) {
                                        IgniteInternalFuture<byte[]> fut =
                                            igfsCtx.data().dataBlock(info, path, blockIdx, reader);

                                        assert fut != null;

                                        fut.get();
                                    }
                                }

                                if (info.lockId() != null) {
                                    throw fsException("Failed to open file (file is opened for writing) [path=" +
                                        path + ", fileId=" + info.id() + ", lockId=" + info.lockId() + ']');
                                }

                                // Set lock and return.
                                lockedInfo = invokeLock(info.id(), false);
                            }

                            if (evts.isRecordable(EventType.EVT_IGFS_FILE_OPENED_WRITE))
                                pendingEvts.add(new IgfsEvent(path, locNode, EventType.EVT_IGFS_FILE_OPENED_WRITE));

                            return new IgfsCreateResult(lockedInfo, outT1.get());
                        }

                        @Override public IgfsCreateResult onFailure(@Nullable Exception err)
                            throws IgniteCheckedException {
                            U.closeQuiet(outT1.get());

                            U.error(log, "File append in DUAL mode failed [path=" + path + ", bufferSize=" + bufSize +
                                ']', err);

                            throw new IgniteCheckedException("Failed to append to the file due to secondary file " +
                                "system exception: " + path, err);
                    }
                };

                try {
                    return synchronizeAndExecute(task, fs, !create/*strict*/, path);
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
            throw new IllegalStateException("Failed to append to file in DUAL mode because Grid is stopping: " + path);
    }

    /**
     * Get info for the given path.
     *
     * @param path Path.
     * @return Info.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgfsEntryInfo infoForPath(IgfsPath path) throws IgniteCheckedException {
        return client ? runClientTask(new IgfsClientMetaInfoForPathCallable(cfg.getName(), path)) : info(fileId(path));
    }

    /**
     * Get IDs for the given path.
     *
     * @param path Path.
     * @return IDs.
     * @throws IgniteCheckedException If failed.
     */
    public List<IgniteUuid> idsForPath(IgfsPath path) throws IgniteCheckedException {
        return client ? runClientTask(new IgfsClientMetaIdsForPathCallable(cfg.getName(), path)) : fileIds(path);
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
        final int bufSize) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert path != null;

                // First, try getting file info without any transactions and synchronization.
                IgfsEntryInfo info = infoForPath(path);

                if (info != null) {
                    if (!info.isFile())
                        throw fsException(new IgfsPathIsDirectoryException("Failed to open file (not a file): " +
                            path));

                    return new IgfsSecondaryInputStreamDescriptor(info, lazySecondaryReader(fs, path, bufSize));
                }

                // If failed, try synchronize.
                SynchronizationTask<IgfsSecondaryInputStreamDescriptor> task =
                    new SynchronizationTask<IgfsSecondaryInputStreamDescriptor>() {
                        @Override public IgfsSecondaryInputStreamDescriptor onSuccess(
                            Map<IgfsPath, IgfsEntryInfo> infos) throws Exception {
                            IgfsEntryInfo info = infos.get(path);

                            if (info == null)
                                throw fsException(new IgfsPathNotFoundException("File not found: " + path));
                            if (!info.isFile())
                                throw fsException(new IgfsPathIsDirectoryException("Failed to open file " +
                                    "(not a file): " + path));

                            return new IgfsSecondaryInputStreamDescriptor(infos.get(path),
                                lazySecondaryReader(fs, path, bufSize));
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
     * Create lazy secondary file system reader.
     *
     * @param fs File system.
     * @param path Path.
     * @param bufSize Buffer size.
     * @return Lazy reader.
     */
    private static IgfsLazySecondaryFileSystemPositionedReadable lazySecondaryReader(IgfsSecondaryFileSystem fs,
        IgfsPath path, int bufSize) {
        return new IgfsLazySecondaryFileSystemPositionedReadable(fs, path, bufSize);
    }

    /**
     * Synchronizes with secondary file system.
     *
     * @param fs File system.
     * @param path Path.
     * @return File info or {@code null} if file not found.
     * @throws IgniteCheckedException If sync task failed.
     */
    @Nullable public IgfsEntryInfo synchronizeFileDual(final IgfsSecondaryFileSystem fs, final IgfsPath path)
        throws IgniteCheckedException {
        assert fs != null;
        assert path != null;

        if (busyLock.enterBusy()) {
            try {
                // First, try getting file info without any transactions and synchronization.
                IgfsEntryInfo info = infoForPath(path);

                if (info != null)
                    return info;

                // If failed, try synchronize.
                SynchronizationTask<IgfsEntryInfo> task =
                    new SynchronizationTask<IgfsEntryInfo>() {
                        @Override public IgfsEntryInfo onSuccess(Map<IgfsPath, IgfsEntryInfo> infos)
                            throws Exception {
                            return infos.get(path);
                        }

                        @Override public IgfsEntryInfo onFailure(@Nullable Exception err) throws IgniteCheckedException {
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
                    @Override public Boolean onSuccess(Map<IgfsPath, IgfsEntryInfo> infos) throws Exception {
                        fs.mkdirs(path, props);

                        assert !infos.isEmpty();

                        // Now perform synchronization again starting with the last created parent.
                        IgfsPath parentPath = null;

                        for (IgfsPath curPath : infos.keySet()) {
                            if (parentPath == null || curPath.isSubDirectoryOf(parentPath))
                                parentPath = curPath;
                        }

                        assert parentPath != null;

                        IgfsEntryInfo parentPathInfo = infos.get(parentPath);

                        synchronize(fs, parentPath, parentPathInfo, path, true, null);

                        if (evts.isRecordable(EventType.EVT_IGFS_DIR_CREATED)) {
                            IgfsPath evtPath = path;

                            while (!parentPath.equals(evtPath)) {
                                pendingEvts.addFirst(new IgfsEvent(evtPath, locNode, EventType.EVT_IGFS_DIR_CREATED));

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
                    @Override public Boolean onSuccess(Map<IgfsPath, IgfsEntryInfo> infos) throws Exception {
                        IgfsEntryInfo srcInfo = infos.get(src);
                        IgfsEntryInfo srcParentInfo = infos.get(src.parent());
                        IgfsEntryInfo destInfo = infos.get(dest);
                        IgfsEntryInfo destParentInfo = dest.parent() != null ? infos.get(dest.parent()) : null;

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
                        if (destInfo == null)
                            moveNonTx(srcInfo.id(), src.name(), srcParentInfo.id(), dest.name(), destParentInfo.id());
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
                            if (evts.isRecordable(EventType.EVT_IGFS_FILE_RENAMED))
                                pendingEvts.add(new IgfsEvent(
                                    src,
                                    destInfo == null ? dest : new IgfsPath(dest, src.name()),
                                    locNode,
                                    EventType.EVT_IGFS_FILE_RENAMED));
                        }
                        else if (evts.isRecordable(EventType.EVT_IGFS_DIR_RENAMED))
                            pendingEvts.add(new IgfsEvent(src, dest, locNode, EventType.EVT_IGFS_DIR_RENAMED));

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
     * Update path in DUAL mode.
     *
     * @param fs Secondary file system.
     * @param path Path to update.
     * @param props Properties to be applied.
     * @return Update file info.
     * @throws IgniteCheckedException If update failed.
     */
    public IgfsEntryInfo updateDual(final IgfsSecondaryFileSystem fs, final IgfsPath path,
        final Map<String, String> props) throws IgniteCheckedException {
        assert fs != null;
        assert path != null;
        assert props != null && !props.isEmpty();

        if (busyLock.enterBusy()) {
            try {
                SynchronizationTask<IgfsEntryInfo> task = new SynchronizationTask<IgfsEntryInfo>() {
                    @Override public IgfsEntryInfo onSuccess(Map<IgfsPath, IgfsEntryInfo> infos) throws Exception {
                        if (infos.get(path) == null)
                            return null;

                        fs.update(path, props);

                        return updatePropertiesNonTx(infos.get(path).id(), props);
                    }

                    @Override public IgfsEntryInfo onFailure(@Nullable Exception err) throws IgniteCheckedException {
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
    private IgfsEntryInfo synchronize(IgfsSecondaryFileSystem fs,
        IgfsPath startPath,
        IgfsEntryInfo startPathInfo,
        IgfsPath endPath,
        boolean strict,
        @Nullable Map<IgfsPath, IgfsEntryInfo> created)
        throws IgniteCheckedException
    {
        assert fs != null;
        assert startPath != null && startPathInfo != null && endPath != null;

        validTxState(true);

        IgfsEntryInfo parentInfo = startPathInfo;

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
                IgfsEntryInfo curInfo = status.isDirectory() ?
                    IgfsUtils.createDirectory(
                        IgniteUuid.randomUuid(),
                        null,
                        status.properties(),
                        status.accessTime(),
                        status.modificationTime()
                    ) :
                    IgfsUtils.createFile(
                        IgniteUuid.randomUuid(),
                        igfsCtx.configuration().getBlockSize(),
                        status.length(),
                        null,
                        null,
                        igfsCtx.igfs().evictExclude(curPath, false),
                        status.properties(),
                        status.accessTime(),
                        status.modificationTime()
                    );

                assert parentInfo != null;

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
    @SuppressWarnings({"Contract", "ConstantConditions"})
    private <T> T synchronizeAndExecute(SynchronizationTask<T> task, IgfsSecondaryFileSystem fs, boolean strict,
        @Nullable Collection<IgniteUuid> extraLockIds, IgfsPath... paths) throws IgniteCheckedException {
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
                pathIds.add(idsForPath(path));

            // Start pessimistic.
            try (IgniteInternalTx tx = startTx()) {
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

                Map<IgniteUuid, IgfsEntryInfo> idToInfo = lockIds(lockArr);

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
                        Map<IgfsPath, IgfsEntryInfo> infos = new HashMap<>();

                        TreeMap<IgfsPath, IgfsEntryInfo> created = new TreeMap<>();

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

                                IgfsEntryInfo info = synchronize(fs,
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
        }

        return res;
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
        return metaCache.txStartEx(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     * Update times.
     *
     * @param path Path.
     * @param accessTime Access time.
     * @param modificationTime Modification time.
     * @param secondaryFs Secondary file system.
     * @throws IgniteCheckedException If failed.
     */
    public void updateTimes(IgfsPath path, long accessTime, long modificationTime,
        IgfsSecondaryFileSystemV2 secondaryFs) throws IgniteCheckedException {
        while (true) {
            if (busyLock.enterBusy()) {
                try {
                    validTxState(false);

                    // Prepare path IDs.
                    IgfsPathIds pathIds = pathIds(path);

                    // Prepare lock IDs.
                    Set<IgniteUuid> lockIds = new TreeSet<>(PATH_ID_SORTING_COMPARATOR);

                    pathIds.addExistingIds(lockIds, relaxed);

                    // Start TX.
                    try (IgniteInternalTx tx = startTx()) {
                        Map<IgniteUuid, IgfsEntryInfo> lockInfos = lockIds(lockIds);

                        if (secondaryFs != null && isRetryForSecondary(pathIds, lockInfos))
                            continue;

                        if (!pathIds.verifyIntegrity(lockInfos, relaxed))
                            // Directory structure changed concurrently. So we re-try.
                            continue;

                        if (pathIds.allExists()) {
                            // All files are in place. Update both primary and secondary file systems.
                            if (secondaryFs != null)
                                secondaryFs.setTimes(path, accessTime, modificationTime);

                            IgniteUuid targetId = pathIds.lastExistingId();
                            IgfsEntryInfo targetInfo = lockInfos.get(targetId);

                            id2InfoPrj.invoke(targetId, new IgfsMetaUpdateTimesProcessor(
                                accessTime == -1 ? targetInfo.accessTime() : accessTime,
                                modificationTime == -1 ? targetInfo.modificationTime() : modificationTime)
                            );

                            tx.commit();

                            return;
                        }
                        else {
                            // Propagate call to the secondary FS, as we might haven't cache this part yet.
                            if (secondaryFs != null) {
                                secondaryFs.setTimes(path, accessTime, modificationTime);

                                return;
                            }
                            else
                                throw new IgfsPathNotFoundException("Failed to update times (path not found): " + path);
                        }
                    }
                }
                catch (IgniteException | IgniteCheckedException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw new IgniteCheckedException("setTimes failed due to unexpected exception: " + path, e);
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
            else
                throw new IllegalStateException("Failed to update times because Grid is stopping: " + path);
        }
    }

    /**
     * @param msg Error message.
     * @return Checked exception.
     */
    private static IgniteCheckedException fsException(String msg) {
        return new IgniteCheckedException(new IgfsException(msg));
    }

    /**
     * @param err Unchecked exception.
     * @return Checked exception.
     */
    private static IgniteCheckedException fsException(IgfsException err) {
        return new IgniteCheckedException(err);
    }

    /**
     * Append routine.
     *
     * @param path Path.
     * @param dirProps Directory properties.
     * @param create Create flag.
     * @param blockSize Block size.
     * @param affKey Affinity key.
     * @param evictExclude Evict exclude flag.
     * @param fileProps File properties.
     * @return Resulting info.
     * @throws IgniteCheckedException If failed.
     */
    IgfsEntryInfo append(
        final IgfsPath path,
        Map<String, String> dirProps,
        final boolean create,
        final int blockSize,
        final @Nullable IgniteUuid affKey,
        final boolean evictExclude,
        @Nullable Map<String, String> fileProps) throws IgniteCheckedException {
        validTxState(false);

        while (true) {
            if (busyLock.enterBusy()) {
                try {
                    // Prepare path IDs.
                    IgfsPathIds pathIds = pathIds(path);

                    // Fail-fast: create flag is not specified and some paths are missing.
                    if (!pathIds.allExists() && !create)
                        throw new IgfsPathNotFoundException("Failed to append because file is not found: " + path);

                    // Prepare lock IDs.
                    Set<IgniteUuid> lockIds = new TreeSet<>(PATH_ID_SORTING_COMPARATOR);

                    pathIds.addExistingIds(lockIds, relaxed);
                    pathIds.addSurrogateIds(lockIds);

                    // Start TX.
                    try (IgniteInternalTx tx = startTx()) {
                        Map<IgniteUuid, IgfsEntryInfo> lockInfos = lockIds(lockIds);

                        if (!pathIds.verifyIntegrity(lockInfos, relaxed))
                            // Directory structure changed concurrently. So we simply re-try.
                            continue;

                        if (pathIds.allExists()) {
                            // All participants are found. Simply open the stream.
                            IgfsEntryInfo info = lockInfos.get(pathIds.lastId());

                            // Check: is it a file?
                            if (!info.isFile())
                                throw new IgfsPathIsDirectoryException("Failed to open file for write." + path);

                            // Check if file already opened for write.
                            if (info.lockId() != null)
                                throw new IgfsException("File is already opened for write: " + path);

                            // At this point we can open the stream safely.
                            info = invokeLock(info.id(), false);

                            tx.commit();

                            IgfsUtils.sendEvents(igfsCtx.kernalContext(), path, EventType.EVT_IGFS_FILE_OPENED_WRITE);

                            return info;
                        }
                        else {
                            // Create file and parent folders.
                            IgfsPathsCreateResult res = createFile(pathIds, lockInfos, dirProps, fileProps, blockSize,
                                affKey, evictExclude, null, null);

                            if (res == null)
                                continue;

                            // Commit.
                            tx.commit();

                            // Generate events.
                            generateCreateEvents(res.createdPaths(), true);

                            return res.info();
                        }
                    }
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
            else
                throw new IllegalStateException("Failed to append for file because Grid is stopping:" + path);
        }
    }

    /**
     * Create a file.
     *
     * @param path Path.
     * @param dirProps Directory properties.
     * @param overwrite Overwrite flag.
     * @param blockSize Block size.
     * @param affKey Affinity key.
     * @param evictExclude Evict exclude flag.
     * @param fileProps File properties.
     * @param secondaryCtx Secondary file system create context.
     * @return @return Operation result.
     * @throws IgniteCheckedException If failed.
     */
    IgfsCreateResult create(
        final IgfsPath path,
        Map<String, String> dirProps,
        final boolean overwrite,
        final int blockSize,
        final @Nullable IgniteUuid affKey,
        final boolean evictExclude,
        @Nullable Map<String, String> fileProps,
        @Nullable IgfsSecondaryFileSystemCreateContext secondaryCtx) throws IgniteCheckedException {
        validTxState(false);

        while (true) {
            if (busyLock.enterBusy()) {
                OutputStream secondaryOut = null;

                try {
                    // Prepare path IDs.
                    IgfsPathIds pathIds = pathIds(path);

                    // Prepare lock IDs.
                    Set<IgniteUuid> lockIds = new TreeSet<>(PATH_ID_SORTING_COMPARATOR);

                    pathIds.addExistingIds(lockIds, relaxed);
                    pathIds.addSurrogateIds(lockIds);

                    // In overwrite mode we also lock ID of potential replacement as well as trash ID.
                    IgniteUuid overwriteId = IgniteUuid.randomUuid();
                    IgniteUuid trashId = IgfsUtils.randomTrashId();

                    if (overwrite) {
                        lockIds.add(overwriteId);

                        // Trash ID is only added if we suspect conflict.
                        if (pathIds.allExists())
                            lockIds.add(trashId);
                    }

                    // Start TX.
                    try (IgniteInternalTx tx = startTx()) {
                        Map<IgniteUuid, IgfsEntryInfo> lockInfos = lockIds(lockIds);

                        if (secondaryCtx != null && isRetryForSecondary(pathIds, lockInfos))
                            continue;

                        if (!pathIds.verifyIntegrity(lockInfos, relaxed))
                            // Directory structure changed concurrently. So we simply re-try.
                            continue;

                        if (pathIds.allExists()) {
                            // All participants found.
                            IgfsEntryInfo oldInfo = lockInfos.get(pathIds.lastId());

                            // Check: is it a file?
                            if (!oldInfo.isFile())
                                throw new IgfsPathIsDirectoryException("Failed to create a file: " + path);

                            // Check: can we overwrite it?
                            if (!overwrite)
                                throw new IgfsPathAlreadyExistsException("Failed to create a file: " + path);

                            // Check if file already opened for write.
                            if (oldInfo.lockId() != null)
                                throw new IgfsException("File is already opened for write: " + path);

                            // At this point file can be re-created safely.

                            // Add existing to trash listing.
                            IgniteUuid oldId = pathIds.lastId();

                            id2InfoPrj.invoke(trashId, new IgfsMetaDirectoryListingAddProcessor(
                                IgfsUtils.composeNameForTrash(path, oldId), new IgfsListingEntry(oldInfo)));

                            // Replace ID in parent directory.
                            String name = pathIds.lastPart();
                            IgniteUuid parentId = pathIds.lastParentId();

                            id2InfoPrj.invoke(parentId, new IgfsMetaDirectoryListingReplaceProcessor(name, overwriteId));

                            // Create the file.
                            IgniteUuid newLockId = createFileLockId(false);

                            long newAccessTime;
                            long newModificationTime;
                            Map<String, String> newProps;
                            long newLen;
                            int newBlockSize;

                            if (secondaryCtx != null) {
                                secondaryOut = secondaryCtx.create();

                                newAccessTime = 0L;
                                newModificationTime = 0L;
                                newProps = null;
                            }
                            else {
                                newAccessTime = System.currentTimeMillis();
                                newModificationTime = newAccessTime;
                                newProps = fileProps;
                            }

                            newLen = 0L;
                            newBlockSize = blockSize;

                            IgfsEntryInfo newInfo = invokeAndGet(overwriteId,
                                new IgfsMetaFileCreateProcessor(newAccessTime, newModificationTime, newProps,
                                    newBlockSize, affKey, newLockId, evictExclude, newLen));

                            // Prepare result and commit.
                            tx.commit();

                            IgfsUtils.sendEvents(igfsCtx.kernalContext(), path, EventType.EVT_IGFS_FILE_OPENED_WRITE);

                            return new IgfsCreateResult(newInfo, secondaryOut);
                        }
                        else {
                            // Create file and parent folders.
                            T1<OutputStream> secondaryOutHolder = null;

                            if (secondaryCtx != null)
                                secondaryOutHolder = new T1<>();

                            IgfsPathsCreateResult res;

                            try {
                                res = createFile(pathIds, lockInfos, dirProps, fileProps, blockSize,
                                    affKey, evictExclude, secondaryCtx, secondaryOutHolder);
                            }
                            finally {
                                if (secondaryOutHolder != null)
                                    secondaryOut =  secondaryOutHolder.get();
                            }

                            if (res == null)
                                continue;

                            // Commit.
                            tx.commit();

                            // Generate events.
                            generateCreateEvents(res.createdPaths(), true);

                            return new IgfsCreateResult(res.info(), secondaryOut);
                        }
                    }
                }
                catch (IgniteException | IgniteCheckedException e) {
                    U.closeQuiet(secondaryOut);

                    throw e;
                }
                catch (Exception e) {
                    U.closeQuiet(secondaryOut);

                    throw new IgniteCheckedException("Create failed due to unexpected exception: " + path, e);
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
            else
                throw new IllegalStateException("Failed to mkdir because Grid is stopping. [path=" + path + ']');
        }
    }

    /**
     * Create directory and it's parents.
     *
     * @param pathIds Path IDs.
     * @param lockInfos Lock infos.
     * @param dirProps Directory properties.
     * @return Result or {@code} if the first parent already contained child with the same name.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable IgfsPathsCreateResult createDirectory(IgfsPathIds pathIds, Map<IgniteUuid, IgfsEntryInfo> lockInfos,
        Map<String, String> dirProps) throws IgniteCheckedException {
        // Check if entry we are going to write to is directory.
        if (lockInfos.get(pathIds.lastExistingId()).isFile())
            throw new IgfsParentNotDirectoryException("Failed to create directory (parent " +
                "element is not a directory)");

        return createFileOrDirectory(true, pathIds, lockInfos, dirProps, null, 0, null, false, null, null);
    }

    /**
     * Create file and all it's parents.
     *
     * @param pathIds Paths IDs.
     * @param lockInfos Lock infos.
     * @param dirProps Directory properties.
     * @param fileProps File propertris.
     * @param blockSize Block size.
     * @param affKey Affinity key (optional)
     * @param evictExclude Evict exclude flag.
     * @param secondaryCtx Secondary file system create context.
     * @param secondaryOutHolder Holder for the secondary output stream.
     * @return Result or {@code} if the first parent already contained child with the same name.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private IgfsPathsCreateResult createFile(IgfsPathIds pathIds, Map<IgniteUuid, IgfsEntryInfo> lockInfos,
        Map<String, String> dirProps, Map<String, String> fileProps, int blockSize, @Nullable IgniteUuid affKey,
        boolean evictExclude, @Nullable IgfsSecondaryFileSystemCreateContext secondaryCtx,
        @Nullable T1<OutputStream> secondaryOutHolder)
        throws IgniteCheckedException{
        // Check if entry we are going to write to is directory.
        if (lockInfos.get(pathIds.lastExistingId()).isFile())
            throw new IgfsParentNotDirectoryException("Failed to open file for write " +
                "(parent element is not a directory): " + pathIds.path());

        return createFileOrDirectory(false, pathIds, lockInfos, dirProps, fileProps, blockSize, affKey, evictExclude,
            secondaryCtx, secondaryOutHolder);
    }

    /**
     * Create file or directory.
     *
     * @param dir Directory flag.
     * @param pathIds Path IDs.
     * @param lockInfos Lock infos.
     * @param dirProps Directory properties.
     * @param fileProps File properties.
     * @param blockSize Block size.
     * @param affKey Affinity key.
     * @param evictExclude Evict exclude flag.
     * @param secondaryCtx Secondary file system create context.
     * @param secondaryOutHolder Secondary output stream holder.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private IgfsPathsCreateResult createFileOrDirectory(boolean dir, IgfsPathIds pathIds,
        Map<IgniteUuid, IgfsEntryInfo> lockInfos, Map<String, String> dirProps, Map<String, String> fileProps,
        int blockSize, @Nullable IgniteUuid affKey, boolean evictExclude,
        @Nullable IgfsSecondaryFileSystemCreateContext secondaryCtx, @Nullable T1<OutputStream> secondaryOutHolder)
        throws IgniteCheckedException {
        // This is our starting point.
        int lastExistingIdx = pathIds.lastExistingIndex();
        IgfsEntryInfo lastExistingInfo = lockInfos.get(pathIds.lastExistingId());

        // If current info already contains entry with the same name as it's child, then something
        // has changed concurrently. We must re-try because we cannot get info of this unexpected
        // element due to possible deadlocks.
        int curIdx = lastExistingIdx + 1;

        String curPart = pathIds.part(curIdx);
        IgniteUuid curId = pathIds.surrogateId(curIdx);

        if (lastExistingInfo.hasChild(curPart))
            return null;

        // Create entry in the secondary file system if needed.
        if (secondaryCtx != null) {
            assert secondaryOutHolder != null;

            secondaryOutHolder.set(secondaryCtx.create());
        }

        Map<IgniteUuid, EntryProcessor> procMap = new HashMap<>();

        // First step: add new entry to the last existing element.
        procMap.put(lastExistingInfo.id(), new IgfsMetaDirectoryListingAddProcessor(curPart,
            new IgfsListingEntry(curId, dir || !pathIds.isLastIndex(curIdx))));

        // Events support.
        IgfsPath lastCreatedPath = pathIds.lastExistingPath();

        List<IgfsPath> createdPaths = new ArrayList<>(pathIds.count() - curIdx);

        // Second step: create middle directories.
        long curTime = System.currentTimeMillis();

        while (curIdx < pathIds.count() - 1) {
            lastCreatedPath = new IgfsPath(lastCreatedPath, curPart);

            int nextIdx = curIdx + 1;

            String nextPart = pathIds.part(nextIdx);
            IgniteUuid nextId = pathIds.surrogateId(nextIdx);

            long accessTime;
            long modificationTime;
            Map<String, String> props;

            if (secondaryCtx != null) {
                accessTime = 0L;
                modificationTime = 0L;
                props = null;
            }
            else {
                accessTime = curTime;
                modificationTime = curTime;
                props = dirProps;
            }

            procMap.put(curId, new IgfsMetaDirectoryCreateProcessor(accessTime, modificationTime, props,
                nextPart, new IgfsListingEntry(nextId, dir || !pathIds.isLastIndex(nextIdx))));

            // Save event.
            createdPaths.add(lastCreatedPath);

            // Advance things further.
            curIdx++;

            curPart = nextPart;
            curId = nextId;
        }

        // Third step: create leaf.
        if (dir) {
            long accessTime;
            long modificationTime;
            Map<String, String> props;

            if (secondaryCtx != null) {
                accessTime = 0L;
                modificationTime = 0L;
                props = null;
            }
            else {
                accessTime = curTime;
                modificationTime = curTime;
                props = dirProps;
            }

            procMap.put(curId, new IgfsMetaDirectoryCreateProcessor(accessTime, modificationTime, props));
        }
        else {
            long newAccessTime;
            long newModificationTime;
            Map<String, String> newProps;
            long newLen;
            int newBlockSize;

            if (secondaryCtx != null) {
                newAccessTime = 0L;
                newModificationTime = 0L;
                newProps = null;
            }
            else {
                newAccessTime = curTime;
                newModificationTime = curTime;
                newProps = fileProps;
            }

            newLen = 0L;
            newBlockSize = blockSize;

            procMap.put(curId, new IgfsMetaFileCreateProcessor(newAccessTime, newModificationTime, newProps,
                newBlockSize, affKey, createFileLockId(false), evictExclude, newLen));
        }

        createdPaths.add(pathIds.path());

        // Execute cache operations.
        Map<Object, EntryProcessorResult> invokeRes = ((IgniteInternalCache)id2InfoPrj).invokeAll(procMap);

        IgfsEntryInfo info = (IgfsEntryInfo)invokeRes.get(curId).get();

        return new IgfsPathsCreateResult(createdPaths, info);
    }

    /**
     * Generate events for created file or directory.
     *
     * @param createdPaths Created paths.
     * @param file Whether file was created.
     */
    private void generateCreateEvents(List<IgfsPath> createdPaths, boolean file) {
        if (evts.isRecordable(EventType.EVT_IGFS_DIR_CREATED)) {
            for (int i = 0; i < createdPaths.size() - 1; i++)
                IgfsUtils.sendEvents(igfsCtx.kernalContext(), createdPaths.get(i),
                    EventType.EVT_IGFS_DIR_CREATED);
        }

        IgfsPath leafPath = createdPaths.get(createdPaths.size() - 1);

        if (file) {
            IgfsUtils.sendEvents(igfsCtx.kernalContext(), leafPath, EventType.EVT_IGFS_FILE_CREATED);
            IgfsUtils.sendEvents(igfsCtx.kernalContext(), leafPath, EventType.EVT_IGFS_FILE_OPENED_WRITE);
        }
        else
            IgfsUtils.sendEvents(igfsCtx.kernalContext(), leafPath, EventType.EVT_IGFS_DIR_CREATED);
    }

    /**
     * Signal delete worker thread.
     */
    private void signalDeleteWorker() {
        IgfsDeleteWorker delWorker0 = delWorker;

        if (delWorker0 != null)
            delWorker0.signal();
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
        public T onSuccess(Map<IgfsPath, IgfsEntryInfo> infos) throws Exception;

        /**
         * Callback handler in case synchronization failed.
         *
         * @param err Optional exception.
         * @return Task result.
         * @throws IgniteCheckedException In case exception is to be thrown in that case.
         */
        public T onFailure(Exception err) throws IgniteCheckedException;
    }
}