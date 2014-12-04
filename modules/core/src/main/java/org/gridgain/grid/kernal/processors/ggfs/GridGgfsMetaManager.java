/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.kernal.processors.ggfs.GridGgfsFileInfo.*;

/**
 * Cache based structure (meta data) manager.
 */
@SuppressWarnings("all")
public class GridGgfsMetaManager extends GridGgfsManager {
    /** GGFS configuration. */
    private GridGgfsConfiguration cfg;

    /** Metadata cache. */
    private GridCache<Object, Object> metaCache;

    /** */
    private GridFuture<?> metaCacheStartFut;

    /** File ID to file info projection. */
    private GridCacheProjectionEx<IgniteUuid, GridGgfsFileInfo> id2InfoPrj;

    /** Predefined key for sampling mode value. */
    private GridCacheInternal sampling;

    /** Logger. */
    private GridLogger log;

    /** Delete worker. */
    private volatile GridGgfsDeleteWorker delWorker;

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
        if (!metaCacheStartFut.isDone()) {
            try {
                metaCacheStartFut.get();
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws GridException {
        cfg = ggfsCtx.configuration();

        metaCache = ggfsCtx.kernalContext().cache().cache(cfg.getMetaCacheName());

        metaCacheStartFut = ggfsCtx.kernalContext().cache().internalCache(cfg.getMetaCacheName()).preloader()
            .startFuture();

        if (metaCache.configuration().getAtomicityMode() != TRANSACTIONAL)
            throw new GridException("Meta cache should be transactional: " + cfg.getMetaCacheName());

        evts = ggfsCtx.kernalContext().event();

        sampling = new GridGgfsSamplingKey(cfg.getName());

        assert metaCache != null;

        id2InfoPrj = (GridCacheProjectionEx<IgniteUuid, GridGgfsFileInfo>)metaCache.<IgniteUuid, GridGgfsFileInfo>cache();

        log = ggfsCtx.kernalContext().log(GridGgfsMetaManager.class);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws GridException {
        locNode = ggfsCtx.kernalContext().discovery().localNode();

        // Start background delete worker.
        delWorker = new GridGgfsDeleteWorker(ggfsCtx);

        delWorker.start();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        GridGgfsDeleteWorker delWorker0 = delWorker;

        if (delWorker0 != null)
            delWorker0.cancel();

        if (delWorker0 != null) {
            try {
                U.join(delWorker0);
            }
            catch (GridInterruptedException ignored) {
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
                return ggfsCtx.kernalContext().discovery().cacheNodes(metaCache.name(), -1);
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
     * @throws GridException If failed.
     */
    @Nullable public IgniteUuid fileId(GridGgfsPath path) throws GridException {
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
     * @throws GridException If failed.
     */
    @Nullable private IgniteUuid fileId(GridGgfsPath path, boolean skipTx) throws GridException {
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
     * @throws GridException If failed.
     */
    @Nullable public IgniteUuid fileId(IgniteUuid parentId, String fileName) throws GridException {
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
     * @throws GridException If failed.
     */
    @Nullable private IgniteUuid fileId(IgniteUuid parentId, String fileName, boolean skipTx) throws GridException {
        GridGgfsListingEntry entry = directoryListing(parentId, skipTx).get(fileName);

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
     * @throws GridException If failed.
     */
    public List<IgniteUuid> fileIds(GridGgfsPath path) throws GridException {
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
     * @throws GridException If failed.
     */
    private List<IgniteUuid> fileIds(GridGgfsPath path, boolean skipTx) throws GridException {
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
     * @throws GridException IF failed.
     */
    public boolean exists(IgniteUuid fileId) throws GridException{
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
     * @throws GridException If failed.
     */
    @Nullable public GridGgfsFileInfo info(@Nullable IgniteUuid fileId) throws GridException {
        if (busyLock.enterBusy()) {
            try {
                if (fileId == null)
                    return null;

                GridGgfsFileInfo info = id2InfoPrj.get(fileId);

                // Force root ID always exist in cache.
                if (info == null && ROOT_ID.equals(fileId))
                    id2InfoPrj.putxIfAbsent(ROOT_ID, info = new GridGgfsFileInfo());

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
     * @throws GridException If failed.
     */
    public Map<IgniteUuid, GridGgfsFileInfo> infos(Collection<IgniteUuid> fileIds) throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);
                assert fileIds != null;

                if (F.isEmpty(fileIds))
                    return Collections.emptyMap();

                Map<IgniteUuid, GridGgfsFileInfo> map = id2InfoPrj.getAll(fileIds);

                // Force root ID always exist in cache.
                if (fileIds.contains(ROOT_ID) && !map.containsKey(ROOT_ID)) {
                    GridGgfsFileInfo info = new GridGgfsFileInfo();

                    id2InfoPrj.putxIfAbsent(ROOT_ID, info);

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
     * @throws GridException If failed.
     */
    public GridGgfsFileInfo lock(IgniteUuid fileId) throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);
                assert fileId != null;

                GridCacheTx tx = metaCache.txStart(PESSIMISTIC, REPEATABLE_READ);

                try {
                    // Lock file ID for this transaction.
                    GridGgfsFileInfo oldInfo = info(fileId);

                    if (oldInfo == null)
                        throw new GridException("Failed to lock file (file not found): " + fileId);

                    GridGgfsFileInfo newInfo = lockInfo(oldInfo);

                    boolean put = metaCache.putx(fileId, newInfo);

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
     * @throws GridException In case lock is already set on that file.
     */
    public GridGgfsFileInfo lockInfo(GridGgfsFileInfo info) throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert info != null;

                if (info.lockId() != null)
                    throw new GridException("Failed to lock file (file is being concurrently written) [fileId=" +
                        info.id() + ", lockId=" + info.lockId() + ']');

                return new GridGgfsFileInfo(info, IgniteUuid.randomUuid(), info.modificationTime());
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
     * @throws GridException If failed.
     */
    public void unlock(GridGgfsFileInfo info, long modificationTime) throws GridException {
        assert validTxState(false);
        assert info != null;

        if (busyLock.enterBusy()) {
            try {
                IgniteUuid lockId = info.lockId();

                if (lockId == null)
                    return;

                // Temporary clear interrupted state for unlocking.
                boolean interrupted = Thread.interrupted();

                IgniteUuid fileId = info.id();

                GridCacheTx tx = metaCache.txStart(PESSIMISTIC, REPEATABLE_READ);

                try {
                    // Lock file ID for this transaction.
                    GridGgfsFileInfo oldInfo = info(fileId);

                    if (oldInfo == null)
                        throw new GridGgfsFileNotFoundException("Failed to unlock file (file not found): " + fileId);

                    if (!info.lockId().equals(oldInfo.lockId()))
                        throw new GridException("Failed to unlock file (inconsistent file lock ID) [fileId=" + fileId +
                            ", lockId=" + info.lockId() + ", actualLockId=" + oldInfo.lockId() + ']');

                    GridGgfsFileInfo newInfo = new GridGgfsFileInfo(oldInfo, null, modificationTime);

                    boolean put = metaCache.putx(fileId, newInfo);

                    assert put : "Value was not stored in cache [fileId=" + fileId + ", newInfo=" + newInfo + ']';

                    tx.commit();
                }
                catch (GridClosureException e) {
                    throw U.cast(e);
                }
                finally {
                    tx.close();

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
     * @throws GridException If failed.
     */
    private Map<IgniteUuid, GridGgfsFileInfo> lockIds(IgniteUuid... fileIds) throws GridException {
        assert validTxState(true);
        assert fileIds != null && fileIds.length > 0;

        // Always sort file IDs participating in transaction to escape cache transaction deadlocks.
        Arrays.sort(fileIds);

        // Wrap array as collection (1) to escape superfluous check in projection and (2) to check assertions.
        Collection<IgniteUuid> keys = Arrays.asList(fileIds);

        if (log.isDebugEnabled())
            log.debug("Locking file ids: " + keys);

        // Lock files and get their infos.
        Map<IgniteUuid, GridGgfsFileInfo> map = id2InfoPrj.getAll(keys);

        if (log.isDebugEnabled())
            log.debug("Locked file ids: " + keys);

        // Force root ID always exist in cache.
        if (keys.contains(ROOT_ID) && !map.containsKey(ROOT_ID)) {
            GridGgfsFileInfo info = new GridGgfsFileInfo();

            id2InfoPrj.putxIfAbsent(ROOT_ID, info);

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
     * @throws GridException If failed.
     */
    public Map<String, GridGgfsListingEntry> directoryListing(IgniteUuid fileId) throws GridException {
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
     * @throws GridException If failed to get file for fragmentizer.
     */
    public GridGgfsFileInfo fileForFragmentizer(Collection<IgniteUuid> exclude) throws GridException {
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
     * @throws GridException If failed to get file for fragmentizer.
     */
    private GridGgfsFileInfo fileForFragmentizer0(IgniteUuid parentId, Collection<IgniteUuid> exclude)
        throws GridException {
        GridGgfsFileInfo info = info(parentId);

        // Check if file was concurrently deleted.
        if (info == null)
            return null;

        assert info.isDirectory();

        Map<String, GridGgfsListingEntry> listing = info.listing();

        for (GridGgfsListingEntry entry : listing.values()) {
            if (entry.isFile()) {
                GridGgfsFileInfo fileInfo = info(entry.fileId());

                if (fileInfo != null) {
                    if (!exclude.contains(fileInfo.id()) &&
                        fileInfo.fileMap() != null &&
                        !fileInfo.fileMap().ranges().isEmpty())
                        return fileInfo;
                }
            }
            else {
                GridGgfsFileInfo fileInfo = fileForFragmentizer0(entry.fileId(), exclude);

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
     * @throws GridException If failed.
     */
    private Map<String, GridGgfsListingEntry> directoryListing(IgniteUuid fileId, boolean skipTx) throws GridException {
        assert fileId != null;

        GridGgfsFileInfo info = skipTx ? id2InfoPrj.getAllOutTx(Collections.singletonList(fileId)).get(fileId) :
            id2InfoPrj.get(fileId);

        return info == null ? Collections.<String, GridGgfsListingEntry>emptyMap() : info.listing();
    }

    /**
     * Add file into file system structure.
     *
     * @param parentId Parent file ID.
     * @param fileName File name in the parent's listing.
     * @param newFileInfo File info to store in the parent's listing.
     * @return File id already stored in meta cache or {@code null} if passed file info was stored.
     * @throws GridException If failed.
     */
    public IgniteUuid putIfAbsent(IgniteUuid parentId, String fileName, GridGgfsFileInfo newFileInfo)
        throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);
                assert parentId != null;
                assert fileName != null;
                assert newFileInfo != null;

                IgniteUuid res = null;

                GridCacheTx tx = metaCache.txStart(PESSIMISTIC, REPEATABLE_READ);

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
     * @throws GridException If failed.
     */
    private IgniteUuid putIfAbsentNonTx(IgniteUuid parentId, String fileName, GridGgfsFileInfo newFileInfo)
        throws GridException {
        if (log.isDebugEnabled())
            log.debug("Locking parent id [parentId=" + parentId + ", fileName=" + fileName + ", newFileInfo=" +
                newFileInfo + ']');

        validTxState(true);

        // Lock only parent file ID.
        GridGgfsFileInfo parentInfo = info(parentId);

        assert validTxState(true);

        if (parentInfo == null)
            throw new GridGgfsFileNotFoundException("Failed to lock parent directory (not found): " + parentId);

        if (!parentInfo.isDirectory())
            throw new GridGgfsInvalidPathException("Parent file is not a directory: " + parentInfo);

        Map<String, GridGgfsListingEntry> parentListing = parentInfo.listing();

        assert parentListing != null;

        GridGgfsListingEntry entry = parentListing.get(fileName);

        assert validTxState(true);

        if (entry != null)
            return entry.fileId();

        IgniteUuid fileId = newFileInfo.id();

        if (!id2InfoPrj.putxIfAbsent(fileId, newFileInfo))
            throw new GridGgfsException("Failed to add file details into cache: " + newFileInfo);

        assert metaCache.get(parentId) != null;

        id2InfoPrj.transform(parentId, new UpdateListing(fileName, new GridGgfsListingEntry(newFileInfo), false));

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
     * @throws GridException If failed.
     */
    public void move(IgniteUuid fileId, String srcFileName, IgniteUuid srcParentId, String destFileName,
        IgniteUuid destParentId) throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                GridCacheTx tx = metaCache.txStart(PESSIMISTIC, REPEATABLE_READ);

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
     * @throws GridException If failed.
     */
    private void moveNonTx(IgniteUuid fileId, @Nullable String srcFileName, IgniteUuid srcParentId, String destFileName,
        IgniteUuid destParentId) throws GridException {
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
        Map<IgniteUuid, GridGgfsFileInfo> infoMap = lockIds(srcParentId, fileId, destParentId);

        validTxState(true);

        GridGgfsFileInfo srcInfo = infoMap.get(srcParentId);

        if (srcInfo == null)
            throw new GridGgfsFileNotFoundException("Failed to lock source directory (not found?)" +
                " [srcParentId=" + srcParentId + ']');

        if (!srcInfo.isDirectory())
            throw new GridGgfsInvalidPathException("Source is not a directory: " + srcInfo);

        GridGgfsFileInfo destInfo = infoMap.get(destParentId);

        if (destInfo == null)
            throw new GridGgfsFileNotFoundException("Failed to lock destination directory (not found?)" +
                " [destParentId=" + destParentId + ']');

        if (!destInfo.isDirectory())
            throw new GridGgfsInvalidPathException("Destination is not a directory: " + destInfo);

        GridGgfsFileInfo fileInfo = infoMap.get(fileId);

        if (fileInfo == null)
            throw new GridGgfsFileNotFoundException("Failed to lock target file (not found?) [fileId=" +
                fileId + ']');

        GridGgfsListingEntry srcEntry = srcInfo.listing().get(srcFileName);
        GridGgfsListingEntry destEntry = destInfo.listing().get(destFileName);

        // If source file does not exist or was re-created.
        if (srcEntry == null || !srcEntry.fileId().equals(fileId))
            throw new GridGgfsFileNotFoundException("Failed to remove file name from the source directory" +
                " (file not found) [fileId=" + fileId + ", srcFileName=" + srcFileName +
                ", srcParentId=" + srcParentId + ", srcEntry=" + srcEntry + ']');

        // If stored file already exist.
        if (destEntry != null)
            throw new GridGgfsInvalidPathException("Failed to add file name into the destination directory " +
                "(file already exists) [fileId=" + fileId + ", destFileName=" + destFileName +
                ", destParentId=" + destParentId + ", destEntry=" + destEntry + ']');

        assert metaCache.get(srcParentId) != null;
        assert metaCache.get(destParentId) != null;

        // Remove listing entry from the source parent listing.
        id2InfoPrj.transform(srcParentId, new UpdateListing(srcFileName, srcEntry, true));

        // Add listing entry into the destination parent listing.
        id2InfoPrj.transform(destParentId, new UpdateListing(destFileName, srcEntry, false));
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
     * @throws GridException If failed.
     */
    @Nullable public GridGgfsFileInfo removeIfEmpty(IgniteUuid parentId, String fileName, IgniteUuid fileId,
        GridGgfsPath path, boolean rmvLocked)
        throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                GridCacheTx tx = metaCache.txStart(PESSIMISTIC, REPEATABLE_READ);

                try {
                    if (parentId != null)
                        lockIds(parentId, fileId, TRASH_ID);
                    else
                        lockIds(fileId, TRASH_ID);

                    GridGgfsFileInfo fileInfo = removeIfEmptyNonTx(parentId, fileName, fileId, path, rmvLocked);

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
     * @throws GridException If failed.
     */
    @Nullable private GridGgfsFileInfo removeIfEmptyNonTx(@Nullable IgniteUuid parentId, String fileName, IgniteUuid fileId,
        GridGgfsPath path, boolean rmvLocked)
        throws GridException {
        assert validTxState(true);
        assert parentId != null;
        assert fileName != null;
        assert fileId != null;
        assert !ROOT_ID.equals(fileId);

        if (log.isDebugEnabled())
            log.debug("Remove file: [parentId=" + parentId + ", fileName= " + fileName + ", fileId=" + fileId + ']');

        // Safe gets because locks are obtained in removeIfEmpty.
        GridGgfsFileInfo fileInfo = id2InfoPrj.get(fileId);
        GridGgfsFileInfo parentInfo = id2InfoPrj.get(parentId);

        if (fileInfo == null || parentInfo == null) {
            if (parentInfo != null) { // fileInfo == null
                GridGgfsListingEntry entry = parentInfo.listing().get(fileName);

                // If file info does not exists but listing entry exists, throw inconsistent exception.
                if (entry != null && entry.fileId().equals(fileId))
                    throw new GridException("Failed to remove file (file system is in inconsistent state) " +
                        "[fileInfo=" + fileInfo + ", fileName=" + fileName + ", fileId=" + fileId + ']');
            }

            return null; // Parent directory or removed file cannot be locked (not found?).
        }

        assert parentInfo.isDirectory();

        if (!rmvLocked && fileInfo.lockId() != null)
            throw new GridGgfsException("Failed to remove file (file is opened for writing) [fileName=" +
                fileName + ", fileId=" + fileId + ", lockId=" + fileInfo.lockId() + ']');

        // Validate own directory listing.
        if (fileInfo.isDirectory()) {
            Map<String, GridGgfsListingEntry> listing = fileInfo.listing();

            if (!F.isEmpty(listing))
                throw new GridGgfsDirectoryNotEmptyException("Failed to remove file (directory is not empty)" +
                    " [fileId=" + fileId + ", listing=" + listing + ']');
        }

        // Validate file in the parent listing.
        GridGgfsListingEntry listingEntry = parentInfo.listing().get(fileName);

        if (listingEntry == null || !listingEntry.fileId().equals(fileId))
            return null;

        // Actual remove.
        softDeleteNonTx(parentId, fileName, fileId);

        // Update a file info of the removed file with a file path,
        // which will be used by delete worker for event notifications.
        id2InfoPrj.transform(fileId, new UpdatePath(path));

        return GridGgfsFileInfo.builder(fileInfo).path(path).build();
    }

    /**
     * Move path to the trash directory.
     *
     * @param parentId Parent ID.
     * @param pathName Path name.
     * @param pathId Path ID.
     * @return ID of an entry located directly under the trash directory.
     * @throws GridException If failed.
     */
    IgniteUuid softDelete(@Nullable IgniteUuid parentId, @Nullable String pathName, IgniteUuid pathId) throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                GridCacheTx tx = metaCache.txStart(PESSIMISTIC, REPEATABLE_READ);

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
     * @throws GridException If failed.
     */
    @Nullable private IgniteUuid softDeleteNonTx(@Nullable IgniteUuid parentId, @Nullable String name, IgniteUuid id)
        throws GridException {
        assert validTxState(true);

        IgniteUuid resId;

        if (parentId == null) {
            // Handle special case when we deleting root directory.
            assert ROOT_ID.equals(id);

            GridGgfsFileInfo rootInfo = id2InfoPrj.get(ROOT_ID);

            if (rootInfo == null)
                return null; // Root was never created.

            // Ensure trash directory existence.
            if (id2InfoPrj.get(TRASH_ID) == null)
                id2InfoPrj.put(TRASH_ID, new GridGgfsFileInfo(TRASH_ID));

            Map<String, GridGgfsListingEntry> rootListing = rootInfo.listing();

            if (!rootListing.isEmpty()) {
                IgniteUuid[] lockIds = new IgniteUuid[rootInfo.listing().size()];

                int i = 0;

                for (GridGgfsListingEntry entry : rootInfo.listing().values())
                    lockIds[i++] = entry.fileId();

                // Lock children IDs in correct order.
                lockIds(lockIds);

                // Construct new info and move locked entries from root to it.
                Map<String, GridGgfsListingEntry> transferListing = new HashMap<>();

                transferListing.putAll(rootListing);

                GridGgfsFileInfo newInfo = new GridGgfsFileInfo(transferListing);

                id2InfoPrj.put(newInfo.id(), newInfo);

                // Add new info to trash listing.
                id2InfoPrj.transform(TRASH_ID, new UpdateListing(newInfo.id().toString(),
                    new GridGgfsListingEntry(newInfo), false));

                // Remove listing entries from root.
                for (Map.Entry<String, GridGgfsListingEntry> entry : transferListing.entrySet())
                    id2InfoPrj.transform(ROOT_ID, new UpdateListing(entry.getKey(), entry.getValue(), true));

                resId = newInfo.id();
            }
            else
                resId = null;
        }
        else {
            // Ensure trash directory existence.
            if (id2InfoPrj.get(TRASH_ID) == null)
                id2InfoPrj.put(TRASH_ID, new GridGgfsFileInfo(TRASH_ID));

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
     * @throws GridException If failed.
     */
    Collection<IgniteUuid> delete(IgniteUuid parentId, Map<String, GridGgfsListingEntry> listing)
        throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert parentId != null;
                assert listing != null;
                assert validTxState(false);

                GridCacheTx tx = metaCache.txStart(PESSIMISTIC, REPEATABLE_READ);

                try {
                    Collection<IgniteUuid> res = new HashSet<>();

                    // Obtain all necessary locks in one hop.
                    IgniteUuid[] allIds = new IgniteUuid[listing.size() + 1];

                    allIds[0] = parentId;

                    int i = 1;

                    for (GridGgfsListingEntry entry : listing.values())
                        allIds[i++] = entry.fileId();

                    Map<IgniteUuid, GridGgfsFileInfo> locks = lockIds(allIds);

                    GridGgfsFileInfo parentInfo = locks.get(parentId);

                    // Ensure parent is still in place.
                    if (parentInfo != null) {
                        Map<String, GridGgfsListingEntry> newListing =
                            new HashMap<>(parentInfo.listing().size(), 1.0f);

                        newListing.putAll(parentInfo.listing());

                        // Remove child entries if possible.
                        for (Map.Entry<String, GridGgfsListingEntry> entry : listing.entrySet()) {
                            IgniteUuid entryId = entry.getValue().fileId();

                            GridGgfsFileInfo entryInfo = locks.get(entryId);

                            if (entryInfo != null) {
                                // Delete only files or empty folders.
                                if (entryInfo.isFile() || entryInfo.isDirectory() && entryInfo.listing().isEmpty()) {
                                    id2InfoPrj.remove(entryId);

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
                        id2InfoPrj.putx(parentId, new GridGgfsFileInfo(newListing, parentInfo));
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
     * @throws GridException If failed.
     */
    boolean delete(IgniteUuid parentId, String name, IgniteUuid id) throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                GridCacheTx tx = metaCache.txStart(PESSIMISTIC, REPEATABLE_READ);

                try {
                    boolean res = false;

                    Map<IgniteUuid, GridGgfsFileInfo> infos = lockIds(parentId, id);

                    // Proceed only in case both parent and child exist.
                    if (infos.containsKey(parentId) && infos.containsKey(id)) {
                        GridGgfsFileInfo parentInfo = infos.get(parentId);

                        assert parentInfo != null;

                        GridGgfsListingEntry listingEntry = parentInfo.listing().get(name);

                        if (listingEntry != null)
                            id2InfoPrj.transform(parentId, new UpdateListing(name, listingEntry, true));

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
     * @throws GridException If operation failed.
     */
    public Collection<IgniteUuid> pendingDeletes() throws GridException {
        if (busyLock.enterBusy()) {
            try {
                GridGgfsFileInfo trashInfo = id2InfoPrj.get(TRASH_ID);

                if (trashInfo != null) {
                    Map<String, GridGgfsListingEntry> listing = trashInfo.listing();

                    if (listing != null && !listing.isEmpty()) {
                        return F.viewReadOnly(listing.values(), new IgniteClosure<GridGgfsListingEntry, IgniteUuid>() {
                            @Override public IgniteUuid apply(GridGgfsListingEntry e) {
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
     * @throws GridException If operation failed.
     */
    @Nullable private GridGgfsFileInfo updatePropertiesNonTx(@Nullable IgniteUuid parentId, IgniteUuid fileId,
        String fileName, Map<String, String> props) throws GridException {
        assert fileId != null;
        assert !F.isEmpty(props) : "Expects not-empty file's properties";
        assert validTxState(true);

        if (log.isDebugEnabled())
            log.debug("Update file properties [fileId=" + fileId + ", props=" + props + ']');

        try {
            GridGgfsFileInfo oldInfo;
            GridGgfsFileInfo parentInfo;

            // Lock file ID for this transaction.
            if (parentId == null) {
                oldInfo = info(fileId);
                parentInfo = null;
            }
            else {
                Map<IgniteUuid, GridGgfsFileInfo> locked = lockIds(parentId, fileId);

                oldInfo = locked.get(fileId);
                parentInfo = locked.get(parentId);

                if (parentInfo == null)
                    return null; // Parent not found.
            }

            assert validTxState(true);

            if (oldInfo == null)
                return null; // File not found.

            if (parentInfo != null) {
                Map<String, GridGgfsListingEntry> listing = parentInfo.listing();

                GridGgfsListingEntry entry = listing.get(fileName);

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

            GridGgfsFileInfo newInfo = new GridGgfsFileInfo(oldInfo, tmp);

            id2InfoPrj.putx(fileId, newInfo);

            if (parentId != null) {
                GridGgfsListingEntry entry = new GridGgfsListingEntry(newInfo);

                assert metaCache.get(parentId) != null;

                id2InfoPrj.transform(parentId, new UpdateListing(fileName, entry, false));
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
     * @throws GridException If operation failed.
     */
    @Nullable public GridGgfsFileInfo updateProperties(@Nullable IgniteUuid parentId, IgniteUuid fileId, String fileName,
        Map<String, String> props) throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                GridCacheTx tx = metaCache.txStart(PESSIMISTIC, REPEATABLE_READ);

                try {
                    GridGgfsFileInfo info = updatePropertiesNonTx(parentId, fileId, fileName, props);

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

                id2InfoPrj.transformAsync(parentId, new UpdateListingEntry(fileId, fileName, lenDelta, 0,
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
     * @throws GridException If failed.
     */
    @Nullable public GridGgfsFileInfo updateInfo(IgniteUuid fileId, IgniteClosure<GridGgfsFileInfo, GridGgfsFileInfo> c)
        throws GridException {
        assert validTxState(false);
        assert fileId != null;
        assert c != null;

        if (busyLock.enterBusy()) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Update file info [fileId=" + fileId + ", c=" + c + ']');

                GridCacheTx tx = metaCache.isLockedByThread(fileId) ? null : metaCache.txStart(PESSIMISTIC,
                    REPEATABLE_READ);

                try {
                    // Lock file ID for this transaction.
                    GridGgfsFileInfo oldInfo = info(fileId);

                    if (oldInfo == null)
                        return null; // File not found.

                    GridGgfsFileInfo newInfo = c.apply(oldInfo);

                    if (newInfo == null)
                        throw new GridGgfsException("Failed to update file info with null value" +
                            " [oldInfo=" + oldInfo + ", newInfo=" + newInfo + ", c=" + c + ']');

                    if (!oldInfo.id().equals(newInfo.id()))
                        throw new GridGgfsException("Failed to update file info (file IDs differ)" +
                            " [oldInfo=" + oldInfo + ", newInfo=" + newInfo + ", c=" + c + ']');

                    if (oldInfo.isDirectory() != newInfo.isDirectory())
                        throw new GridGgfsException("Failed to update file info (file types differ)" +
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
     * @throws GridException If failed.
     */
    public boolean sampling(Boolean val) throws GridException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                GridCacheTx tx = metaCache.txStart(PESSIMISTIC, REPEATABLE_READ);

                try {
                    Object prev = val != null ? metaCache.put(sampling, val) : metaCache.remove(sampling);

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
     * @throws GridException If failed.
     */
    public Boolean sampling() throws GridException {
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
     * @throws GridException If file creation failed.
     */
    public GridGgfsSecondaryOutputStreamDescriptor createDual(final GridGgfsFileSystem fs, final GridGgfsPath path,
        final boolean simpleCreate, @Nullable final Map<String, String> props, final boolean overwrite, final int bufSize,
        final short replication, final long blockSize, final IgniteUuid affKey)
        throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert path != null;

                // Events to fire (can be done outside of a transaction).
                final Deque<GridGgfsEvent> pendingEvts = new LinkedList<>();

                SynchronizationTask<GridGgfsSecondaryOutputStreamDescriptor> task =
                    new SynchronizationTask<GridGgfsSecondaryOutputStreamDescriptor>() {
                        /** Output stream to the secondary file system. */
                        private OutputStream out;

                        @Override public GridGgfsSecondaryOutputStreamDescriptor onSuccess(Map<GridGgfsPath,
                            GridGgfsFileInfo> infos) throws Exception {
                            assert !infos.isEmpty();

                            // Determine the first existing parent.
                            GridGgfsPath parentPath = null;

                            for (GridGgfsPath curPath : infos.keySet()) {
                                if (parentPath == null || curPath.isSubDirectoryOf(parentPath))
                                    parentPath = curPath;
                            }

                            assert parentPath != null;

                            GridGgfsFileInfo parentInfo = infos.get(parentPath);

                            // Delegate to the secondary file system.
                            out = simpleCreate ? fs.create(path, overwrite) :
                                fs.create(path, bufSize, overwrite, replication, blockSize, props);

                            GridGgfsPath parent0 = path.parent();

                            assert parent0 != null : "path.parent() is null (are we creating ROOT?): " + path;

                            // If some of the parent directories were missing, synchronize again.
                            if (!parentPath.equals(parent0)) {
                                parentInfo = synchronize(fs, parentPath, parentInfo, parent0, true, null);

                                // Fire notification about missing directories creation.
                                if (evts.isRecordable(EVT_GGFS_DIR_CREATED)) {
                                    GridGgfsPath evtPath = parent0;

                                    while (!parentPath.equals(evtPath)) {
                                        pendingEvts.addFirst(new GridGgfsEvent(evtPath, locNode, EVT_GGFS_DIR_CREATED));

                                        evtPath = evtPath.parent();

                                        assert evtPath != null; // If this fails, then ROOT does not exist.
                                    }
                                }
                            }

                            // Get created file info.
                            GridGgfsFile status = fs.info(path);

                            if (status == null)
                                throw new GridGgfsException("Failed to open output stream to the file created in " +
                                    "the secondary file system because it no longer exists: " + path);
                            else if (status.isDirectory())
                                throw new GridGgfsException("Failed to open output stream to the file created in " +
                                    "the secondary file system because the path points to a directory: " + path);

                            GridGgfsFileInfo newInfo = new GridGgfsFileInfo(status.blockSize(), status.length(), affKey,
                                IgniteUuid.randomUuid(), ggfsCtx.ggfs().evictExclude(path, false), status.properties());

                            // Add new file info to the listing optionally removing the previous one.
                            IgniteUuid oldId = putIfAbsentNonTx(parentInfo.id(), path.name(), newInfo);

                            if (oldId != null) {
                                GridGgfsFileInfo oldInfo = info(oldId);

                                id2InfoPrj.removex(oldId); // Remove the old one.
                                id2InfoPrj.putx(newInfo.id(), newInfo); // Put the new one.

                                id2InfoPrj.transform(parentInfo.id(),
                                    new UpdateListing(path.name(), parentInfo.listing().get(path.name()), true));
                                id2InfoPrj.transform(parentInfo.id(),
                                    new UpdateListing(path.name(), new GridGgfsListingEntry(newInfo), false));

                                GridFuture<?> delFut = ggfsCtx.data().delete(oldInfo);

                                // Record PURGE event if needed.
                                if (evts.isRecordable(EVT_GGFS_FILE_PURGED)) {
                                    delFut.listenAsync(new CI1<GridFuture<?>>() {
                                        @Override public void apply(GridFuture<?> t) {
                                            try {
                                                t.get(); // Ensure delete succeeded.

                                                evts.record(new GridGgfsEvent(path, locNode, EVT_GGFS_FILE_PURGED));
                                            }
                                            catch (GridException e) {
                                                LT.warn(log, e, "Old file deletion failed in DUAL mode [path=" + path +
                                                    ", simpleCreate=" + simpleCreate + ", props=" + props +
                                                    ", overwrite=" + overwrite + ", bufferSize=" + bufSize +
                                                    ", replication=" + replication + ", blockSize=" + blockSize + ']');
                                            }
                                        }
                                    });
                                }

                                // Record DELETE event if needed.
                                if (evts.isRecordable(EVT_GGFS_FILE_DELETED))
                                    pendingEvts.add(new GridGgfsEvent(path, locNode, EVT_GGFS_FILE_DELETED));
                            }

                            // Record CREATE event if needed.
                            if (evts.isRecordable(EVT_GGFS_FILE_CREATED))
                                pendingEvts.add(new GridGgfsEvent(path, locNode, EVT_GGFS_FILE_CREATED));

                            return new GridGgfsSecondaryOutputStreamDescriptor(parentInfo.id(), newInfo, out);
                        }

                        @Override public GridGgfsSecondaryOutputStreamDescriptor onFailure(Exception err)
                            throws GridException {
                            U.closeQuiet(out);

                            U.error(log, "File create in DUAL mode failed [path=" + path + ", simpleCreate=" +
                                simpleCreate + ", props=" + props + ", overwrite=" + overwrite + ", bufferSize=" +
                                bufSize + ", replication=" + replication + ", blockSize=" + blockSize + ']', err);

                            if (err instanceof GridGgfsException)
                                throw (GridGgfsException)err;
                            else
                                throw new GridGgfsException("Failed to create the file due to secondary file system " +
                                    "exception: " + path, err);
                        }
                    };

                try {
                    return synchronizeAndExecute(task, fs, false, path.parent());
                }
                finally {
                    for (GridGgfsEvent evt : pendingEvts)
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
     * @throws GridException If output stream open for append has failed.
     */
    public GridGgfsSecondaryOutputStreamDescriptor appendDual(final GridGgfsFileSystem fs, final GridGgfsPath path,
        final int bufSize) throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert path != null;

                SynchronizationTask<GridGgfsSecondaryOutputStreamDescriptor> task =
                    new SynchronizationTask<GridGgfsSecondaryOutputStreamDescriptor>() {
                        /** Output stream to the secondary file system. */
                        private OutputStream out;

                        @Override public GridGgfsSecondaryOutputStreamDescriptor onSuccess(Map<GridGgfsPath,
                            GridGgfsFileInfo> infos) throws Exception {
                            GridGgfsFileInfo info = infos.get(path);

                            if (info.isDirectory())
                                throw new GridGgfsException("Failed to open output stream to the file in the " +
                                    "secondary file system because the path points to a directory: " + path);

                            out = fs.append(path, bufSize, false, null);

                            // Synchronize file ending.
                            long len = info.length();
                            int blockSize = info.blockSize();

                            int remainder = (int)(len % blockSize);

                            if (remainder > 0) {
                                int blockIdx = (int)(len / blockSize);

                                GridGgfsReader reader = fs.open(path, bufSize);

                                try {
                                    ggfsCtx.data().dataBlock(info, path, blockIdx, reader).get();
                                }
                                finally {
                                    reader.close();
                                }
                            }

                            // Set lock and return.
                            info = lockInfo(info);

                            metaCache.putx(info.id(), info);

                            return new GridGgfsSecondaryOutputStreamDescriptor(infos.get(path.parent()).id(), info, out);
                        }

                        @Override public GridGgfsSecondaryOutputStreamDescriptor onFailure(@Nullable Exception err)
                            throws GridException {
                            U.closeQuiet(out);

                            U.error(log, "File append in DUAL mode failed [path=" + path + ", bufferSize=" + bufSize +
                                ']', err);

                            if (err instanceof GridGgfsException)
                                throw (GridGgfsException)err;
                            else
                                throw new GridException("Failed to append to the file due to secondary file system " +
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
     * @throws GridException If input stream open has failed.
     */
    public GridGgfsSecondaryInputStreamDescriptor openDual(final GridGgfsFileSystem fs, final GridGgfsPath path,
        final int bufSize)
        throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert path != null;

                // First, try getting file info without any transactions and synchronization.
                GridGgfsFileInfo info = info(fileId(path));

                if (info != null) {
                    if (!info.isFile())
                        throw new GridGgfsInvalidPathException("Failed to open file (not a file): " + path);

                    return new GridGgfsSecondaryInputStreamDescriptor(info, fs.open(path, bufSize));
                }

                // If failed, try synchronize.
                SynchronizationTask<GridGgfsSecondaryInputStreamDescriptor> task =
                    new SynchronizationTask<GridGgfsSecondaryInputStreamDescriptor>() {
                        @Override public GridGgfsSecondaryInputStreamDescriptor onSuccess(
                            Map<GridGgfsPath, GridGgfsFileInfo> infos) throws Exception {
                            GridGgfsFileInfo info = infos.get(path);

                            if (info == null)
                                throw new GridGgfsFileNotFoundException("File not found: " + path);
                            if (!info.isFile())
                                throw new GridGgfsInvalidPathException("Failed to open file (not a file): " + path);

                            return new GridGgfsSecondaryInputStreamDescriptor(infos.get(path), fs.open(path, bufSize));
                        }

                        @Override public GridGgfsSecondaryInputStreamDescriptor onFailure(@Nullable Exception err)
                            throws GridException {
                            U.error(log, "File open in DUAL mode failed [path=" + path + ", bufferSize=" + bufSize +
                                ']', err);

                            if (err instanceof GridGgfsException)
                                throw (GridException)err;
                            else
                                throw new GridException("Failed to open the path due to secondary file system " +
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
     * @throws GridException If sync task failed.
     */
    @Nullable public GridGgfsFileInfo synchronizeFileDual(final GridGgfsFileSystem fs, final GridGgfsPath path)
        throws GridException {
        assert fs != null;
        assert path != null;

        if (busyLock.enterBusy()) {
            try {
                // First, try getting file info without any transactions and synchronization.
                GridGgfsFileInfo info = info(fileId(path));

                if (info != null)
                    return info;

                // If failed, try synchronize.
                SynchronizationTask<GridGgfsFileInfo> task =
                    new SynchronizationTask<GridGgfsFileInfo>() {
                        @Override public GridGgfsFileInfo onSuccess(Map<GridGgfsPath, GridGgfsFileInfo> infos)
                            throws Exception {
                            return infos.get(path);
                        }

                        @Override public GridGgfsFileInfo onFailure(@Nullable Exception err) throws GridException {
                            if (err instanceof GridGgfsException)
                                throw (GridException)err;
                            else
                                throw new GridException("Failed to synchronize path due to secondary file system " +
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
     * @throws GridException If directory creation failed.
     */
    public boolean mkdirsDual(final GridGgfsFileSystem fs, final GridGgfsPath path, final Map<String, String> props)
        throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert path != null;
                assert props != null;

                if (path.parent() == null)
                    return true; // No additional handling for root directory is needed.

                // Events to fire (can be done outside of a transaction).
                final Deque<GridGgfsEvent> pendingEvts = new LinkedList<>();

                SynchronizationTask<Boolean> task = new SynchronizationTask<Boolean>() {
                    @Override public Boolean onSuccess(Map<GridGgfsPath, GridGgfsFileInfo> infos) throws Exception {
                        fs.mkdirs(path, props);

                        assert !infos.isEmpty();

                        // Now perform synchronization again starting with the last created parent.
                        GridGgfsPath parentPath = null;

                        for (GridGgfsPath curPath : infos.keySet()) {
                            if (parentPath == null || curPath.isSubDirectoryOf(parentPath))
                                parentPath = curPath;
                        }

                        assert parentPath != null;

                        GridGgfsFileInfo parentPathInfo = infos.get(parentPath);

                        synchronize(fs, parentPath, parentPathInfo, path, true, null);

                        if (evts.isRecordable(EVT_GGFS_DIR_CREATED)) {
                            GridGgfsPath evtPath = path;

                            while (!parentPath.equals(evtPath)) {
                                pendingEvts.addFirst(new GridGgfsEvent(evtPath, locNode, EVT_GGFS_DIR_CREATED));

                                evtPath = evtPath.parent();

                                assert evtPath != null; // If this fails, then ROOT does not exist.
                            }
                        }

                        return true;
                    }

                    @Override public Boolean onFailure(@Nullable Exception err) throws GridException {
                        U.error(log, "Directory creation in DUAL mode failed [path=" + path + ", properties=" + props +
                            ']', err);

                        throw new GridException("Failed to create the path due to secondary file system exception: " +
                            path, err);
                    }
                };

                try {
                    return synchronizeAndExecute(task, fs, false, path.parent());
                }
                finally {
                    for (GridGgfsEvent evt : pendingEvts)
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
     * @throws GridException If failed.
     */
    public boolean renameDual(final GridGgfsFileSystem fs, final GridGgfsPath src, final GridGgfsPath dest) throws
        GridException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert src != null;
                assert dest != null;

                if (src.parent() == null)
                    return false; // Root directory cannot be renamed.

                // Events to fire (can be done outside of a transaction).
                final Collection<GridGgfsEvent> pendingEvts = new LinkedList<>();

                SynchronizationTask<Boolean> task = new SynchronizationTask<Boolean>() {
                    @Override public Boolean onSuccess(Map<GridGgfsPath, GridGgfsFileInfo> infos) throws Exception {
                        GridGgfsFileInfo srcInfo = infos.get(src);
                        GridGgfsFileInfo srcParentInfo = infos.get(src.parent());
                        GridGgfsFileInfo destInfo = infos.get(dest);
                        GridGgfsFileInfo destParentInfo = dest.parent() != null ? infos.get(dest.parent()) : null;

                        // Source path and destination (or destination parent) must exist.
                        if (srcInfo == null)
                            throw new GridGgfsFileNotFoundException("Failed to rename (source path not found): " + src);

                        if (destInfo == null && destParentInfo == null)
                            throw new GridGgfsFileNotFoundException("Failed to rename (destination path not found): " +
                                dest);

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
                                throw new GridGgfsException("Failed to rename the path in the local file system " +
                                    "because destination path already exists and it is a file: " + dest);
                            else
                                moveNonTx(srcInfo.id(), src.name(), srcParentInfo.id(), src.name(), destInfo.id());
                        }

                        // Record event if needed.
                        if (srcInfo.isFile()) {
                            if (evts.isRecordable(EVT_GGFS_FILE_RENAMED))
                                pendingEvts.add(new GridGgfsEvent(
                                    src,
                                    destInfo == null ? dest : new GridGgfsPath(dest, src.name()),
                                    locNode,
                                    EVT_GGFS_FILE_RENAMED));
                        }
                        else if (evts.isRecordable(EVT_GGFS_DIR_RENAMED))
                            pendingEvts.add(new GridGgfsEvent(src, dest, locNode, EVT_GGFS_DIR_RENAMED));

                        return true;
                    }

                    @Override public Boolean onFailure(@Nullable Exception err) throws GridException {
                        U.error(log, "Path rename in DUAL mode failed [source=" + src + ", destination=" + dest + ']',
                            err);

                        if (err instanceof GridGgfsException)
                            throw (GridException)err;
                        else
                            throw new GridException("Failed to rename the path due to secondary file system " +
                                "exception: " + src, err);
                    }
                };

                try {
                    return synchronizeAndExecute(task, fs, false, src, dest);
                }
                finally {
                    for (GridGgfsEvent evt : pendingEvts)
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
     * @throws GridException If delete failed.
     */
    public boolean deleteDual(final GridGgfsFileSystem fs, final GridGgfsPath path, final boolean recursive)
        throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert fs != null;
                assert path != null;

                SynchronizationTask<Boolean> task = new SynchronizationTask<Boolean>() {
                    @Override public Boolean onSuccess(Map<GridGgfsPath, GridGgfsFileInfo> infos) throws Exception {
                        GridGgfsFileInfo info = infos.get(path);

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
                        id2InfoPrj.transform(info.id(), new UpdatePath(path));

                        return true; // No additional handling is required.
                    }

                    @Override public Boolean onFailure(@Nullable Exception err) throws GridException {
                        U.error(log, "Path delete in DUAL mode failed [path=" + path + ", recursive=" + recursive + ']',
                            err);

                        throw new GridException("Failed to delete the path due to secondary file system exception: ",
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
     * @throws GridException If update failed.
     */
    public GridGgfsFileInfo updateDual(final GridGgfsFileSystem fs, final GridGgfsPath path, final Map<String, String> props)
        throws GridException {
        assert fs != null;
        assert path != null;
        assert props != null && !props.isEmpty();

        if (busyLock.enterBusy()) {
            try {
                SynchronizationTask<GridGgfsFileInfo> task = new SynchronizationTask<GridGgfsFileInfo>() {
                    @Override public GridGgfsFileInfo onSuccess(Map<GridGgfsPath, GridGgfsFileInfo> infos)
                        throws Exception {
                        if (infos.get(path) == null)
                            return null;

                        fs.update(path, props);

                        assert path.parent() == null || infos.get(path.parent()) != null;

                        return updatePropertiesNonTx(infos.get(path.parent()).id(), infos.get(path).id(), path.name(),
                            props);
                    }

                    @Override public GridGgfsFileInfo onFailure(@Nullable Exception err) throws GridException {
                        U.error(log, "Path update in DUAL mode failed [path=" + path + ", properties=" + props + ']',
                            err);

                        throw new GridException("Failed to update the path due to secondary file system exception: " +
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
     * @throws GridException If failed.
     */
    private GridGgfsFileInfo synchronize(GridGgfsFileSystem fs, GridGgfsPath startPath, GridGgfsFileInfo startPathInfo,
        GridGgfsPath endPath, boolean strict, @Nullable Map<GridGgfsPath, GridGgfsFileInfo> created)
        throws GridException {
        assert fs != null;
        assert startPath != null && startPathInfo != null && endPath != null;

        validTxState(true);

        GridGgfsFileInfo parentInfo = startPathInfo;

        List<String> components = endPath.components();

        GridGgfsPath curPath = startPath;

        for (int i = startPath.components().size(); i < components.size(); i++) {
            curPath = new GridGgfsPath(curPath, components.get(i));

            if (created != null && created.containsKey(curPath))
                // Re-use already created info.
                parentInfo = created.get(curPath);
            else {
                // Get file status from the secondary file system.
                GridGgfsFile status = fs.info(curPath);

                if (status != null) {
                    if (!status.isDirectory() && !curPath.equals(endPath))
                        throw new GridException("Failed to create path the locally because secondary file system " +
                            "directory structure was modified concurrently and the path is not a directory as " +
                            "expected: " + curPath);
                }
                else {
                    if (strict) {
                        throw new GridException("Failed to create path locally due to secondary file system " +
                            "exception: " + curPath);
                    }
                    else if (created != null)
                        created.put(curPath.parent(), parentInfo);

                    return null;
                }

                // Recreate the path locally.
                GridGgfsFileInfo curInfo = status.isDirectory() ? new GridGgfsFileInfo(true, status.properties()) :
                    new GridGgfsFileInfo(ggfsCtx.configuration().getBlockSize(), status.length(),
                        ggfsCtx.ggfs().evictExclude(curPath, false), status.properties());

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
     * @throws GridException If failed.
     */
    private <T> T synchronizeAndExecute(SynchronizationTask<T> task, GridGgfsFileSystem fs, boolean strict,
        GridGgfsPath... paths)
        throws GridException {
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
     * @throws GridException If failed.
     */
    private <T> T synchronizeAndExecute(SynchronizationTask<T> task, GridGgfsFileSystem fs, boolean strict,
        @Nullable Collection<IgniteUuid> extraLockIds, GridGgfsPath... paths) throws GridException {
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

            for (GridGgfsPath path : paths)
                pathIds.add(fileIds(path));

            // Start pessimistic.
            GridCacheTx tx = metaCache.txStart(PESSIMISTIC, REPEATABLE_READ);

            try {
                // Lock the very first existing parents and possibly the leaf as well.
                Map<GridGgfsPath, GridGgfsPath> pathToParent = new HashMap<>();

                Map<GridGgfsPath, IgniteUuid> pathToId = new HashMap<>();

                for (int i = 0; i < paths.length; i++) {
                    GridGgfsPath path = paths[i];

                    // Determine the very first existing parent
                    List<IgniteUuid> ids = pathIds.get(i);

                    if (ids.size() > 1) {
                        // The path is not root.
                        GridGgfsPath parentPath = path.parent();
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

                Map<IgniteUuid, GridGgfsFileInfo> idToInfo = lockIds(lockArr);

                if (extraLockIds != null) {
                    for (IgniteUuid id : extraLockIds)
                        idToInfo.remove(id);
                }

                // Ensure that locked IDs still point to expected paths.
                GridGgfsPath changed = null;

                for (Map.Entry<GridGgfsPath, IgniteUuid> entry : pathToId.entrySet()) {
                    if (!idToInfo.containsKey(entry.getValue()) ||
                        !F.eq(entry.getValue(), fileId(entry.getKey(), true))) {
                        changed = entry.getKey();

                        break;
                    }
                }

                if (changed != null) {
                    finished = true;

                    throw new GridGgfsConcurrentModificationException(changed);
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
                        Map<GridGgfsPath, GridGgfsFileInfo> infos = new HashMap<>();

                        TreeMap<GridGgfsPath, GridGgfsFileInfo> created = new TreeMap<>();

                        for (GridGgfsPath path : paths) {
                            GridGgfsPath parentPath = path.parent();

                            if (pathToId.containsKey(path)) {
                                infos.put(path, info(pathToId.get(path)));

                                if (parentPath != null)
                                    infos.put(parentPath, info(pathToId.get(parentPath)));
                            }
                            else {
                                GridGgfsPath firstParentPath = pathToParent.get(path);

                                assert firstParentPath != null;
                                assert pathToId.get(firstParentPath) != null;

                                GridGgfsFileInfo info = synchronize(fs, firstParentPath,
                                    idToInfo.get(pathToId.get(firstParentPath)), path, strict, created);

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
            catch (GridException e) {
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
     * @throws GridException If operation failed.
     */
    private <K, V> boolean putx(GridCacheProjection<K, V> cache, K key, IgniteClosure<V, V> c) throws GridException {
        assert validTxState(true);

        V oldVal = cache.get(key);
        V newVal = c.apply(oldVal);

        return newVal == null ? cache.removex(key) : cache.putx(key, newVal);
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
     * @throws GridException If update failed.
     */
    public void updateTimes(IgniteUuid parentId, IgniteUuid fileId, String fileName, long accessTime,
        long modificationTime) throws GridException {
        if (busyLock.enterBusy()) {
            try {
                assert validTxState(false);

                // Start pessimistic transaction.
                GridCacheTx tx = metaCache.txStart(PESSIMISTIC, REPEATABLE_READ);

                try {
                    Map<IgniteUuid, GridGgfsFileInfo> infoMap = lockIds(fileId, parentId);

                    GridGgfsFileInfo fileInfo = infoMap.get(fileId);

                    if (fileInfo == null)
                        throw new GridGgfsFileNotFoundException("Failed to update times (path was not found): " +
                            fileName);

                    GridGgfsFileInfo parentInfo = infoMap.get(parentId);

                    if (parentInfo == null)
                        throw new GridGgfsInvalidPathException("Failed to update times (parent was not found): " +
                            fileName);

                    GridGgfsListingEntry entry = parentInfo.listing().get(fileName);

                    // Validate listing.
                    if (entry == null || !entry.fileId().equals(fileId))
                        throw new GridGgfsInvalidPathException("Failed to update times (file concurrently modified): " +
                            fileName);

                    assert parentInfo.isDirectory();

                    GridGgfsFileInfo updated = new GridGgfsFileInfo(fileInfo,
                        accessTime == -1 ? fileInfo.accessTime() : accessTime,
                        modificationTime == -1 ? fileInfo.modificationTime() : modificationTime);

                    id2InfoPrj.putx(fileId, updated);

                    id2InfoPrj.transform(parentId, new UpdateListingEntry(fileId, fileName, 0, accessTime,
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
        public T onSuccess(Map<GridGgfsPath, GridGgfsFileInfo> infos) throws Exception;

        /**
         * Callback handler in case synchronization failed.
         *
         * @param err Optional exception.
         * @return Task result.
         * @throws GridException In case exception is to be thrown in that case.
         */
        public T onFailure(Exception err) throws GridException;
    }

    /**
     * Path descriptor.
     */
    private static class PathDescriptor {
        /** Path. */
        private final GridGgfsPath path;

        /** Resolved IDs. */
        private final List<IgniteUuid> ids;

        /** Parent path. */
        private GridGgfsPath parentPath;

        /** Parent path info. */
        private GridGgfsFileInfo parentInfo;

        /**
         * Constructor.
         *
         * @param path Path.
         * @param ids Resolved path IDs.
         * @param parentPath Parent path.
         * @param parentInfo Parent info.
         */
        PathDescriptor(GridGgfsPath path, List<IgniteUuid> ids, GridGgfsPath parentPath, GridGgfsFileInfo parentInfo) {
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
        private void updateParent(GridGgfsPath newParentPath, GridGgfsFileInfo newParentInfo) {
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
        private GridGgfsPath parentPath() {
            return parentPath;
        }

        /**
         * Get parent path info.
         *
         * @return Parent path info.
         */
        private GridGgfsFileInfo parentInfo() {
            return parentInfo;
        }
    }

    /**
     * Updates file length information in parent listing.
     */
    private static final class UpdateListingEntry implements IgniteClosure<GridGgfsFileInfo, GridGgfsFileInfo>,
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
        private UpdateListingEntry(IgniteUuid fileId, String fileName, long lenDelta,
            long accessTime, long modificationTime) {
            this.fileId = fileId;
            this.fileName = fileName;
            this.lenDelta = lenDelta;
            this.accessTime = accessTime;
            this.modificationTime = modificationTime;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsFileInfo apply(GridGgfsFileInfo fileInfo) {
            Map<String, GridGgfsListingEntry> listing = fileInfo.listing();

            GridGgfsListingEntry entry = listing.get(fileName);

            if (entry == null || !entry.fileId().equals(fileId))
                return fileInfo;

            entry = new GridGgfsListingEntry(entry, entry.length() + lenDelta,
                accessTime == -1 ? entry.accessTime() : accessTime,
                modificationTime == -1 ? entry.modificationTime() : modificationTime);

            // Create new map to replace info.
            listing = new HashMap<>(listing);

            // Modify listing map in-place since map is serialization-safe.
            listing.put(fileName, entry);

            return new GridGgfsFileInfo(listing, fileInfo);
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
    private static final class UpdateListing implements IgniteClosure<GridGgfsFileInfo, GridGgfsFileInfo>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** File name to add into parent listing. */
        private String fileName;

        /** File ID.*/
        private GridGgfsListingEntry entry;

        /** Update operation: remove entry from listing if {@code true} or add entry to listing if {@code false}. */
        private boolean rmv;

        /**
         * Constructs update directory listing closure.
         *
         * @param fileName File name to add into parent listing.
         * @param entry Listing entry to add or remove.
         * @param rmv Remove entry from listing if {@code true} or add entry to listing if {@code false}.
         */
        private UpdateListing(String fileName, GridGgfsListingEntry entry, boolean rmv) {
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
        @Override @Nullable public GridGgfsFileInfo apply(GridGgfsFileInfo fileInfo) {
            assert fileInfo != null : "File info not found for the child: " + entry.fileId();
            assert fileInfo.isDirectory();

            Map<String, GridGgfsListingEntry> listing =
                U.newHashMap(fileInfo.listing().size() + (rmv ? 0 : 1));

            listing.putAll(fileInfo.listing());

            if (rmv) {
                GridGgfsListingEntry oldEntry = listing.get(fileName);

                if (oldEntry == null || !oldEntry.fileId().equals(entry.fileId()))
                    throw new GridRuntimeException("Directory listing doesn't contain expected file" +
                        " [listing=" + listing + ", fileName=" + fileName + ", entry=" + entry + ']');

                // Modify listing in-place.
                listing.remove(fileName);
            }
            else {
                // Modify listing in-place.
                GridGgfsListingEntry oldEntry = listing.put(fileName, entry);

                if (oldEntry != null && !oldEntry.fileId().equals(entry.fileId()))
                    throw new GridRuntimeException("Directory listing contains unexpected file" +
                        " [listing=" + listing + ", fileName=" + fileName + ", entry=" + entry +
                        ", oldEntry=" + oldEntry + ']');
            }

            return new GridGgfsFileInfo(listing, fileInfo);
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
            entry = (GridGgfsListingEntry)in.readObject();
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
    private static final class UpdatePath implements IgniteClosure<GridGgfsFileInfo, GridGgfsFileInfo>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** New path. */
        private GridGgfsPath path;

        /**
         * @param path Path.
         */
        private UpdatePath(GridGgfsPath path) {
            this.path = path;
        }

        /**
         * Default constructor (required by Externalizable).
         */
        public UpdatePath() {
        }

        /** {@inheritDoc} */
        @Override public GridGgfsFileInfo apply(GridGgfsFileInfo info) {
            return GridGgfsFileInfo.builder(info).path(path).build();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(path);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            path = (GridGgfsPath)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(UpdatePath.class, this);
        }
    }
}
