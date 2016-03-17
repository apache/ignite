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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.EventType;
import org.apache.ignite.igfs.IgfsDirectoryNotEmptyException;
import org.apache.ignite.igfs.IgfsParentNotDirectoryException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathAlreadyExistsException;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_OPENED_WRITE;

/**
 * Version of Meta Manager implementation with relaxed synchronization.
 */
@SuppressWarnings("all")
public class RelaxedIgfsMetaManager extends IgfsMetaManager {

    public RelaxedIgfsMetaManager() {
        // noop
    }

    /**
     * Move routine.
     *
     * @param srcPath Source path.
     * @param dstPath Destinatoin path.
     * @return File info of renamed entry.
     * @throws IgniteCheckedException In case of exception.
     */
    @Override  public IgfsFileInfo move(IgfsPath srcPath, IgfsPath dstPath) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                // 1. First get source and destination path IDs.
                List<IgniteUuid> srcPathIds = fileIds(srcPath);
                List<IgniteUuid> dstPathIds = fileIds(dstPath);

                final Set<IgniteUuid> allIds = new TreeSet<>(PATH_ID_SORTING_COMPARATOR);

                @Nullable final IgniteUuid dstLeafId = dstPathIds.get(dstPathIds.size() - 1);

                if (dstLeafId == null) {
                    // Delete null entry for the unexisting destination element:
                    dstPathIds.remove(dstPathIds.size() - 1);

                    allIds.add(dstPathIds.get(dstPathIds.size() - 1));
                }
                else {
                    allIds.add(dstLeafId);
                }

                final IgniteUuid victimId = srcPathIds.get(srcPathIds.size() - 1);
                final IgniteUuid victimParentId = srcPathIds.get(srcPathIds.size() - 2);

                allIds.add(victimId); // move victim
                allIds.add(victimParentId); // move victim parent

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
                    if (!verifyExists(allInfos, victimParentId)
                        || !verifyExists(allInfos, victimId)
                        || !verifyParentChild(allInfos, victimParentId, srcPath.name(), victimId)) {
                        throw new IgfsPathNotFoundException("Failed to perform move because source directory " +
                            "structure changed concurrently [src=" + srcPath + ", dst=" + dstPath + ']');
                    }

                    // 5. Verify integrity of destination directory.
                    final IgfsPath dstDirPath = dstLeafId != null ? dstPath : dstPath.parent();

                    final IgniteUuid dstDirId = dstPathIds.get(dstPathIds.size() - 1);

                    if (!verifyExists(allInfos, dstDirId)) {
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
                    if (dstTargetInfo.listing().containsKey(dstName)) {
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
     * Verifies existence of a file in transaction.
     *
     * @param infos Map of the locked
     * @param id The id to check.
     * @return If the file exists.
     */
    private boolean verifyExists(Map<IgniteUuid, IgfsFileInfo> infos, IgniteUuid id) {
        validTxState(true);

        return infos.get(id) != null;
    }

    /**
     * Verifies parent - child relationship in a transaction.
     *
     * @param infos
     * @param parent
     * @param childName
     * @param ch
     * @return
     */
    private boolean verifyParentChild(Map<IgniteUuid, IgfsFileInfo> infos, IgniteUuid parent,
                                      String childName, IgniteUuid ch) {
        assert ch != null;
        validTxState(true);

        IgfsFileInfo p = infos.get(parent);

        IgfsListingEntry e = p.listing().get(childName);

        if (e == null) {
            return false;
        }

        return ch.equals(e.fileId());
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
    @Override IgniteUuid softDelete(final IgfsPath path, final boolean recursive) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                validTxState(false);

                final SortedSet<IgniteUuid> allIds = new TreeSet<>(PATH_ID_SORTING_COMPARATOR);

                List<IgniteUuid> pathIdList = fileIds(path);

                assert pathIdList.size() > 1;

                final IgniteUuid victimId = pathIdList.get(pathIdList.size() - 1);
                final IgniteUuid parentId = pathIdList.get(pathIdList.size() - 2);

                assert !IgfsUtils.isRootOrTrashId(victimId) : "Cannot delete root or trash directories.";

                allIds.add(victimId);
                allIds.add(parentId);

                if (allIds.remove(null))
                    return null; // A fragment of the path no longer exists.

                IgniteUuid trashId = IgfsUtils.randomTrashId();

                boolean added = allIds.add(trashId);
                assert added;

                final IgniteInternalTx tx = startTx();

                try {
                    final Map<IgniteUuid, IgfsFileInfo> infoMap = lockIds(allIds);

                    // Directory stucture was changed concurrently, so the original path no longer exists:
                    if (!verifyExists(infoMap, victimId)
                        || !verifyExists(infoMap, parentId)
                        || !verifyParentChild(infoMap, parentId, path.name(), victimId))
                        return null;

                    final IgfsFileInfo victimInfo = infoMap.get(victimId);

                    if (!recursive && victimInfo.isDirectory() && !victimInfo.listing().isEmpty())
                        // Throw exception if not empty and not recursive.
                        throw new IgfsDirectoryNotEmptyException("Failed to remove directory (directory is not " +
                            "empty and recursive flag is not set).");

                    IgfsFileInfo destInfo = infoMap.get(trashId);

                    assert destInfo != null;

                    final String srcFileName = path.name();

                    final String destFileName = victimId.toString();

                    assert destInfo.listing().get(destFileName) == null : "Failed to add file name into the " +
                        "destination directory (file already exists) [destName=" + destFileName + ']';

                    IgfsFileInfo srcParentInfo = infoMap.get(parentId);

                    assert srcParentInfo != null;

                    IgniteUuid srcParentId = srcParentInfo.id();
                    assert srcParentId.equals(parentId);

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
     * Mkdirs implementation.
     *
     * @param path The path to create.
     * @param props The properties to use for created directories.
     * @return True iff a directory was created during the operation.
     * @throws IgniteCheckedException If a non-directory file exists on the requested path, and in case of other errors.
     */
    @Override boolean mkdirs(final IgfsPath path, final Map<String, String> props) throws IgniteCheckedException {
        assert props != null;
        validTxState(false);

        RelaxedDirectoryChainBuilder b = null;

        while (true) {
            if (busyLock.enterBusy()) {
                try {
                    b = new RelaxedDirectoryChainBuilder(path, props);

                    // Start TX.
                    IgniteInternalTx tx = startTx();

                    try {
                        final Map<IgniteUuid, IgfsFileInfo> lockedInfos = lockIds(b.idSet);

                        // If the path was changed, we close the current Tx and repeat the procedure again
                        // starting from taking the path ids.
                        if (verifyExists(lockedInfos, b.lowermostExistingId)) {
                            // Locked path okay, trying to proceed with the remainder creation.
                            IgfsFileInfo lowermostExistingInfo = lockedInfos.get(b.lowermostExistingId);

                            // Check only the lowermost directory in the existing directory chain
                            // because others are already checked in #verifyPathIntegrity() above.
                            if (!lowermostExistingInfo.isDirectory())
                                throw new IgfsParentNotDirectoryException("Failed to create directory (parent " +
                                    "element is not a directory)");

                            if (b.existingIdCnt == b.components.size() + 1) {
                                assert b.existingPath.equals(path);

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
    @Override IgniteBiTuple<IgfsFileInfo, IgniteUuid> create(
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
        assert !(append && overwrite); // both append & overwrite must not be true.

        final String name = path.name();

        RelaxedDirectoryChainBuilder b = null;

        final IgniteUuid trashId = IgfsUtils.randomTrashId();

        while (true) {
            if (busyLock.enterBusy()) {
                try {
                    b = new RelaxedDirectoryChainBuilder(path, dirProps, fileProps, blockSize, affKey, evictExclude);

                    final IgniteUuid overwriteId = IgniteUuid.randomUuid();

                    // Start Tx:
                    IgniteInternalTx tx = startTx();

                    try {
                        assert b.idList.size() >= 2;

                        final IgniteUuid parentId = b.idList.get(b.idList.size() - 2);

                        if (overwrite) {
                            // Lock also the TRASH directory because in case of overwrite we
                            // may need to delete the old file:
                            b.idSet.add(trashId);
                            b.idSet.add(overwriteId);

                            if (b.existingIdCnt == b.components.size() + 1) {
                                assert parentId != null;

                                // Full path exists AND overwrite => we must lock the parent also:
                                b.idSet.add(parentId);
                            }
                        }

                        final Map<IgniteUuid, IgfsFileInfo> lockedInfos = lockIds(b.idSet);

                        assert !overwrite || lockedInfos.get(trashId) != null; // TRASH must exist at this point.

                        // If the path was changed, we close the current Tx and repeat the procedure again
                        // starting from taking the path ids.
                        final boolean pathVerified;

//                        // TODO: Problematic version:
//
//                        if (overwrite && (b.existingIdCnt == b.components.size() + 1)) {
//                            pathVerified = verifyExists(lockedInfos, parentId);
//                        }
//                        else {
//                            pathVerified = verifyExists(lockedInfos, b.lowermostExistingId);
//                        }

                        // TODO: workable version:
                        if (overwrite && (b.existingIdCnt == b.components.size() + 1)) {
                            pathVerified = verifyExists(lockedInfos, parentId)
                                && verifyExists(lockedInfos, b.lowermostExistingId)
                                && verifyParentChild(lockedInfos, parentId, name, b.lowermostExistingId);
                        } else {
                            pathVerified = verifyExists(lockedInfos, b.lowermostExistingId);
                        }

                        if (pathVerified) {
                            // Locked path okay, trying to proceed with the remainder creation.
                            final IgfsFileInfo lowermostExistingInfo = lockedInfos.get(b.lowermostExistingId);

                            if (b.existingIdCnt == b.components.size() + 1) {
                                // Full requestd path exists.
                                assert b.existingPath.equals(path);

                                if (lowermostExistingInfo != null && lowermostExistingInfo.isDirectory()) {
                                    throw new IgfsPathAlreadyExistsException("Failed to "
                                            + (append ? "open" : "create") + " file (path points to an " +
                                        "existing directory): " + path);
                                }
                                else {
                                    // This is a file.
                                    assert lowermostExistingInfo == null || lowermostExistingInfo.isFile();

                                    final IgniteUuid lockId = lowermostExistingInfo == null
                                        ? null : lowermostExistingInfo.lockId();

                                    if (append) {
                                        assert lowermostExistingInfo != null;

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

                                        IgfsFileInfo parentInfo = lockedInfos.get(parentId);

                                        // Otherwise verifyPathIntegrity must have returned false:
                                        assert parentId != null;
                                        assert parentInfo != null;

                                        boolean deleted = false;

                                        // The overwrite victim may not exist and may have been moved somewhere,
                                        // so we delete it only if it is in the expected place.
                                        if (lowermostExistingInfo != null
                                            && verifyParentChild(lockedInfos, parentId,
                                                name, lowermostExistingInfo.id())) {
                                            final IgfsListingEntry deletedEntry = lockedInfos.get(parentId).listing()
                                                .get(name);

                                            assert deletedEntry != null;

                                            transferEntry(deletedEntry, parentId, name, trashId,
                                                lowermostExistingInfo.id().toString());

                                            // Update a file info of the removed file with a file path,
                                            // which will be used by delete worker for event notifications.
                                            invokeUpdatePath(lowermostExistingInfo.id(), path);

                                            deleted = true;
                                        }

                                        // Make a new locked info:
                                        long t = System.currentTimeMillis();

                                        final IgfsFileInfo newFileInfo = new IgfsFileInfo(cfg.getBlockSize(), 0L,
                                            affKey, createFileLockId(false), evictExclude, fileProps, t, t);

                                        assert newFileInfo.lockId() != null; // locked info should be created.

                                        createNewEntry(newFileInfo, parentId, name);

                                        IgniteBiTuple<IgfsFileInfo, IgniteUuid> t2 = new T2<>(newFileInfo, parentId);

                                        tx.commit();

                                        if (deleted)
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

                            Map<String, IgfsListingEntry> parentListing = lowermostExistingInfo.listing();

                            final String uppermostFileToBeCreatedName = b.components.get(b.existingIdCnt - 1);

                            final IgfsListingEntry entry = parentListing.get(uppermostFileToBeCreatedName);

                            if (entry == null) {
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
    private class RelaxedDirectoryChainBuilder extends IgfsMetaManager.DirectoryChainBuilder {
        /**
         * Constructor for directories.
         *
         * @param path Path.
         * @param props Properties.
         * @throws IgniteCheckedException If failed.
         */
        protected RelaxedDirectoryChainBuilder(IgfsPath path, Map<String, String> props) throws IgniteCheckedException {
            super(path, props, props, true, 0, null, false);
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
        protected RelaxedDirectoryChainBuilder(IgfsPath path, Map<String, String> dirProps, Map<String, String> fileProps,
                                        int blockSize, @Nullable IgniteUuid affKey, boolean evictExclude)
            throws IgniteCheckedException {
            super(path, dirProps, fileProps, false, blockSize, affKey, evictExclude);
        }

        /** {@inheritDoc} */
        @Override protected void initIdSets() {
            IgfsPath existingPath = path.root();

            // Find the lowermost existing id:
            IgniteUuid lowermostExistingId = null;

            int idIdx = 0;

            for (IgniteUuid id: idList) {
                if (id != null) {
                    lowermostExistingId = id;

                    if (idIdx >= 1) // skip root.
                        existingPath = new IgfsPath(existingPath, components.get(idIdx - 1));

                    idIdx++;
                }
            }

            idSet.add(lowermostExistingId);

            this.lowermostExistingId = lowermostExistingId;

            this.existingPath = existingPath;

            this.existingIdCnt = idIdx;
        }

        /**
         * Builds middle nodes.
         */
        protected IgfsFileInfo buildMiddleNode(String childName, IgfsFileInfo childInfo) {
            return new IgfsFileInfo(Collections.singletonMap(childName,
                    new IgfsListingEntry(childInfo)), middleProps);
        }

        /**
         * Builds leaf.
         */
        protected IgfsFileInfo buildLeaf()  {
            long t = System.currentTimeMillis();

            return new IgfsFileInfo(true, leafProps, t, t);
        }
    }
}