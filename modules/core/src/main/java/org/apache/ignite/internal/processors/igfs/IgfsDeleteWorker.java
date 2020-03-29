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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_PURGED;

/**
 * IGFS worker for removal from the trash directory.
 */
public class IgfsDeleteWorker extends IgfsThread {
    /** Awake frequency, */
    private static final long FREQUENCY = 1000;

    /** How many files/folders to delete at once (i.e in a single transaction). */
    private static final int MAX_DELETE_BATCH = 100;

    /** IGFS context. */
    private final IgfsContext igfsCtx;

    /** Metadata manager. */
    private final IgfsMetaManager meta;

    /** Data manager. */
    private final IgfsDataManager data;

    /** Logger. */
    private final IgniteLogger log;

    /** Lock. */
    private final Lock lock = new ReentrantLock();

    /** Condition. */
    private final Condition cond = lock.newCondition();

    /** Force worker to perform actual delete. */
    private boolean force;

    /** Cancellation flag. */
    private volatile boolean cancelled;

    /**
     * Constructor.
     *
     * @param igfsCtx IGFS context.
     */
    IgfsDeleteWorker(IgfsContext igfsCtx) {
        super("igfs-delete-worker%" + igfsCtx.igfs().name() + "%" + igfsCtx.kernalContext().localNodeId() + "%");

        this.igfsCtx = igfsCtx;

        meta = igfsCtx.meta();
        data = igfsCtx.data();

        assert meta != null;
        assert data != null;

        log = igfsCtx.kernalContext().log(IgfsDeleteWorker.class);
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException {
        if (log.isDebugEnabled())
            log.debug("Delete worker started.");

        while (!cancelled) {
            lock.lock();

            try {
                if (!cancelled && !force)
                    cond.await(FREQUENCY, TimeUnit.MILLISECONDS);

                force = false; // Reset force flag.
            }
            finally {
                lock.unlock();
            }

            if (!cancelled)
                delete();
        }
    }

    /**
     * Notify the worker that new entry to delete appeared.
     */
    void signal() {
        lock.lock();

        try {
            force = true;

            cond.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Cancels the worker.
     */
    void cancel() {
        cancelled = true;

        interrupt();
    }

    /**
     * Perform cleanup of trash directories.
     */
    private void delete() {
        for (int i = 0; i < IgfsUtils.TRASH_CONCURRENCY; i++)
            delete(IgfsUtils.trashId(i));
    }

    /**
     * Perform cleanup of concrete trash directory.
     *
     * @param trashId Trash ID.
     */
    private void delete(IgniteUuid trashId) {
        IgfsEntryInfo info = null;

        try {
            info = meta.info(trashId);
        }
        catch (ClusterTopologyServerNotFoundException ignore) {
            // Ignore.
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Cannot obtain trash directory info (is node stopping?)");

            if (log.isDebugEnabled())
                U.error(log, "Cannot obtain trash directory info.", e);
        }

        if (info != null) {
            for (Map.Entry<String, IgfsListingEntry> entry : info.listing().entrySet()) {
                IgniteUuid fileId = entry.getValue().fileId();

                if (log.isDebugEnabled())
                    log.debug("Deleting IGFS trash entry [name=" + entry.getKey() + ", fileId=" + fileId + ']');

                try {
                    if (!cancelled) {
                        if (delete(trashId, entry.getKey(), fileId)) {
                            if (log.isDebugEnabled())
                                log.debug("Sending delete confirmation message [name=" + entry.getKey() +
                                    ", fileId=" + fileId + ']');
                        }
                    }
                    else
                        break;
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // Ignore this exception while stopping.
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to delete entry from the trash directory: " + entry.getKey(), e);
                }
            }
        }
    }

    /**
     * Remove particular entry from the TRASH directory.
     *
     * @param trashId ID of the trash directory.
     * @param name Entry name.
     * @param id Entry ID.
     * @return {@code True} in case the entry really was deleted form the file system by this call.
     * @throws IgniteCheckedException If failed.
     */
    private boolean delete(IgniteUuid trashId, String name, IgniteUuid id) throws IgniteCheckedException {
        assert name != null;
        assert id != null;

        while (true) {
            IgfsEntryInfo info = meta.info(id);

            if (info != null) {
                if (info.isDirectory()) {
                    if (!deleteDirectoryContents(trashId, id))
                        return false;

                    if (meta.delete(trashId, name, id))
                        return true;
                }
                else {
                    assert info.isFile();

                    // Lock the file with special lock Id to prevent concurrent writing:
                    IgfsEntryInfo lockedInfo = meta.lock(id, true);

                    if (lockedInfo == null)
                        return false; // File is locked, we cannot delete it.

                    assert id.equals(lockedInfo.id());

                    // Delete file content first.
                    // In case this node crashes, other node will re-delete the file.
                    data.delete(lockedInfo).get();

                    boolean ret = meta.delete(trashId, name, id);

                    if (ret) {
                        IgfsPath path = IgfsUtils.extractOriginalPathFromTrash(name);

                        assert path != null;

                        IgfsUtils.sendEvents(igfsCtx.kernalContext(), path, EVT_IGFS_FILE_PURGED);
                    }

                    return ret;
                }
            }
            else
                return false; // Entry was deleted concurrently.
        }
    }

    /**
     * Remove particular entry from the trash directory or subdirectory.
     *
     * @param parentId Parent ID.
     * @param id Entry id.
     * @return true iff all the items in the directory were deleted (directory is seen to be empty).
     * @throws IgniteCheckedException If delete failed for some reason.
     */
    private boolean deleteDirectoryContents(IgniteUuid parentId, final IgniteUuid id) throws IgniteCheckedException {
        assert parentId != null;
        assert id != null;

        while (true) {
            IgfsEntryInfo info = meta.info(id);

            if (info != null) {
                assert info.isDirectory();

                final Map<String, IgfsListingEntry> listing = info.listing();

                if (listing.isEmpty())
                    return true; // Directory is empty.

                final Map<String, IgfsListingEntry> delListing = new HashMap<>(MAX_DELETE_BATCH, 1.0f);

                final GridCompoundFuture<Object, ?> fut = new GridCompoundFuture<>();

                int failedFiles = 0;

                for (final Map.Entry<String, IgfsListingEntry> entry : listing.entrySet()) {
                    if (cancelled)
                        return false;

                    if (entry.getValue().isDirectory()) {
                        if (deleteDirectoryContents(id, entry.getValue().fileId())) // *** Recursive call.
                            delListing.put(entry.getKey(), entry.getValue());
                        else
                            failedFiles++;
                    }
                    else {
                        IgfsEntryInfo fileInfo = meta.info(entry.getValue().fileId());

                        if (fileInfo != null) {
                            assert fileInfo.isFile();

                            IgfsEntryInfo lockedInfo = meta.lock(fileInfo.id(), true);

                            if (lockedInfo == null)
                                // File is already locked:
                                failedFiles++;
                            else {
                                assert IgfsUtils.DELETE_LOCK_ID.equals(lockedInfo.lockId());

                                fut.add(data.delete(lockedInfo));

                                delListing.put(entry.getKey(), entry.getValue());
                            }
                        }
                    }

                    if (delListing.size() == MAX_DELETE_BATCH)
                        break;
                }

                fut.markInitialized();

                // Wait for data cache to delete values before clearing meta cache.
                try {
                    fut.get();
                }
                catch (IgniteFutureCancelledCheckedException ignore) {
                    // This future can be cancelled only due to IGFS shutdown.
                    cancelled = true;

                    return false;
                }

                // Actual delete of folder content.
                Collection<IgniteUuid> delIds = meta.delete(id, delListing);

                if (listing.size() == delIds.size())
                    return true; // All entries were deleted.

                if (listing.size() == delListing.size() + failedFiles)
                    // All the files were tried, no reason to continue the loop:
                    return false;
            }
            else
                return true; // Directory entry was deleted concurrently.
        }
    }
}
