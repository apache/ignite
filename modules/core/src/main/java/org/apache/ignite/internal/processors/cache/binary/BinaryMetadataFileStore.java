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
package org.apache.ignite.internal.processors.cache.binary;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.TMP_SUFFIX;

/**
 * Class handles saving/restoring binary metadata to/from disk.
 *
 * Current implementation needs to be rewritten as it issues IO operations from discovery thread which may lead to
 * segmentation of nodes from cluster.
 */
class BinaryMetadataFileStore {
    /** Link to resolved binary metadata directory. Null for non persistent mode */
    private File metadataDir;

    /** */
    private final ConcurrentMap<Integer, BinaryMetadataHolder> metadataLocCache;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final boolean enabled;

    /** */
    private FileIOFactory fileIOFactory;

    /** */
    private final IgniteLogger log;

    /** */
    private BinaryMetadataAsyncWriter writer;

    /**
     * @param metadataLocCache Metadata locale cache.
     * @param ctx Context.
     * @param log Logger.
     * @param metadataDir Path to binary metadata store configured by user, should include binary_meta
     * and consistentId.
     * @param forceEnabled If {@code true} then will write files even if persistence and CDC disabled.
     */
    BinaryMetadataFileStore(
        final ConcurrentMap<Integer, BinaryMetadataHolder> metadataLocCache,
        final GridKernalContext ctx,
        final IgniteLogger log,
        final File metadataDir,
        final boolean forceEnabled
    ) throws IgniteCheckedException {
        this.metadataLocCache = metadataLocCache;
        this.ctx = ctx;

        enabled = forceEnabled || enabled(ctx.config());

        this.log = log;

        if (!enabled)
            return;

        DataStorageConfiguration dsCfg = ctx.config().getDataStorageConfiguration();

        fileIOFactory = dsCfg == null ? new DataStorageConfiguration().getFileIOFactory() : dsCfg.getFileIOFactory();

        this.metadataDir = metadataDir;

        fixLegacyFolder(ctx.pdsFolderResolver().resolveFolders().folderName());
    }

    /**
     * Starts worker thread for async writing of binary metadata.
     */
    void start() throws IgniteCheckedException {
        if (!enabled)
            return;

        U.ensureDirectory(metadataDir, "directory for serialized binary metadata", log);

        writer = new BinaryMetadataAsyncWriter();

        new IgniteThread(writer).start();
    }

    /**
     * Stops worker for async writing of binary metadata.
     */
    void stop() {
        U.cancel(writer);
    }

    /**
     * @param binMeta Binary metadata to be written to disk.
     */
    void writeMetadata(BinaryMetadata binMeta) {
        if (!enabled)
            return;

        try {
            File file = new File(metadataDir, BinaryUtils.binaryMetaFileName(binMeta.typeId()));
            File tmpFile = new File(file.getAbsolutePath() + TMP_SUFFIX);

            // TODO: delete it on Ignite start. https://issues.apache.org/jira/browse/IGNITE-20897
            if (tmpFile.exists())
                U.delete(tmpFile);

            byte[] marshalled = U.marshal(ctx, binMeta);

            try (final FileIO out = fileIOFactory.create(tmpFile)) {
                int left = marshalled.length;
                while ((left -= out.writeFully(marshalled, 0, Math.min(marshalled.length, left))) > 0)
                    ;

                out.force();
            }

            Files.move(tmpFile.toPath(), file.toPath(), ATOMIC_MOVE, REPLACE_EXISTING);
        }
        catch (Exception e) {
            final String msg = "Failed to save metadata for typeId: " + binMeta.typeId() +
                "; exception was thrown: " + e.getMessage();

            U.error(log, msg);

            U.cancel(writer);

            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw new IgniteException(msg, e);
        }
    }

    /**
     * Remove metadata for specified type.
     *
     * @param typeId Type identifier.
     */
    private void removeMeta(int typeId) {
        if (!enabled)
            return;

        ctx.marshallerContext().unregisterClassNameLocally(typeId);

        File file = new File(metadataDir, BinaryUtils.binaryMetaFileName(typeId));

        if (!file.delete()) {
            final String msg = "Failed to remove metadata for typeId: " + typeId;

            U.error(log, msg);

            writer.cancel();

            IgniteException e = new IgniteException(msg);

            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /**
     * Restores metadata on startup of {@link CacheObjectBinaryProcessorImpl} but before starting discovery.
     */
    void restoreMetadata() {
        if (!enabled)
            return;

        for (File file : metadataDir.listFiles(NodeFileTree::notTmpFile))
            restoreMetadata(file);
    }

    /**
     * Restores single type metadata.
     *
     * @param typeId Type identifier.
     */
    void restoreMetadata(int typeId) {
        restoreMetadata(new File(metadataDir, BinaryUtils.binaryMetaFileName(typeId)));
    }

    /** */
    private void restoreMetadata(File file) {
        try (FileInputStream in = new FileInputStream(file)) {
            BinaryMetadata meta = U.unmarshal(ctx.config().getMarshaller(), in, U.resolveClassLoader(ctx.config()));

            metadataLocCache.put(meta.typeId(), new BinaryMetadataHolder(meta, 0, 0));
        }
        catch (Exception e) {
            U.warn(log, "Failed to restore metadata from file: " + file.getName() +
                "; exception was thrown: " + e.getMessage());
        }
    }

    /**
     * Checks if binary metadata for the same typeId is already presented on disk. If so merges it with new metadata and
     * stores the result. Otherwise just writes new metadata.
     *
     * @param binMeta new binary metadata to write to disk.
     */
    void mergeAndWriteMetadata(BinaryMetadata binMeta) {
        BinaryMetadata existingMeta = readMetadata(binMeta.typeId());

        if (existingMeta != null) {
            BinaryMetadata mergedMeta = BinaryUtils.mergeMetadata(existingMeta, binMeta);

            writeMetadata(mergedMeta);
        }
        else
            writeMetadata(binMeta);
    }

    /**
     * Reads binary metadata for given typeId.
     *
     * @param typeId typeId of BinaryMetadata to be read.
     */
    private BinaryMetadata readMetadata(int typeId) {
        File file = new File(metadataDir, BinaryUtils.binaryMetaFileName(typeId));

        if (!file.exists())
            return null;

        try (FileInputStream in = new FileInputStream(file)) {
            return U.unmarshal(ctx.config().getMarshaller(), in, U.resolveClassLoader(ctx.config()));
        }
        catch (Exception e) {
            U.warn(log, "Failed to restore metadata from file: " + file.getName() +
                "; exception was thrown: " + e.getMessage());
        }

        return null;
    }

    /**
     *
     */
    void prepareMetadataWriting(BinaryMetadata meta, int typeVer) {
        if (!enabled)
            return;

        writer.prepareWriteFuture(meta, typeVer);
    }

    /**
     * @param typeId Type ID.
     * @param typeVer Type version.
     */
    void writeMetadataAsync(int typeId, int typeVer) {
        if (!enabled)
            return;

        writer.startTaskAsync(typeId, typeVer);
    }

    /**
     * @param typeId Type ID.
     */
    public void removeMetadataAsync(int typeId) {
        if (!enabled)
            return;

        writer.startTaskAsync(typeId, BinaryMetadataTransport.REMOVED_VERSION);
    }

    /**
     * {@code typeVer} parameter is always non-negative except one special case
     * (see {@link CacheObjectBinaryProcessorImpl#addMeta(int, BinaryType, boolean)} for context):
     * if request for bin meta update arrives right at the moment when node is stopping
     * {@link MetadataUpdateResult} of special type is generated: UPDATE_DISABLED.
     *
     * @param typeId
     * @param typeVer
     * @throws IgniteCheckedException
     */
    void waitForWriteCompletion(int typeId, int typeVer) throws IgniteCheckedException {
        if (!enabled)
            return;

        writer.waitForWriteCompletion(typeId, typeVer);
    }

    /**
     * @param typeId Binary metadata type id.
     * @param typeVer Type version.
     */
    void finishWrite(int typeId, int typeVer) {
        if (!enabled)
            return;

        writer.finishWriteFuture(typeId, typeVer, null);
    }

    /**
     * Try looking for legacy directory with binary metadata and move it to new directory
     */
    private void fixLegacyFolder(String consistendId) throws IgniteCheckedException {
        if (ctx.config().getWorkDirectory() == null)
            return;

        File legacyDir = new File(new File(
            ctx.config().getWorkDirectory(),
            "binary_meta"
        ), consistendId);

        File legacyTmpDir = new File(legacyDir.toString() + TMP_SUFFIX);

        if (legacyTmpDir.exists() && !IgniteUtils.delete(legacyTmpDir))
            throw new IgniteCheckedException("Failed to delete legacy binary metadata dir: "
                + legacyTmpDir.getAbsolutePath());

        if (legacyDir.exists()) {
            try {
                IgniteUtils.copy(legacyDir, metadataDir, true);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to copy legacy binary metadata dir to new location", e);
            }

            try {
                // rename legacy dir so if deletion fails in the middle, we won't be stuck with half-deleted metadata
                Files.move(legacyDir.toPath(), legacyTmpDir.toPath());
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to rename legacy binary metadata dir", e);
            }

            if (!IgniteUtils.delete(legacyTmpDir))
                throw new IgniteCheckedException("Failed to delete legacy binary metadata dir");
        }
    }

    /**
     * @param typeId Type ID.
     */
    void prepareMetadataRemove(int typeId) {
        if (!enabled)
            return;

        writer.cancelTasksForType(typeId);

        writer.prepareRemoveFuture(typeId);
    }

    /** @return {@code True} if file store enabled. */
    public static boolean enabled(IgniteConfiguration cfg) {
        return CU.isPersistenceEnabled(cfg) || CU.isCdcEnabled(cfg);
    }

    /**
     *
     */
    private class BinaryMetadataAsyncWriter extends GridWorker {
        /**
         * Queue of write tasks submitted for execution.
         */
        private final BlockingQueue<OperationTask> queue = new LinkedBlockingQueue<>();

        /**
         * Write operation tasks prepared for writing (but not yet submitted to execution (actual writing).
         */
        private final ConcurrentMap<OperationSyncKey, OperationTask> preparedTasks = new ConcurrentHashMap<>();

        /** */
        BinaryMetadataAsyncWriter() {
            super(ctx.igniteInstanceName(), "binary-metadata-writer",
                BinaryMetadataFileStore.this.log, ctx.workersRegistry());
        }

        /**
         * @param typeId Type ID.
         * @param typeVer Type version.
         */
        synchronized void startTaskAsync(int typeId, int typeVer) {
            if (isCancelled())
                return;

            OperationTask task = preparedTasks.get(new OperationSyncKey(typeId, typeVer));

            if (task != null) {
                if (log.isDebugEnabled())
                    log.debug(
                        "Submitting task for async write for" +
                            " [typeId=" + typeId +
                            ", typeVersion=" + typeVer + ']'
                    );

                queue.add(task);
            }
            else {
                if (log.isDebugEnabled())
                    log.debug(
                        "Task for async write for" +
                            " [typeId=" + typeId +
                            ", typeVersion=" + typeVer + "] not found"
                    );
            }
        }

        /** {@inheritDoc} */
        @Override public synchronized void cancel() {
            super.cancel();

            queue.clear();

            IgniteCheckedException err = new IgniteCheckedException("Operation has been cancelled (node is stopping).");

            for (Map.Entry<OperationSyncKey, OperationTask> e : preparedTasks.entrySet()) {
                if (log.isDebugEnabled())
                    log.debug(
                        "Cancelling future for write operation for" +
                            " [typeId=" + e.getKey().typeId +
                            ", typeVer=" + e.getKey().typeVer + ']'
                    );

                e.getValue().future.onDone(err);
            }

            preparedTasks.clear();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                try {
                    body0();
                }
                catch (InterruptedException e) {
                    if (!isCancelled.get()) {
                        ctx.failure().process(new FailureContext(FailureType.SYSTEM_WORKER_TERMINATION, e));

                        throw e;
                    }
                }
            }
        }

        /** */
        private void body0() throws InterruptedException {
            OperationTask task;

            blockingSectionBegin();

            try {
                task = queue.take();

                if (log.isDebugEnabled())
                    log.debug(
                        "Starting write operation for" +
                            " [typeId=" + task.typeId() +
                            ", typeVer=" + task.typeVersion() + ']'
                    );

                task.execute(BinaryMetadataFileStore.this);
            }
            finally {
                blockingSectionEnd();
            }

            finishWriteFuture(task.typeId(), task.typeVersion(), task);
        }

        /**
         * @param typeId Binary metadata type id.
         */
        synchronized void cancelTasksForType(int typeId) {
            final IgniteCheckedException err = new IgniteCheckedException("Operation has been cancelled by type remove.");

            preparedTasks.entrySet().removeIf(entry -> {
                if (entry.getKey().typeId == typeId) {
                    entry.getValue().future().onDone(err);

                    return true;
                }
                return false;
            });
        }

        /**
         * @param typeId Binary metadata type id.
         * @param typeVer Type version.
         * @param task Task to remove.
         */
        void finishWriteFuture(int typeId, int typeVer, OperationTask task) {
            boolean removed;

            if (task != null)
                removed = preparedTasks.remove(new OperationSyncKey(typeId, typeVer), task);
            else {
                task = preparedTasks.remove(new OperationSyncKey(typeId, typeVer));

                removed = task != null;
            }

            if (removed) {
                if (log.isDebugEnabled())
                    log.debug(
                        "Future for write operation for" +
                            " [typeId=" + typeId +
                            ", typeVer=" + typeVer + ']' +
                            " completed."
                    );

                task.future.onDone();
            }
            else {
                if (log.isDebugEnabled())
                    log.debug(
                        "Future for write operation for" +
                            " [typeId=" + typeId +
                            ", typeVer=" + typeVer + ']' +
                            " not found."
                    );
            }
        }

        /**
         * @param meta Binary metadata.
         * @param typeVer Type version.
         */
        synchronized void prepareWriteFuture(BinaryMetadata meta, int typeVer) {
            if (isCancelled())
                return;

            if (log.isDebugEnabled())
                log.debug(
                    "Prepare task for async write for" +
                        "[typeName=" + meta.typeName() +
                        ", typeId=" + meta.typeId() +
                        ", typeVersion=" + typeVer + ']'
                );

            preparedTasks.putIfAbsent(new OperationSyncKey(meta.typeId(), typeVer), new WriteOperationTask(meta, typeVer));
        }

        /**
         */
        synchronized void prepareRemoveFuture(int typeId) {
            if (isCancelled())
                return;

            if (log.isDebugEnabled())
                log.debug(
                    "Prepare task for async remove for" +
                        "[typeId=" + typeId + ']'
                );

            preparedTasks.putIfAbsent(new OperationSyncKey(typeId, BinaryMetadataTransport.REMOVED_VERSION),
                new RemoveOperationTask(typeId));
        }

        /**
         * @param typeId Type ID.
         * @param typeVer Type version.
         * @throws IgniteCheckedException If write operation failed.
         */
        void waitForWriteCompletion(int typeId, int typeVer) throws IgniteCheckedException {
            //special case, see javadoc of {@link BinaryMetadataFileStore#waitForWriteCompletion}
            if (typeVer == -1) {
                if (log.isDebugEnabled())
                    log.debug("No need to wait for " + typeId + ", negative typeVer was passed.");

                return;
            }

            OperationTask task = preparedTasks.get(new OperationSyncKey(typeId, typeVer));

            if (task != null) {
                if (log.isDebugEnabled())
                    log.debug(
                        "Waiting for write completion of" +
                            " [typeId=" + typeId +
                            ", typeVer=" + typeVer + "]"
                    );

                try {
                    task.future.get();
                }
                finally {
                    if (log.isDebugEnabled())
                        log.debug(
                            "Released for write completion of" +
                                " [typeId=" + typeId +
                                ", typeVer=" + typeVer + ']'
                        );
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug(
                        "Task for async write for" +
                            " [typeId=" + typeId +
                            ", typeVersion=" + typeVer + "] not found"
                    );
            }
        }
    }

    /**
     *
     */
    private abstract static class OperationTask {
        /** */
        private final GridFutureAdapter<Void> future = new GridFutureAdapter<>();

        /** */
        abstract void execute(BinaryMetadataFileStore store);

        /** */
        abstract int typeId();

        /** */
        abstract int typeVersion();

        /**
         * @return Task future.
         */
        GridFutureAdapter<Void> future() {
            return future;
        }
    }

    /**
     *
     */
    private static final class WriteOperationTask extends OperationTask {
        /** */
        private final BinaryMetadata meta;

        /** */
        private final int typeVer;

        /**
         * @param meta Metadata for binary type.
         * @param ver Version of type.
         */
        private WriteOperationTask(BinaryMetadata meta, int ver) {
            this.meta = meta;
            typeVer = ver;
        }

        /** {@inheritDoc} */
        @Override void execute(BinaryMetadataFileStore store) {
            store.writeMetadata(meta);
        }

        /** {@inheritDoc} */
        @Override int typeId() {
            return meta.typeId();
        }

        /** {@inheritDoc} */
        @Override int typeVersion() {
            return typeVer;
        }
    }

    /**
     *
     */
    private static final class RemoveOperationTask extends OperationTask {
        /** */
        private final int typeId;

        /**
         */
        private RemoveOperationTask(int typeId) {
            this.typeId = typeId;
        }

        /** {@inheritDoc} */
        @Override void execute(BinaryMetadataFileStore store) {
            store.removeMeta(typeId);
        }

        /** {@inheritDoc} */
        @Override int typeId() {
            return typeId;
        }

        /** {@inheritDoc} */
        @Override int typeVersion() {
            return BinaryMetadataTransport.REMOVED_VERSION;
        }
    }

    /**
     *
     */
    private static final class OperationSyncKey {
        /** */
        private final int typeId;

        /** */
        private final int typeVer;

        /** */
        private OperationSyncKey(int typeId, int typeVer) {
            this.typeId = typeId;
            this.typeVer = typeVer;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 31 * typeId + typeVer;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (!(obj instanceof OperationSyncKey))
                return false;

            OperationSyncKey that = (OperationSyncKey)obj;

            return (that.typeId == typeId) && (that.typeVer == typeVer);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(OperationSyncKey.class, this);
        }
    }
}
