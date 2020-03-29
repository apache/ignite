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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

/**
 * Class handles saving/restoring binary metadata to/from disk.
 *
 * Current implementation needs to be rewritten as it issues IO operations from discovery thread which may lead to
 * segmentation of nodes from cluster.
 */
class BinaryMetadataFileStore {
    /** Link to resolved binary metadata directory. Null for non persistent mode */
    private File workDir;

    /** */
    private final ConcurrentMap<Integer, BinaryMetadataHolder> metadataLocCache;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final boolean isPersistenceEnabled;

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
     * @param binaryMetadataFileStoreDir Path to binary metadata store configured by user, should include binary_meta
     * and consistentId
     */
    BinaryMetadataFileStore(
        final ConcurrentMap<Integer, BinaryMetadataHolder> metadataLocCache,
        final GridKernalContext ctx,
        final IgniteLogger log,
        @Nullable final File binaryMetadataFileStoreDir
    ) throws IgniteCheckedException {
        this.metadataLocCache = metadataLocCache;
        this.ctx = ctx;
        this.isPersistenceEnabled = CU.isPersistenceEnabled(ctx.config());
        this.log = log;

        if (!CU.isPersistenceEnabled(ctx.config()))
            return;

        fileIOFactory = ctx.config().getDataStorageConfiguration().getFileIOFactory();

        if (binaryMetadataFileStoreDir != null)
            workDir = binaryMetadataFileStoreDir;
        else {
            final String subFolder = ctx.pdsFolderResolver().resolveFolders().folderName();

            workDir = new File(U.resolveWorkDirectory(
                ctx.config().getWorkDirectory(),
                "binary_meta",
                false
            ),
                subFolder);
        }

        U.ensureDirectory(workDir, "directory for serialized binary metadata", log);

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
        if (!isPersistenceEnabled)
            return;

        try {
            File file = new File(workDir, binMeta.typeId() + ".bin");

            byte[] marshalled = U.marshal(ctx, binMeta);

            try (final FileIO out = fileIOFactory.create(file)) {
                int left = marshalled.length;
                while ((left -= out.writeFully(marshalled, 0, Math.min(marshalled.length, left))) > 0)
                    ;

                out.force();
            }
        }
        catch (Exception e) {
            final String msg = "Failed to save metadata for typeId: " + binMeta.typeId() +
                "; exception was thrown: " + e.getMessage();

            U.error(log, msg);

            writer.cancel();

            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw new IgniteException(msg, e);
        }
    }

    /**
     * Restores metadata on startup of {@link CacheObjectBinaryProcessorImpl} but before starting discovery.
     */
    void restoreMetadata() {
        if (!isPersistenceEnabled)
            return;

        for (File file : workDir.listFiles()) {
            try (FileInputStream in = new FileInputStream(file)) {
                BinaryMetadata meta = U.unmarshal(ctx.config().getMarshaller(), in, U.resolveClassLoader(ctx.config()));

                metadataLocCache.put(meta.typeId(), new BinaryMetadataHolder(meta, 0, 0));
            }
            catch (Exception e) {
                U.warn(log, "Failed to restore metadata from file: " + file.getName() +
                    "; exception was thrown: " + e.getMessage());
            }
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
        File file = new File(workDir, Integer.toString(typeId) + ".bin");

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
        if (!isPersistenceEnabled)
            return;

        writer.prepareWriteFuture(meta, typeVer);
    }

    /**
     * @param typeId Type ID.
     * @param typeVer Type version.
     */
    void writeMetadataAsync(int typeId, int typeVer) {
        if (!isPersistenceEnabled)
            return;

        writer.startWritingAsync(typeId, typeVer);
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
        if (!isPersistenceEnabled)
            return;

        writer.waitForWriteCompletion(typeId, typeVer);
    }

    /**
     * @param typeId Binary metadata type id.
     * @param typeVer Type version.
     */
    void finishWrite(int typeId, int typeVer) {
        if (!isPersistenceEnabled)
            return;

        writer.finishWriteFuture(typeId, typeVer);
    }

    /**
     *
     */
    private class BinaryMetadataAsyncWriter extends GridWorker {
        /**
         * Queue of write tasks submitted for execution.
         */
        private final BlockingQueue<WriteOperationTask> queue = new LinkedBlockingQueue<>();

        /**
         * Write operation tasks prepared for writing (but not yet submitted to execution (actual writing).
         */
        private final ConcurrentMap<OperationSyncKey, WriteOperationTask> preparedWriteTasks = new ConcurrentHashMap<>();

        /** */
        BinaryMetadataAsyncWriter() {
            super(ctx.igniteInstanceName(), "binary-metadata-writer", BinaryMetadataFileStore.this.log, ctx.workersRegistry());
        }

        /**
         * @param typeId Type ID.
         * @param typeVer Type version.
         */
        synchronized void startWritingAsync(int typeId, int typeVer) {
            if (isCancelled())
                return;

            WriteOperationTask task = preparedWriteTasks.get(new OperationSyncKey(typeId, typeVer));

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

            for (Map.Entry<OperationSyncKey, WriteOperationTask> e : preparedWriteTasks.entrySet()) {
                if (log.isDebugEnabled())
                    log.debug(
                        "Cancelling future for write operation for" +
                            " [typeId=" + e.getKey().typeId +
                            ", typeVer=" + e.getKey().typeVer + ']'
                    );

                e.getValue().future.onDone(err);
            }

            preparedWriteTasks.clear();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                try {
                    body0();
                }
                catch (InterruptedException e) {
                    if (!isCancelled) {
                        ctx.failure().process(new FailureContext(FailureType.SYSTEM_WORKER_TERMINATION, e));

                        throw e;
                    }
                }
            }
        }

        /** */
        private void body0() throws InterruptedException {
            WriteOperationTask task;

            blockingSectionBegin();

            try {
                task = queue.take();

                if (log.isDebugEnabled())
                    log.debug(
                        "Starting write operation for" +
                            " [typeId=" + task.meta.typeId() +
                            ", typeVer=" + task.typeVer + ']'
                    );

                writeMetadata(task.meta);
            }
            finally {
                blockingSectionEnd();
            }

            finishWriteFuture(task.meta.typeId(), task.typeVer);
        }

        /**
         * @param typeId Binary metadata type id.
         * @param typeVer Type version.
         */
        void finishWriteFuture(int typeId, int typeVer) {
            WriteOperationTask task = preparedWriteTasks.remove(new OperationSyncKey(typeId, typeVer));

            if (task != null) {
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

            preparedWriteTasks.putIfAbsent(new OperationSyncKey(meta.typeId(), typeVer), new WriteOperationTask(meta, typeVer));
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

            WriteOperationTask task = preparedWriteTasks.get(new OperationSyncKey(typeId, typeVer));

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
    private static final class WriteOperationTask {
        /** */
        private final BinaryMetadata meta;

        /** */
        private final int typeVer;

        /** */
        private final GridFutureAdapter future = new GridFutureAdapter();

        /** */
        private WriteOperationTask(BinaryMetadata meta, int ver) {
            this.meta = meta;
            typeVer = ver;
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
