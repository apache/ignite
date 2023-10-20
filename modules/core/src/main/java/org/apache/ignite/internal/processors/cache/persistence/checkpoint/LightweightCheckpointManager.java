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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.LongJVMPauseDetector;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CHECKPOINT_READ_LOCK_TIMEOUT;

/**
 * Like a sharp checkpoint algorithm implemented in {@link CheckpointManager} this checkpoint ensures that
 * all pages marked dirty under {@link #checkpointTimeoutLock()} will be consistently saved to disk.
 *
 * But unlike {@link CheckpointManager} lightweight checkpoint doesn't store any checkpoint markers to disk
 * nor write cp-related records to WAL log.
 *
 * This allows to use it in situations where no recovery is needed after crush in the middle of checkpoint
 * but work can simply be replayed from the beginning.
 *
 * Such situations include defragmentation and node recovery after crush
 * (regular sharp checkpoint cannot be used during recovery).
 */
public class LightweightCheckpointManager {
    /** Checkpoint worker. */
    private volatile Checkpointer checkpointer;

    /** Main checkpoint steps. */
    private final CheckpointWorkflow checkpointWorkflow;

    /** Timeout checkpoint lock which should be used while write to memory happened. */
    final CheckpointTimeoutLock checkpointTimeoutLock;

    /** Checkpoint page writer factory. */
    private final CheckpointPagesWriterFactory checkpointPagesWriterFactory;

    /** Checkpointer builder. It allows to create a new checkpointer on each call. */
    private final Supplier<Checkpointer> checkpointerProvider;

    /**
     * @param logger Logger producer.
     * @param igniteInstanceName Ignite instance name.
     * @param checkpointThreadName Name of main checkpoint thread.
     * @param workersRegistry Workers registry.
     * @param persistenceCfg Persistence configuration.
     * @param dataRegions Data regions.
     * @param pageMemoryGroupResolver Page memory resolver.
     * @param throttlingPolicy Throttling policy.
     * @param persStoreMetrics Persistence metrics.
     * @param longJvmPauseDetector Long JVM pause detector.
     * @param failureProcessor Failure processor.
     * @param cacheProcessor Cache processor.
     */
    public LightweightCheckpointManager(
        Function<Class<?>, IgniteLogger> logger,
        String igniteInstanceName,
        String checkpointThreadName,
        WorkersRegistry workersRegistry,
        DataStorageConfiguration persistenceCfg,
        Supplier<Collection<DataRegion>> dataRegions,
        IgniteThrowableFunction<Integer, PageMemoryEx> pageMemoryGroupResolver,
        PageMemoryImpl.ThrottlingPolicy throttlingPolicy,
        DataStorageMetricsImpl persStoreMetrics,
        LongJVMPauseDetector longJvmPauseDetector,
        FailureProcessor failureProcessor,
        GridCacheProcessor cacheProcessor
    ) {
        CheckpointReadWriteLock lock = new CheckpointReadWriteLock(logger);

        checkpointWorkflow = new CheckpointWorkflow(
            logger,
            null,
            null,
            lock,
            persistenceCfg.getCheckpointWriteOrder(),
            dataRegions,
            Collections::emptyList,
            persistenceCfg.getCheckpointThreads(),
            igniteInstanceName
        );

        ThreadLocal<ByteBuffer> threadBuf = new ThreadLocal<ByteBuffer>() {
            /** {@inheritDoc} */
            @Override protected ByteBuffer initialValue() {
                ByteBuffer tmpWriteBuf = ByteBuffer.allocateDirect(persistenceCfg.getPageSize());

                tmpWriteBuf.order(ByteOrder.nativeOrder());

                return tmpWriteBuf;
            }
        };

        checkpointPagesWriterFactory = new CheckpointPagesWriterFactory(
            cacheProcessor.context().kernalContext(),
            logger,
            (pageMemEx, fullPage, buf, tag) ->
                pageMemEx.pageManager().write(fullPage.groupId(), fullPage.pageId(), buf, tag, true),
            persStoreMetrics,
            throttlingPolicy,
            threadBuf,
            pageMemoryGroupResolver
        );

        checkpointerProvider = () -> new Checkpointer(
            igniteInstanceName,
            checkpointThreadName,
            workersRegistry,
            logger,
            longJvmPauseDetector,
            failureProcessor,
            persStoreMetrics,
            cacheProcessor,
            checkpointWorkflow,
            checkpointPagesWriterFactory,
            null,
            persistenceCfg.getCheckpointFrequency(),
            persistenceCfg.getCheckpointThreads(),
            () -> 0
        );

        checkpointer = checkpointerProvider.get();

        Long cfgCheckpointReadLockTimeout = persistenceCfg != null
            ? persistenceCfg.getCheckpointReadLockTimeout()
            : null;

        long checkpointReadLockTimeout = IgniteSystemProperties.getLong(IGNITE_CHECKPOINT_READ_LOCK_TIMEOUT,
            cfgCheckpointReadLockTimeout != null
                ? cfgCheckpointReadLockTimeout
                : workersRegistry.getSystemWorkerBlockedTimeout());

        checkpointTimeoutLock = new CheckpointTimeoutLock(
            logger,
            failureProcessor,
            dataRegions,
            lock,
            checkpointer,
            checkpointReadLockTimeout
        );
    }

    /**
     * @return Checkpoint lock which can be used for protection of writing to memory.
     */
    public CheckpointTimeoutLock checkpointTimeoutLock() {
        return checkpointTimeoutLock;
    }

    /**
     * Replace thread local with buffers. Thread local should provide direct buffer with one page in length.
     *
     * @param threadBuf new thread-local with buffers for the checkpoint threads.
     */
    public void threadBuf(ThreadLocal<ByteBuffer> threadBuf) {
        checkpointPagesWriterFactory.threadBuf(threadBuf);
    }

    /**
     * @param lsnr Listener.
     * @param dataRegion
     */
    public void addCheckpointListener(CheckpointListener lsnr, DataRegion dataRegion) {
        checkpointWorkflow.addCheckpointListener(lsnr, dataRegion);
    }

    /**
     * @param lsnr Listener.
     */
    public void removeCheckpointListener(CheckpointListener lsnr) {
        checkpointWorkflow.removeCheckpointListener(lsnr);
    }

    /**
     * @param memoryRecoveryRecordPtr Memory recovery record pointer.
     */
    public void memoryRecoveryRecordPtr(WALPointer memoryRecoveryRecordPtr) {
        checkpointWorkflow.memoryRecoveryRecordPtr(memoryRecoveryRecordPtr);
    }

    /**
     * Start the new checkpoint immediately.
     *
     * @param reason Reason.
     * @param lsnr Listener which will be called on finish.
     * @return Triggered checkpoint progress.
     */
    public <R> CheckpointProgress forceCheckpoint(
        String reason,
        IgniteInClosure<? super IgniteInternalFuture<R>> lsnr
    ) {
        Checkpointer cp = this.checkpointer;

        if (cp == null)
            return null;

        return cp.scheduleCheckpoint(0, reason, lsnr);
    }

    /**
     *
     */
    public Checkpointer getCheckpointer() {
        return checkpointer;
    }

    /**
     * @param context Group context. Can be {@code null} in case of crash recovery.
     * @param groupId Group ID.
     * @param partId Partition ID.
     */
    public void schedulePartitionDestroy(@Nullable CacheGroupContext context, int groupId, int partId) {
        Checkpointer cp = checkpointer;

        if (cp != null)
            cp.schedulePartitionDestroy(context, groupId, partId);
    }

    /**
     * For test use only.
     */
    public IgniteInternalFuture<Void> enableCheckpoints(boolean enable) {
        return checkpointer.enableCheckpoints(enable);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void finalizeCheckpointOnRecovery(
        long ts,
        UUID id,
        WALPointer ptr,
        StripedExecutor exec
    ) throws IgniteCheckedException {
        assert checkpointer != null : "Checkpointer hasn't initialized yet";

        checkpointer.finalizeCheckpointOnRecovery(ts, id, ptr, exec);
    }

    /**
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @return {@code True} if the request to destroy the partition was canceled.
     */
    public boolean cancelOrWaitPartitionDestroy(int grpId, int partId) throws IgniteCheckedException {
        Checkpointer cp = checkpointer;

        return cp != null && cp.cancelOrWaitPartitionDestroy(grpId, partId);
    }

    /**
     * @param cancel Cancel flag.
     */
    public void stop(boolean cancel) {
        checkpointTimeoutLock.stop();

        Checkpointer cp = this.checkpointer;

        if (cp != null)
            cp.shutdownCheckpointer(cancel);

        checkpointWorkflow.stop();

        this.checkpointer = null;
    }

    /**
     * Initialize the checkpoint and prepare it to work. It should be called if the stop was called before.
     */
    public void init() {
        if (this.checkpointer == null) {
            checkpointWorkflow.start();

            this.checkpointer = checkpointerProvider.get();
        }
    }

    /**
     * Checkpoint starts to do their work after this method.
     */
    public void start() {
        assert checkpointer != null : "Checkpointer can't be null during the start";

        this.checkpointer.start();
    }

    /**
     * Checkpoint lock blocks when stop method is called. This method allows continuing the work with a checkpoint lock
     * if needed.
     */
    public void unblockCheckpointLock() {
        checkpointTimeoutLock.start();
    }
}
