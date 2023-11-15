/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.cdc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.cdc.CdcFileLockHolder;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.CdcDisableRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.cdc.CdcConsumerState.WAL_STATE_FILE_NAME;
import static org.apache.ignite.internal.cdc.CdcMain.STATE_DIR;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CDC_DISABLE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER;

/**
 * Task to delete lost segment CDC links before the CDC is disabled or the last gap.
 */
@GridInternal
public class CdcDeleteLostSegmentsTask extends VisorMultiNodeTask<CdcDeleteLostSegmentLinksCommandArg, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<CdcDeleteLostSegmentLinksCommandArg, Void> job(CdcDeleteLostSegmentLinksCommandArg arg) {
        return new CdcDeleteLostSegmentsJob(arg, false);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Void reduce0(List<ComputeJobResult> results) throws IgniteException {
        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                throw new IgniteException("Failed to delete lost segment CDC links on a node " +
                    "[nodeId=" + res.getNode().id() + ']', res.getException());
            }
        }

        return null;
    }

    /** */
    private static class CdcDeleteLostSegmentsJob extends VisorJob<CdcDeleteLostSegmentLinksCommandArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected logger. */
        @LoggerResource
        protected IgniteLogger log;

        /** */
        private transient FileWriteAheadLogManager wal;

        /** */
        private transient File walCdcDir;

        /**
         * Create job with specified argument.
         *
         * @param arg   Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected CdcDeleteLostSegmentsJob(CdcDeleteLostSegmentLinksCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(CdcDeleteLostSegmentLinksCommandArg arg) throws IgniteException {
            wal = (FileWriteAheadLogManager)ignite.context().cache().context().wal(true);

            if (wal == null)
                throw new IgniteException("CDC is not configured.");

            walCdcDir = wal.walCdcDirectory();

            if (walCdcDir == null)
                throw new IgniteException("CDC is not configured.");

            CdcFileLockHolder lock = new CdcFileLockHolder(walCdcDir.getAbsolutePath(), "Delete lost segments job", log);

            try {
                lock.tryLock(1);

                Long lastSegBeforeSkip = findLastSegmentBeforeSkip();

                deleteAllUntil(lastSegBeforeSkip);

                Long cdcDisableSgmnt = findLastSegmentWithCdcDisabledRecord();

                deleteAllUntil(cdcDisableSgmnt);

                if (lastSegBeforeSkip != null || cdcDisableSgmnt != null)
                    deleteCdcWalState();
                else if (log.isInfoEnabled())
                    log.info("Lost segment CDC links or CDC disable record were not found.");
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to delete lost segment CDC links. " +
                    "Unable to acquire lock to lock CDC folder. Make sure a CDC app is shut down " +
                    "[dir=" + walCdcDir.getAbsolutePath() + ", reason=" + e.getMessage() + ']');
            }
            finally {
                U.closeQuiet(lock);
            }

            return null;
        }

        /** @return The index of the segment previous to the last gap or {@code null} if no gaps were found. */
        private Long findLastSegmentBeforeSkip() {
            AtomicReference<Long> lastLostSgmnt = new AtomicReference<>();
            AtomicLong lastSgmnt = new AtomicLong(-1);

            consumeCdcSegments(segment -> {
                if (lastLostSgmnt.get() != null)
                    return;

                long idx = FileWriteAheadLogManager.segmentIndex(segment);

                if (lastSgmnt.get() == -1 || lastSgmnt.get() - idx == 1) {
                    lastSgmnt.set(idx);

                    return;
                }

                if (log.isInfoEnabled())
                    log.info("Found lost segment CDC links [lastLostSgmntIdx=" + idx + ']');

                lastLostSgmnt.set(idx);
            });

            return lastLostSgmnt.get();
        }

        /** @return The index of the segment that contains the last {@link CdcDisableRecord}. */
        private Long findLastSegmentWithCdcDisabledRecord() {
            AtomicReference<Long> lastRec = new AtomicReference<>();

            consumeCdcSegments(segment -> {
                if (lastRec.get() != null)
                    return;

                if (log.isInfoEnabled())
                    log.info("Start process CDC segment [segment=" + segment + ']');

                IgniteWalIteratorFactory.IteratorParametersBuilder builder =
                    new IgniteWalIteratorFactory.IteratorParametersBuilder()
                        .log(log)
                        .sharedContext(ignite.context().cache().context())
                        .filesOrDirs(segment.toFile())
                        .addFilter((type, ptr) -> type == CDC_DISABLE);

                if (ignite.configuration().getDataStorageConfiguration().getPageSize() != 0)
                    builder.pageSize(ignite.configuration().getDataStorageConfiguration().getPageSize());

                try (WALIterator it = new IgniteWalIteratorFactory(log).iterator(builder)) {
                    if (it.hasNext()) {
                        if (log.isInfoEnabled())
                            log.info("Found CDC disable record [ptr=" + it.next().get1() + ']');

                        lastRec.set(FileWriteAheadLogManager.segmentIndex(segment));
                    }
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to read CDC segment [path=" + segment + ']', e);
                }
            });

            return lastRec.get();
        }

        /** */
        private void deleteCdcWalState() {
            Path stateDir = walCdcDir.toPath().resolve(STATE_DIR);

            File state = stateDir.resolve(WAL_STATE_FILE_NAME).toFile();

            if (state.exists() && !state.delete())
                throw new IgniteException("Failed to delete wal state file [file=" + state.getAbsolutePath() + ']');

            if (log.isInfoEnabled())
                log.info("The CDC application WAL state has been deleted.");
        }

        /** Delete all segments with an absolute index less than or equal to the given one. */
        private void deleteAllUntil(Long lastIdx) {
            if (lastIdx == null)
                return;

            consumeCdcSegments(segment -> {
                if (FileWriteAheadLogManager.segmentIndex(segment) > lastIdx)
                    return;

                if (!segment.toFile().delete()) {
                    throw new IgniteException("Failed to delete lost segment CDC link [segment=" +
                        segment.toAbsolutePath() + ']');
                }

                log.info("Segment CDC link deleted [file=" + segment.toAbsolutePath() + ']');
            });
        }

        /** Consume CDC segments in descending order. */
        private void consumeCdcSegments(Consumer<Path> cnsmr) {
            try (Stream<Path> cdcFiles = Files.list(walCdcDir.toPath())) {
                cdcFiles
                    .filter(p -> WAL_SEGMENT_FILE_FILTER.accept(p.toFile()))
                    .sorted(Comparator.comparingLong(FileWriteAheadLogManager::segmentIndex)
                        .reversed()) // Sort by segment index in descending order.
                    .forEach(cnsmr);
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to list CDC links.", e);
            }
        }
    }
}
