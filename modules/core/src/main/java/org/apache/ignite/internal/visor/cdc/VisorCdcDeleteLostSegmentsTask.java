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

package org.apache.ignite.internal.visor.cdc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.cdc.CdcConsumerState;
import org.apache.ignite.internal.cdc.CdcFileLockHolder;
import org.apache.ignite.internal.management.cdc.CdcDeleteLostSegmentLinksCommandArg;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.CdcDisabledRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.cdc.CdcConsumerState.WAL_STATE_FILE_NAME;
import static org.apache.ignite.internal.cdc.CdcMain.STATE_DIR;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CDC_DISABLED;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER;

/**
 * Task to delete lost segment CDC links. For in-memory CDC, also resets the state to the last {@link CdcDisabledRecord}.
 */
@GridInternal
public class VisorCdcDeleteLostSegmentsTask extends VisorMultiNodeTask<CdcDeleteLostSegmentLinksCommandArg, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<CdcDeleteLostSegmentLinksCommandArg, Void> job(CdcDeleteLostSegmentLinksCommandArg arg) {
        return new VisorCdcDeleteLostSegmentsJob(arg, false);
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
    private static class VisorCdcDeleteLostSegmentsJob extends VisorJob<CdcDeleteLostSegmentLinksCommandArg, Void> {
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
        protected VisorCdcDeleteLostSegmentsJob(CdcDeleteLostSegmentLinksCommandArg arg, boolean debug) {
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

                boolean lostDeleted = deleteLostSegments();

                WALPointer lastCdcDisabledRec = findLastCdcDisabledRecord();

                if (lastCdcDisabledRec != null)
                    setWalState(lastCdcDisabledRec.next()); // Reset WAL state to the next record.
                else if (lostDeleted)
                    setWalState(null); // Delete WAL state.
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

        /** @return {@code True} if lost segments were found and successfully deleted. */
        private boolean deleteLostSegments() {
            Set<File> delete = new TreeSet<>();

            AtomicLong lastSgmnt = new AtomicLong(-1);

            consumeCdcSegments(segment -> {
                long idx = FileWriteAheadLogManager.segmentIndex(segment);

                if (lastSgmnt.get() == -1 || lastSgmnt.get() - idx == 1) {
                    lastSgmnt.set(idx);

                    return;
                }

                delete.add(segment.toFile());
            });

            if (delete.isEmpty()) {
                log.info("Lost segment CDC links were not found.");

                return false;
            }

            log.info("Found lost segment CDC links. The following links will be deleted: " + delete);

            delete.forEach(file -> {
                if (!file.delete()) {
                    throw new IgniteException("Failed to delete lost segment CDC link [file=" +
                        file.getAbsolutePath() + ']');
                }

                log.info("Segment CDC link deleted [file=" + file.getAbsolutePath() + ']');
            });

            return true;
        }

        /** @return WAL pointer to the last {@link CdcDisabledRecord}. */
        private WALPointer findLastCdcDisabledRecord() {
            if (!wal.inMemoryCdc())
                return null;

            AtomicReference<WALPointer> lastRec = new AtomicReference<>();

            consumeCdcSegments(segment -> {
                if (lastRec.get() != null)
                    return;

                if (log.isInfoEnabled())
                    log.info("Processing CDC segment [segment=" + segment + ']');

                IgniteWalIteratorFactory.IteratorParametersBuilder builder =
                    new IgniteWalIteratorFactory.IteratorParametersBuilder()
                        .log(log)
                        .sharedContext(ignite.context().cache().context())
                        .filesOrDirs(segment.toFile())
                        .addFilter((type, ptr) -> type == CDC_DISABLED);

                if (ignite.configuration().getDataStorageConfiguration().getPageSize() != 0)
                    builder.pageSize(ignite.configuration().getDataStorageConfiguration().getPageSize());

                try (WALIterator it = new IgniteWalIteratorFactory(log).iterator(builder)) {
                    while (it.hasNext())
                        lastRec.set(it.next().getKey());
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to read CDC segment [path=" + segment + ']', e);
                }
            });

            if (log.isInfoEnabled() && lastRec.get() != null)
                log.info("Found CDC disabled record [ptr=" + lastRec.get() + ']');

            return lastRec.get();
        }

        /** @param ptr WAL pointer to set state or {@code null} to delete state. */
        private void setWalState(WALPointer ptr) {
            Path stateDir = walCdcDir.toPath().resolve(STATE_DIR);

            if (ptr == null) {
                File state = stateDir.resolve(WAL_STATE_FILE_NAME).toFile();

                if (state.exists() && !state.delete())
                    throw new IgniteException("Failed to delete wal state file [file=" + state.getAbsolutePath() + ']');

                return;
            }

            CdcConsumerState state = new CdcConsumerState(log, stateDir);

            try {
                Files.createDirectories(stateDir);

                state.saveWal(new T2<>(ptr, 0));
            }
            catch (IOException e) {
                throw new IgniteException("Failed to set WAL state file.", e);
            }
        }

        /** Consume CDC segments in reversed order. */
        private void consumeCdcSegments(Consumer<Path> cnsmr) {
            try (Stream<Path> cdcFiles = Files.list(walCdcDir.toPath())) {
                cdcFiles
                    .filter(p -> WAL_SEGMENT_FILE_FILTER.accept(p.toFile()))
                    .sorted(Comparator.comparingLong(FileWriteAheadLogManager::segmentIndex)
                        .reversed()) // Sort by segment index.
                    .forEach(cnsmr);
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to list CDC links.", e);
            }
        }
    }
}
