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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.cdc.CdcFileLockHolder;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.cdc.CdcConsumerState.WAL_STATE_FILE_NAME;
import static org.apache.ignite.internal.cdc.CdcMain.STATE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER;

/**
 * Task to delete lost segment CDC links.
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
            File walCdcDir = ignite.context().pdsFolderResolver().resolveDirectories().walCdc();

            if (walCdcDir == null)
                throw new IgniteException("CDC is not configured.");

            CdcFileLockHolder lock = new CdcFileLockHolder(walCdcDir.getAbsolutePath(), "Delete lost segments job", log);

            try {
                lock.tryLock(1);

                try (Stream<Path> cdcFiles = Files.list(walCdcDir.toPath())) {
                    Set<File> delete = new HashSet<>();

                    AtomicLong lastSgmnt = new AtomicLong(-1);

                    cdcFiles
                        .filter(p -> WAL_SEGMENT_FILE_FILTER.accept(p.toFile()))
                        .sorted(Comparator.comparingLong(FileWriteAheadLogManager::segmentIndex)
                            .reversed()) // Sort by segment index.
                        .forEach(path -> {
                            long idx = FileWriteAheadLogManager.segmentIndex(path);

                            if (lastSgmnt.get() == -1 || lastSgmnt.get() - idx == 1) {
                                lastSgmnt.set(idx);

                                return;
                            }

                            delete.add(path.toFile());
                        });

                    if (delete.isEmpty()) {
                        log.info("Lost segment CDC links were not found.");

                        return null;
                    }

                    log.info("Found lost segment CDC links. The following links will be deleted: " + delete);

                    delete.forEach(file -> {
                        if (!file.delete()) {
                            throw new IgniteException("Failed to delete lost segment CDC link [file=" +
                                file.getAbsolutePath() + ']');
                        }

                        log.info("Segment CDC link deleted [file=" + file.getAbsolutePath() + ']');
                    });

                    Path stateDir = walCdcDir.toPath().resolve(STATE_DIR);

                    if (stateDir.toFile().exists()) {
                        File walState = stateDir.resolve(WAL_STATE_FILE_NAME).toFile();

                        if (walState.exists() && !walState.delete()) {
                            throw new IgniteException("Failed to delete wal state file [file=" +
                                walState.getAbsolutePath() + ']');
                        }
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to delete lost segment CDC links.", e);
                }
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException("Failed to delete lost segment CDC links. " +
                    "Unable to acquire lock to lock CDC folder. Make sure a CDC app is shut down " +
                    "[dir=" + walCdcDir.getAbsolutePath() + ", reason=" + e.getMessage() + ']');
            }
            finally {
                U.closeQuiet(lock);
            }

            return null;
        }
    }
}
