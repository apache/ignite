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

package org.apache.ignite.internal.processors.diagnostic;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CorruptedDataStructureException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentRouter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isPersistenceEnabled;

/**
 * Processor which contained helper methods for different diagnostic cases.
 */
public class DiagnosticProcessor extends GridProcessorAdapter {
    /** @see IgniteSystemProperties#IGNITE_DUMP_PAGE_LOCK_ON_FAILURE */
    public static final boolean DFLT_DUMP_PAGE_LOCK_ON_FAILURE = true;

    /** Value of the system property that enables page locks dumping on failure. */
    private static final boolean IGNITE_DUMP_PAGE_LOCK_ON_FAILURE =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_DUMP_PAGE_LOCK_ON_FAILURE,
            DFLT_DUMP_PAGE_LOCK_ON_FAILURE);

    /** Time formatter for dump file name. */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'_'HH-mm-ss_SSS");

    /** Folder name for store diagnostic info. **/
    public static final String DEFAULT_TARGET_FOLDER = "diagnostic";

    /** Full path for store dubug info. */
    private final Path diagnosticPath;

    /** File I/O factory. */
    @Nullable private final FileIOFactory fileIOFactory;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public DiagnosticProcessor(GridKernalContext ctx) throws IgniteCheckedException {
        super(ctx);

        diagnosticPath = U.resolveWorkDirectory(ctx.config().getWorkDirectory(), DEFAULT_TARGET_FOLDER, false)
            .toPath();

        fileIOFactory = isPersistenceEnabled(ctx.config()) ?
            ctx.config().getDataStorageConfiguration().getFileIOFactory() : null;
    }

    /**
     * Print diagnostic info about failure occurred on {@code ignite} instance.
     * Failure details is contained in {@code failureCtx}.
     *
     * @param failureCtx Failure context.
     */
    public void onFailure(FailureContext failureCtx) {
        // Dump data structures page locks.
        if (IGNITE_DUMP_PAGE_LOCK_ON_FAILURE)
            ctx.cache().context().diagnostic().pageLockTracker().dumpLocksToLog();

        CorruptedDataStructureException corruptedDataStructureEx =
            X.cause(failureCtx.error(), CorruptedDataStructureException.class);

        if (corruptedDataStructureEx != null && !F.isEmpty(corruptedDataStructureEx.pageIds()) && fileIOFactory != null) {
            File[] walDirs = walDirs(ctx);

            if (F.isEmpty(walDirs)) {
                if (log.isInfoEnabled())
                    log.info("Skipping dump diagnostic info due to WAL not configured");
            }
            else {
                try {
                    File corruptedPagesFile = corruptedPagesFile(
                        diagnosticPath,
                        fileIOFactory,
                        corruptedDataStructureEx.groupId(),
                        corruptedDataStructureEx.pageIds()
                    );

                    String walDirsStr = Arrays.stream(walDirs).map(File::getAbsolutePath)
                        .collect(joining(", ", "[", "]"));

                    String args = "walDir=" + walDirs[0].getAbsolutePath() + (walDirs.length == 1 ? "" :
                        " walArchiveDir=" + walDirs[1].getAbsolutePath());

                    if (ctx.config().getDataStorageConfiguration().getPageSize() != DFLT_PAGE_SIZE)
                        args += " pageSize=" + ctx.config().getDataStorageConfiguration().getPageSize();

                    args += " pages=" + corruptedPagesFile.getAbsolutePath();

                    log.warning(corruptedDataStructureEx.getClass().getSimpleName() + " has occurred. " +
                        "To diagnose it, make a backup of the following directories: " + walDirsStr + ". " +
                        "Then, run the following command: java -cp <classpath> " +
                        "org.apache.ignite.development.utils.IgniteWalConverter " + args);
                }
                catch (Throwable t) {
                    String pages = LongStream.of(corruptedDataStructureEx.pageIds())
                        .mapToObj(pageId -> corruptedDataStructureEx.groupId() + ":" + pageId)
                        .collect(joining("\n", "", ""));

                    log.error("Failed to dump diagnostic info of partition corruption. Page ids:\n" + pages, t);
                }
            }
        }
    }

    /**
     * Creation and filling of a file with pages that can be corrupted.
     * Pages are written on each line in format "grpId:pageId".
     * File name format "corruptedPages_yyyy-MM-dd'_'HH-mm-ss_SSS.txt".
     *
     * @param dirPath   Path to the directory where the file will be created.
     * @param ioFactory File I/O factory.
     * @param grpId     Cache group id.
     * @param pageIds   PageId's that can be corrupted.
     * @return Created and filled file.
     * @throws IOException If an I/O error occurs.
     */
    public static File corruptedPagesFile(
        Path dirPath,
        FileIOFactory ioFactory,
        int grpId,
        long... pageIds
    ) throws IOException {
        dirPath.toFile().mkdirs();

        File f = dirPath.resolve("corruptedPages_" + LocalDateTime.now().format(TIME_FORMATTER) + ".txt").toFile();

        assert !f.exists();

        try (FileIO fileIO = ioFactory.create(f)) {
            for (long pageId : pageIds) {
                byte[] bytes = (grpId + ":" + pageId + U.nl()).getBytes(UTF_8);

                int left = bytes.length;

                while ((left - fileIO.writeFully(bytes, bytes.length - left, left)) > 0)
                    ;
            }

            fileIO.force();
        }

        return f;
    }

    /**
     * Getting the WAL directories.
     * Note:
     * Index 0: WAL working directory.
     * Index 1: WAL archive directory (may be absent).
     *
     * @param ctx Kernal context.
     * @return WAL directories.
     */
    @Nullable static File[] walDirs(GridKernalContext ctx) {
        IgniteWriteAheadLogManager walMgr = ctx.cache().context().wal();

        NodeFileTree ft = ctx.pdsFolderResolver().fileTree();

        if (walMgr instanceof FileWriteAheadLogManager) {
            SegmentRouter sr = ((FileWriteAheadLogManager)walMgr).getSegmentRouter();

            if (sr != null) {
                return ft.walArchiveEnabled() ? F.asArray(ft.wal(), ft.walArchive()) : F.asArray(ft.wal());
            }
        }

        return null;
    }
}
