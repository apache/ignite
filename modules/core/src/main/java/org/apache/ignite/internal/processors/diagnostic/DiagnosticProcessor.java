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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentRouter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.joining;

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

    /** Folder name for store diagnostic info. **/
    public static final String DEFAULT_TARGET_FOLDER = "diagnostic";

    /** Full path for store dubug info. */
    private final Path diagnosticPath;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public DiagnosticProcessor(GridKernalContext ctx) throws IgniteCheckedException {
        super(ctx);

        diagnosticPath = U.resolveWorkDirectory(ctx.config().getWorkDirectory(), DEFAULT_TARGET_FOLDER, false)
            .toPath();
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

        CorruptedTreeException corruptedTreeE = X.cause(failureCtx.error(), CorruptedTreeException.class);

        if (corruptedTreeE != null && !F.isEmpty(corruptedTreeE.pages())) {
            File[] walDirs = walDirs(ctx);

            if (F.isEmpty(walDirs)) {
                if (log.isInfoEnabled())
                    log.info("Skipping dump diagnostic info due to WAL not configured");
            }
            else {
                try {
                    File corruptedPagesFile = corruptedPagesFile(diagnosticPath, corruptedTreeE.pages());

                    String walDirsStr = Arrays.stream(walDirs).map(File::getAbsolutePath)
                        .collect(joining(", ", "[", "]"));

                    log.warning("An " + corruptedTreeE.getClass().getSimpleName() + " has occurred, to diagnose it " +
                        "will need to postpone the directories " + walDirsStr + " before the node is restarted and " +
                        "run the org.apache.ignite.development.utils.IgniteWalConverter with an additional argument " +
                        "\"pages=" + corruptedPagesFile.getAbsolutePath() + "\"");
                }
                catch (Throwable t) {
                    String pages = Arrays.stream(corruptedTreeE.pages())
                        .map(t2 -> "(" + t2.get1() + ',' + t2.get2() + ')').collect(joining("", "[", "]"));

                    log.error("Failed to dump diagnostic info on tree corruption. PageIds=" + pages, t);
                }
            }
        }
    }

    /**
     * Creation and filling of a file with pages that can be corrupted.
     * Pages are written on each line in format "grpId:pageId".
     * File name format "corruptedPages_TIMESTAMP.txt".
     *
     * @param dirPath Path to the directory where the file will be created.
     * @param pages Pages that could be corrupted. Mapping: cache group id -> page id.
     * @return Created and filled file.
     * @throws IOException If an I/O error occurs.
     */
    public static File corruptedPagesFile(Path dirPath, T2<Integer, Long>... pages) throws IOException {
        dirPath.toFile().mkdirs();

        File f = dirPath.resolve("corruptedPages_" + U.currentTimeMillis() + ".txt").toFile();

        assert !f.exists();

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(f))) {
            for (T2<Integer, Long> p : pages) {
                bw.write(p.get1().toString() + ':' + p.get2().toString());

                bw.newLine();
            }

            bw.flush();
        }

        return f;
    }

    /**
     * Getting the WAL directories.
     *
     * @param ctx Kernal context.
     * @return WAL directories.
     */
    @Nullable static File[] walDirs(GridKernalContext ctx) {
        IgniteWriteAheadLogManager walMgr = ctx.cache().context().wal();

        if (walMgr instanceof FileWriteAheadLogManager) {
            SegmentRouter sr = ((FileWriteAheadLogManager)walMgr).getSegmentRouter();

            if (sr != null) {
                File workDir = sr.getWalWorkDir();
                return sr.hasArchive() ? F.asArray(workDir, sr.getWalArchiveDir()) : F.asArray(workDir);
            }
        }

        return null;
    }
}
