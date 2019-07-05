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
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DiagnosticAction.PRINT_TO_FILE;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DiagnosticAction.PRINT_TO_LOG;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DiagnosticAction.PRINT_TO_RAW_FILE;
import static org.apache.ignite.internal.util.IgniteStopwatch.logTime;

/**
 * Processor which contained helper methods for different diagnostic cases.
 */
public class DiagnosticProcessor extends GridProcessorAdapter {
    /** Value of the system property that enables page locks dumping on failure. */
    private static final boolean IGNITE_DUMP_PAGE_LOCK_ON_FAILURE =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_DUMP_PAGE_LOCK_ON_FAILURE, true);

    /** Time formatter for dump file name. */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss_SSS");

    /** Folder name for store diagnostic info. **/
    public static final String DEFAULT_TARGET_FOLDER = "diagnostic";

    /** File format. */
    static final String FILE_FORMAT = ".txt";

    /** Raw file format. */
    static final String RAW_FILE_FORMAT = ".raw";

    /** Full path for store dubug info. */
    private final Path diagnosticPath;

    /** */
    private final PageHistoryDiagnoster pageHistoryDiagnoster;

    /**
     * @param ctx Kernal context.
     */
    public DiagnosticProcessor(GridKernalContext ctx) throws IgniteCheckedException {
        super(ctx);

        diagnosticPath = U.resolveWorkDirectory(ctx.config().getWorkDirectory(), DEFAULT_TARGET_FOLDER, false).toPath();

        pageHistoryDiagnoster = new PageHistoryDiagnoster(ctx, this::diagnosticFile);

    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        super.onKernalStart(active);

        pageHistoryDiagnoster.onStart();
    }

    /**
     * Dump all history caches of given page.
     *
     * @param builder Parameters of dumping.
     * @throws IgniteCheckedException If scanning was failed.
     */
    public void dumpPageHistory(
        @NotNull PageHistoryDiagnoster.DiagnosticPageBuilder builder
    ) throws IgniteCheckedException {
        logTime(log, "DiagnosticPageHistory", () -> pageHistoryDiagnoster.dumpPageHistory(builder));
    }

    /**
     * Print diagnostic info about failure occurred on {@code ignite} instance.
     * Failure details is contained in {@code failureCtx}.
     *
     * @param ignite Ignite instance.
     * @param failureCtx Failure context.
     */
    public void onFailure(Ignite ignite, FailureContext failureCtx) {
        // Dump data structures page locks.
        if (IGNITE_DUMP_PAGE_LOCK_ON_FAILURE)
            ctx.cache().context().diagnostic().pageLockTracker().dumpLocksToLog();

        // If we have some corruption in data structure,
        // we should scan WAL and print to log and save to file all pages related to corruption for
        // future investigation.
        if (X.hasCause(failureCtx.error(), CorruptedTreeException.class)) {
            CorruptedTreeException corruptedTreeException = X.cause(failureCtx.error(), CorruptedTreeException.class);

            T2<Integer, Long>[] pageIds = corruptedTreeException.pages();

            try {
                dumpPageHistory(
                    new PageHistoryDiagnoster.DiagnosticPageBuilder()
                        .pageIds(pageIds)
                        .addAction(PRINT_TO_LOG)
                        .addAction(PRINT_TO_FILE)
                        .addAction(PRINT_TO_RAW_FILE)
                );
            }
            catch (IgniteCheckedException e) {
                SB sb = new SB();
                sb.a("[");

                for (int i = 0; i < pageIds.length; i++)
                    sb.a("(").a(pageIds[i].get1()).a(",").a(pageIds[i].get2()).a(")");

                sb.a("]");

                ignite.log().error(
                    "Failed to dump diagnostic info on tree corruption. PageIds=" + sb, e);
            }
        }
    }

    /**
     * Resolve file to store diagnostic info.
     *
     * @param customFile Custom file if customized.
     * @param writeMode Diagnostic file write mode.
     * @return File to store diagnostic info.
     */
    private File diagnosticFile(File customFile, DiagnosticFileWriteMode writeMode) {
        if (customFile == null)
            return finalizeFile(diagnosticPath, writeMode);

        if (customFile.isAbsolute())
            return finalizeFile(customFile.toPath(), writeMode);

        return finalizeFile(diagnosticPath.resolve(customFile.toPath()), writeMode);
    }

    /**
     * @param diagnosticPath Path to diagnostic file.
     * @param writeMode Diagnostic file write mode.
     * @return File to store diagnostic info.
     */
    private static File finalizeFile(Path diagnosticPath, DiagnosticFileWriteMode writeMode) {
        diagnosticPath.toFile().mkdirs();

        return diagnosticPath.resolve(LocalDateTime.now().format(TIME_FORMATTER) + getFileExtension(writeMode)).toFile();
    }

    /**
     * Get file format for given write mode.
     *
     * @param writeMode Diagnostic file write mode.
     * @return File extention with dot.
     */
    private static String getFileExtension(DiagnosticFileWriteMode writeMode) {
        switch (writeMode) {
            case HUMAN_READABLE:
                return FILE_FORMAT;

            case RAW:
                return RAW_FILE_FORMAT;

            default:
                throw new IllegalArgumentException("writeMode=" + writeMode);
        }
    }

    /**
     * Possible action after WAL scanning.
     */
    public enum DiagnosticAction {
        /** Print result to log. */
        PRINT_TO_LOG,
        /** Print result to file. */
        PRINT_TO_FILE,
        /** Print result to file in raw format. */
        PRINT_TO_RAW_FILE
    }

    /**
     * Mode of diagnostic dump file.
     */
    public enum DiagnosticFileWriteMode {
        /** Use humanly readable data representation. */
        HUMAN_READABLE,
        /** Use raw data format. */
        RAW
    }
}
