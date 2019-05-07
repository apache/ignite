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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.util.IgniteStopwatch.logTime;

/**
 * Processor which contained helper methods for different diagnostic cases.
 */
public class DiagnosticProcessor extends GridProcessorAdapter {
    /** Time formatter for dump file name. */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss_SSS");
    /** Folder name for store diagnostic info. **/
    static final String DEFAULT_TARGET_FOLDER = "diagnostic";
    /** File format. */
    static final String FILE_FORMAT = ".txt";
    /** Full path for store dubug info. */
    private final Path diagnosticPath;

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
     * Resolve file to store diagnostic info.
     *
     * @param customFile Custom file if customized.
     * @return File to store diagnostic info.
     */
    private File diagnosticFile(File customFile) {
        if (customFile == null)
            return finalizeFile(diagnosticPath);

        if (customFile.isAbsolute())
            return finalizeFile(customFile.toPath());

        return finalizeFile(diagnosticPath.resolve(customFile.toPath()));
    }

    /**
     * @param diagnosticPath Path to diagnostic file.
     * @return File to store diagnostic info.
     */
    private static File finalizeFile(Path diagnosticPath) {
        diagnosticPath.toFile().mkdirs();

        return diagnosticPath.resolve(LocalDateTime.now().format(TIME_FORMATTER) + FILE_FORMAT).toFile();
    }

    /**
     * Possible action after WAL scanning.
     */
    public enum DiagnosticAction {
        /** Print result to log. */
        PRINT_TO_LOG,
        /** Print result to file. */
        PRINT_TO_FILE
    }
}
