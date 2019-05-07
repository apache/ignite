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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentRouter;
import org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandler;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.jetbrains.annotations.NotNull;

import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory
    .IteratorParametersBuilder.withIteratorParameters;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.printToFile;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.printToLog;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.WalScanner.buildWalScanner;

/**
 * Diagnostic WAL page history.
 */
public class PageHistoryDiagnoster {
    /** Kernal context. */
    @GridToStringExclude
    protected final GridKernalContext ctx;
    /** Diagnostic logger. */
    @GridToStringExclude
    protected final IgniteLogger log;

    /** Wal folders to scan. */
    private File[] walFolders;

    /** Function to provide target end file to store diagnostic info. */
    private final Function<File, File> targetFileSupplier;

    /**
     * @param ctx Kernal context.
     * @param supplier Function to provide target end file to store diagnostic info.
     */
    public PageHistoryDiagnoster(GridKernalContext ctx, Function<File, File> supplier) {
        log = ctx.log(getClass());
        this.ctx = ctx;
        targetFileSupplier = supplier;
    }

    /**
     * Do action on start.
     */
    public void onStart() {
        FileWriteAheadLogManager wal = (FileWriteAheadLogManager)ctx.cache().context().wal();

        SegmentRouter segmentRouter = wal.getSegmentRouter();

        if (segmentRouter.hasArchive())
            walFolders = new File[] {segmentRouter.getWalArchiveDir(), segmentRouter.getWalWorkDir()};
        else
            walFolders = new File[] {segmentRouter.getWalWorkDir()};
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
        ScannerHandler action = null;

        for (DiagnosticProcessor.DiagnosticAction act : builder.actions) {
            if (action == null)
                action = toHandler(act, builder.dumpFolder);
            else
                action = action.andThen(toHandler(act, builder.dumpFolder));
        }

        requireNonNull(action, "Should be configured at least one action");

        buildWalScanner(
            withIteratorParameters()
                .log(log)
                .filesOrDirs(walFolders)
        )
            .findAllRecordsFor(
                stream(builder.pageIds.array())
                    .boxed()
                    .collect(toSet())
            )
            .forEach(action);
    }

    /**
     * @param action Action for converting.
     * @param customFile File to store diagnostic info.
     * @return {@link ScannerHandler} for handle records.
     */
    private ScannerHandler toHandler(DiagnosticProcessor.DiagnosticAction action, File customFile) {
        switch (action) {
            case PRINT_TO_LOG:
                return printToLog(log);
            case PRINT_TO_FILE:
                return printToFile(targetFileSupplier.apply(customFile));
            default:
                throw new IllegalArgumentException("Unknown diagnostic action : " + action);
        }
    }

    /**
     * Parameters for diagnostic pages.
     */
    public static class DiagnosticPageBuilder {
        /** Pages for searching in WAL. */
        GridLongList pageIds = GridLongList.asList();
        /** Action after which should be executed after WAL scanning . */
        Set<DiagnosticProcessor.DiagnosticAction> actions = EnumSet.noneOf(DiagnosticProcessor.DiagnosticAction.class);
        /** Folder for dump diagnostic info. */
        File dumpFolder;

        /**
         * @param pageIds Pages for searching in WAL.
         * @return This instance for chaining.
         */
        public DiagnosticPageBuilder pageIds(long... pageIds) {
            this.pageIds = GridLongList.asList(pageIds);

            return this;
        }

        /**
         * @param pageIds Pages for searching in WAL.
         * @return This instance for chaining.
         */
        public DiagnosticPageBuilder pageIds(@NotNull List<Long> pageIds) {
            this.pageIds = GridLongList.asList(pageIds.stream().mapToLong(Long::longValue).toArray());

            return this;
        }

        /**
         * @param action Action after which should be executed after WAL scanning .
         * @return This instance for chaining.
         */
        public DiagnosticPageBuilder addAction(@NotNull DiagnosticProcessor.DiagnosticAction action) {
            this.actions.add(action);

            return this;
        }

        /**
         * @param file Folder for dump diagnostic info.
         * @return This instance for chaining.
         */
        public DiagnosticPageBuilder folderForDump(@NotNull File file) {
            this.dumpFolder = file;

            return this;
        }
    }
}
