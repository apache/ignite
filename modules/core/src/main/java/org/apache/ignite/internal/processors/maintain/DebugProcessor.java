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

package org.apache.ignite.internal.processors.maintain;

import java.io.File;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentRouter;
import org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandler;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder.withIteratorParameters;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.printToFile;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.printToLog;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.WalScanner.buildWalScanner;

/**
 * Processor which contained helper methods for different debug cases.
 */
public class DebugProcessor extends GridProcessorAdapter {
    /** Time formatter for dump file name. */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss_SSS");
    /** Folder name for store debug info. **/
    static final String DEFAULT_TARGET_FOLDER = "debug";

    /** Full path for store dubug info. */
    private final Path debugPath;
    /** Wal folders to scan. */
    private File[] walFolders;

    /**
     * @param ctx Kernal context.
     */
    public DebugProcessor(GridKernalContext ctx) throws IgniteCheckedException {
        super(ctx);

        debugPath = U.resolveWorkDirectory(ctx.config().getWorkDirectory(), DEFAULT_TARGET_FOLDER, false).toPath();

    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        super.onKernalStart(active);

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
    public void dumpPageHistory(@NotNull DebugPageBuilder builder) throws IgniteCheckedException {
        ScannerHandler action = null;

        for (DebugAction mode : builder.actions) {
            if (action == null)
                action = toHandler(mode, builder.dumpFolder);
            else
                action = action.andThen(toHandler(mode, builder.dumpFolder));
        }

        requireNonNull(action, "Should be configured at least one action");

        log.info("Starting to scan : " + builder.pageIds);

        buildWalScanner(withIteratorParameters().log(log).filesOrDirs(walFolders))
            .findAllRecordsFor(builder.pageIds)
            .forEach(action);
    }

    /**
     * @param action Action for converting.
     * @param customFile File to store debug info.
     * @return {@link ScannerHandler} for handle records.
     */
    private ScannerHandler toHandler(DebugAction action, File customFile) {
        switch (action) {
            case PRINT_TO_LOG:
                return printToLog(log);
            case PRINT_TO_FILE:
                return printToFile(debugFile(customFile));
            default:
                throw new IllegalArgumentException("Unknown debug action : " + action);
        }
    }

    /**
     * Resolve file to store debug info.
     *
     * @param customFile Custom file if customized.
     * @return File to store debug info.
     */
    private File debugFile(File customFile) {
        if (customFile == null)
            return finilizeFile(debugPath);

        if (customFile.isAbsolute())
            return finilizeFile(customFile.toPath());

        return finilizeFile(debugPath.resolve(customFile.toPath()));
    }

    /**
     * @param debugPath Path to debug file.
     * @return File to store debug info.
     */
    private File finilizeFile(Path debugPath) {
        debugPath.toFile().mkdirs();

        return debugPath.resolve(LocalDateTime.now().format(TIME_FORMATTER) + ".txt").toFile();
    }

    /**
     * Parameters for debug pages.
     */
    public static class DebugPageBuilder {
        /** Pages for searching in WAL. */
        List<Long> pageIds = new ArrayList<>();
        /** Action after which should be executed after WAL scanning . */
        Set<DebugAction> actions = new HashSet<>();
        /** Folder for dump debug info. */
        File dumpFolder;

        /**
         * @param pageIds Pages for searching in WAL.
         * @return This instance for chaining.
         */
        public DebugPageBuilder pageIds(long... pageIds) {
            this.pageIds = stream(pageIds).boxed().collect(toList());

            return this;
        }

        /**
         * @param pageIds Pages for searching in WAL.
         * @return This instance for chaining.
         */
        public DebugPageBuilder pageIds(@NotNull List<Long> pageIds) {
            this.pageIds = pageIds;

            return this;
        }

        /**
         * @param action Action after which should be executed after WAL scanning .
         * @return This instance for chaining.
         */
        public DebugPageBuilder addAction(@NotNull DebugAction action) {
            this.actions.add(action);

            return this;
        }

        /**
         * @param file Folder for dump debug info.
         * @return This instance for chaining.
         */
        public DebugPageBuilder folderForDump(@NotNull File file) {
            this.dumpFolder = file;

            return this;
        }
    }

    /**
     * Possible action after WAL scanning.
     */
    public enum DebugAction {
        /** Print result to log. */
        PRINT_TO_LOG,
        /** Print result to file. */
        PRINT_TO_FILE
    }
}
