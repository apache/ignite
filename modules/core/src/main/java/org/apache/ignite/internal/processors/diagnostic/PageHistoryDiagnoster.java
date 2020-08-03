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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentRouter;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandler;
import org.apache.ignite.internal.processors.cache.persistence.wal.scanner.WalScanner.ScanTerminateStep;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DiagnosticFileWriteMode;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder.withIteratorParameters;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.printRawToFile;
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
    private final BiFunction<File, DiagnosticFileWriteMode, File> targetFileSupplier;

    private final IgniteWalIteratorFactory iteratorFactory = new IgniteWalIteratorFactory();

    /** */
    private volatile FileWriteAheadLogManager wal;

    /**
     * @param ctx Kernal context.
     * @param supplier Function to provide target end file to store diagnostic info.
     */
    public PageHistoryDiagnoster(GridKernalContext ctx, BiFunction<File, DiagnosticFileWriteMode, File> supplier) {
        log = ctx.log(getClass());
        this.ctx = ctx;
        targetFileSupplier = supplier;
    }

    /**
     * Do action on start.
     */
    public void onStart() {
        FileWriteAheadLogManager wal = (FileWriteAheadLogManager)ctx.cache().context().wal();

        if (wal == null)
            return;

        this.wal = wal;

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
        if (walFolders == null) {
            if (log.isInfoEnabled())
                log.info("Skipping dump page history due to WAL not configured");

            return;
        }

        ScannerHandler action = null;

        for (DiagnosticProcessor.DiagnosticAction act : builder.actions) {
            if (action == null)
                action = toHandler(act, builder.dumpFolder);
            else
                action = action.andThen(toHandler(act, builder.dumpFolder));
        }

        requireNonNull(action, "Should be configured at least one action");

        IteratorParametersBuilder params = withIteratorParameters()
            .log(log)
            .filesOrDirs(walFolders);

        // Resolve available WAL segment files.
        List<FileDescriptor> descs = iteratorFactory.resolveWalFiles(params);

        int descIdx = -1;
        FileWALPointer reserved = null;

        for (int i = 0; i < descs.size(); i++) {
            // Try resever minimal available segment.
            if (wal.reserve(reserved = new FileWALPointer(descs.get(i).idx(), 0, 0))) {
                descIdx = i;

                break;
            }
        }

        if (descIdx == -1) {
            if (log.isInfoEnabled())
                log.info("Skipping dump page history due to can not reserve WAL segments: " + descToString(descs));

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Reserverd WAL segment idx: " + reserved.index());

        // Check gaps in the reserved interval.
        List<T2<Long, Long>> gaps = iteratorFactory.hasGaps(descs.subList(descIdx, descs.size()));

        if (!gaps.isEmpty())
            log.warning("Potentialy missed record because WAL has gaps: " + gapsToString(gaps));

        try {
            scan(builder, params, action, reserved);
        }
        finally {
            assert reserved != null;

            wal.release(reserved);

            if (log.isDebugEnabled())
                log.debug("Release WAL segment idx:" + reserved.index());
        }
    }

    /**
     * @param builder Diagnostic parameter builder.
     * @param params Iterator parameter builder.
     * @param action Action.
     * @param from Pointer from replay.
     */
    private void scan(
        PageHistoryDiagnoster.DiagnosticPageBuilder builder,
        IteratorParametersBuilder params,
        ScannerHandler action,
        FileWALPointer from
    ) throws IgniteCheckedException {
        // Try scan via WAL manager. More safety way on working node.
        try {
            buildWalScanner(wal.replay(from))
                .findAllRecordsFor(builder.pageIds)
                .forEach(action);

            return;

        }
        catch (IgniteCheckedException e) {
            log.warning("Failed to diagnosric scan via WAL manager", e);
        }

        // Try scan via stand alone iterator is not safety if wal still generated and moving to archive.
        // Build scanner for pageIds from reserved pointer.
        ScanTerminateStep scanner = buildWalScanner(params.from(from)).findAllRecordsFor(builder.pageIds);

        scanner.forEach(action);
    }

    /**
     * @param descs WAL file descriptors.
     * @return String representation.
     */
    private String descToString(List<FileDescriptor> descs) {
        StringBuilder sb = new StringBuilder();

        sb.append("[");

        Iterator<FileDescriptor> iter = descs.iterator();

        while (iter.hasNext()) {
            FileDescriptor desc = iter.next();

            sb.append(desc.idx());

            if (!iter.hasNext())
                sb.append(", ");
        }

        sb.append("]");

        return sb.toString();
    }

    /**
     * @param gaps WAL file gaps.
     * @return String representation.
     */
    private String gapsToString(Collection<T2<Long, Long>> gaps) {
        StringBuilder sb = new StringBuilder();

        sb.append("[");

        Iterator<T2<Long, Long>> iter = gaps.iterator();

        while (iter.hasNext()) {
            T2<Long, Long> gap = iter.next();

            sb.append("(").append(gap.get1()).append("..").append(gap.get2()).append(")");

            if (!iter.hasNext())
                sb.append(", ");
        }

        sb.append("]");

        return sb.toString();
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
                return printToFile(targetFileSupplier.apply(customFile, DiagnosticFileWriteMode.HUMAN_READABLE));

            case PRINT_TO_RAW_FILE:
                return printRawToFile(targetFileSupplier.apply(customFile, DiagnosticFileWriteMode.RAW), serializer());

            default:
                throw new IllegalArgumentException("Unknown diagnostic action : " + action);
        }
    }

    /**
     * @return WAL records serializer.
     * @throws IgniteException If serializer initialization failed for some reason.
     */
    private RecordSerializer serializer() {
        GridCacheSharedContext<?, ?> cctx = ctx.cache().context();

        int serializerVer = cctx.wal().serializerVersion();

        try {
            return new RecordSerializerFactoryImpl(cctx).createSerializer(serializerVer);
        }
        catch (IgniteCheckedException e) {
            log.error(
                "Failed to create WAL records serializer for diagnostic purposes [serializerVer=" + serializerVer + "]"
            );

            throw new IgniteException(e);
        }
    }

    /**
     * Parameters for diagnostic pages.
     */
    public static class DiagnosticPageBuilder {
        /** Pages for searching in WAL. */
        List<T2<Integer, Long>> pageIds = new ArrayList<>();

        /** Action after which should be executed after WAL scanning . */
        Set<DiagnosticProcessor.DiagnosticAction> actions = EnumSet.noneOf(DiagnosticProcessor.DiagnosticAction.class);

        /** Folder for dump diagnostic info. */
        File dumpFolder;

        /**
         * @param pageIds Pages for searching in WAL.
         * @return This instance for chaining.
         */
        public DiagnosticPageBuilder pageIds(T2<Integer, Long>... pageIds) {
            this.pageIds.addAll(Arrays.asList(pageIds));

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
