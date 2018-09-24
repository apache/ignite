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

package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteOrder;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.UnzipFileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferExpander;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.lang.System.arraycopy;
import static java.nio.file.Files.walkFileTree;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_NAME_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_FILE_COMPACTED_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.HEADER_RECORD_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.readPosition;

/**
 * Factory for creating iterator over WAL files
 */
public class IgniteWalIteratorFactory {
    /** Logger. */
    private final IgniteLogger log;

    /**
     * Creates WAL files iterator factory.
     * WAL iterator supports automatic converting from CacheObjects and KeyCacheObject into BinaryObjects
     */
    public IgniteWalIteratorFactory() {
        this(ConsoleLogger.INSTANCE);
    }

    /**
     * Creates WAL files iterator factory.
     * WAL iterator supports automatic converting from CacheObjects and KeyCacheObject into BinaryObjects
     *
     * @param log Logger.
     */
    public IgniteWalIteratorFactory(@NotNull final IgniteLogger log) {
        this.log = log;
    }

    /**
     * Creates iterator for file by file scan mode.
     * This method may be used for work folder, file indexes are scanned from the file context.
     * In this mode only provided WAL segments will be scanned. New WAL files created during iteration will be ignored.
     *
     * @param filesOrDirs files to scan. A file can be the path to '.wal' file, or directory with '.wal' files.
     * Order is not important, but it is significant to provide all segments without omissions.
     * Path should not contain special symbols. Special symbols should be already masked.
     * @return closable WAL records iterator, should be closed when non needed.
     * @throws IgniteCheckedException if failed to read files
     * @throws IllegalArgumentException If parameter validation failed.
     */
    public WALIterator iterator(
        @NotNull File... filesOrDirs
    ) throws IgniteCheckedException, IllegalArgumentException {
        return iterator(new IteratorParametersBuilder().filesOrDirs(filesOrDirs));
    }

    /**
     * Creates iterator for file by file scan mode.
     * This method may be used for work folder, file indexes are scanned from the file context.
     * In this mode only provided WAL segments will be scanned. New WAL files created during iteration will be ignored.
     *
     * @param replayFrom File WAL pointer for start replay.
     * @param filesOrDirs files to scan. A file can be the path to '.wal' file, or directory with '.wal' files.
     * Order is not important, but it is significant to provide all segments without omissions.
     * Path should not contain special symbols. Special symbols should be already masked.
     * @return closable WAL records iterator, should be closed when non needed.
     * @throws IgniteCheckedException if failed to read files
     * @throws IllegalArgumentException If parameter validation failed.
     */
    public WALIterator iterator(
        @NotNull FileWALPointer replayFrom,
        @NotNull File... filesOrDirs
    ) throws IgniteCheckedException, IllegalArgumentException {
        return iterator(new IteratorParametersBuilder().from(replayFrom).filesOrDirs(filesOrDirs));
    }

    /**
     * Creates iterator for file by file scan mode.
     * This method may be used for work folder, file indexes are scanned from the file context.
     * In this mode only provided WAL segments will be scanned. New WAL files created during iteration will be ignored.
     *
     * @param filesOrDirs paths to scan. A path can be direct to '.wal' file, or directory with '.wal' files.
     * Order is not important, but it is significant to provide all segments without omissions.
     * Path should not contain special symbols. Special symbols should be already masked.
     * @return closable WAL records iterator, should be closed when non needed.
     * @throws IgniteCheckedException If failed to read files.
     * @throws IllegalArgumentException If parameter validation failed.
     */
    public WALIterator iterator(
        @NotNull String... filesOrDirs
    ) throws IgniteCheckedException, IllegalArgumentException {
        return iterator(new IteratorParametersBuilder().filesOrDirs(filesOrDirs));
    }

    /**
     * Creates iterator for file by file scan mode.
     * This method may be used for work folder, file indexes are scanned from the file context.
     * In this mode only provided WAL segments will be scanned. New WAL files created during iteration will be ignored.
     *
     * @param replayFrom File WAL pointer for start replay.
     * @param filesOrDirs paths to scan. A path can be direct to '.wal' file, or directory with '.wal' files.
     * Order is not important, but it is significant to provide all segments without omissions.
     * Path should not contain special symbols. Special symbols should be already masked.
     * @return closable WAL records iterator, should be closed when non needed.
     * @throws IgniteCheckedException If failed to read files.
     * @throws IllegalArgumentException If parameter validation failed.
     */
    public WALIterator iterator(
        @NotNull FileWALPointer replayFrom,
        @NotNull String... filesOrDirs
    ) throws IgniteCheckedException, IllegalArgumentException {
        return iterator(new IteratorParametersBuilder().from(replayFrom).filesOrDirs(filesOrDirs));
    }

    /**
     * @param iteratorParametersBuilder Iterator parameters builder.
     * @return closable WAL records iterator, should be closed when non needed
     */
    public WALIterator iterator(
        @NotNull IteratorParametersBuilder iteratorParametersBuilder
    ) throws IgniteCheckedException, IllegalArgumentException {
        iteratorParametersBuilder.validate();

        return new StandaloneWalRecordsIterator(log,
            prepareSharedCtx(iteratorParametersBuilder),
            iteratorParametersBuilder.ioFactory,
            resolveWalFiles(iteratorParametersBuilder),
            iteratorParametersBuilder.filter,
            iteratorParametersBuilder.lowBound,
            iteratorParametersBuilder.highBound,
            iteratorParametersBuilder.keepBinary,
            iteratorParametersBuilder.bufferSize
        );
    }

    /**
     * Find WAL gaps, for example:
     * 0 1 2 3 4 7 8 10 - WAL segment files in directory, this method will return
     * List with two tuples [(4,7),(8,10)].
     *
     * @param filesOrDirs Paths to files or directories for scan.
     * @return List of tuples, low and high index segments with gap.
     */
    public List<T2<Long, Long>> hasGaps(
        @NotNull String... filesOrDirs
    ) throws IllegalArgumentException {
        return hasGaps(new IteratorParametersBuilder().filesOrDirs(filesOrDirs));
    }

    /**
     * Find WAL gaps, for example:
     * 0 1 2 3 4 7 8 10 - WAL segment files in directory, this method will return
     * List with two tuples [(4,7),(8,10)].
     *
     * @param filesOrDirs Files or directories to scan.
     * @return List of tuples, low and high index segments with gap.
     */
    public List<T2<Long, Long>> hasGaps(
        @NotNull File... filesOrDirs
    ) throws IllegalArgumentException {
        return hasGaps(new IteratorParametersBuilder().filesOrDirs(filesOrDirs));
    }

    /**
     * @param iteratorParametersBuilder Iterator parameters builder.
     * @return List of tuples, low and high index segments with gap.
     */
    public List<T2<Long, Long>> hasGaps(
        @NotNull IteratorParametersBuilder iteratorParametersBuilder
    ) throws IllegalArgumentException {
        iteratorParametersBuilder.validate();

        List<T2<Long, Long>> gaps = new ArrayList<>();

        List<FileDescriptor> descriptors = resolveWalFiles(iteratorParametersBuilder);

        Iterator<FileDescriptor> it = descriptors.iterator();

        FileDescriptor prevFd = null;

        while (it.hasNext()) {
            FileDescriptor nextFd = it.next();

            if (prevFd == null) {
                prevFd = nextFd;

                continue;
            }

            if (prevFd.idx() + 1 != nextFd.idx())
                gaps.add(new T2<>(prevFd.idx(), nextFd.idx()));

            prevFd = nextFd;
        }

        return gaps;
    }

    /**
     * This methods checks all provided files to be correct WAL segment.
     * Header record and its position is checked. WAL position is used to determine real index.
     * File index from file name is ignored.
     *
     * @param iteratorParametersBuilder IteratorParametersBuilder.
     * @return list of file descriptors with checked header records, having correct file index is set
     */
    public List<FileDescriptor> resolveWalFiles(
        IteratorParametersBuilder iteratorParametersBuilder
    ) {
        File[] filesOrDirs = iteratorParametersBuilder.filesOrDirs;

        if (filesOrDirs == null || filesOrDirs.length == 0)
            return Collections.emptyList();

        final FileIOFactory ioFactory = iteratorParametersBuilder.ioFactory;

        final TreeSet<FileDescriptor> descriptors = new TreeSet<>();

        for (File file : filesOrDirs) {
            if (file.isDirectory()) {
                try {
                    walkFileTree(file.toPath(), new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                            addFileDescriptor(path.toFile(), ioFactory, descriptors);

                            return FileVisitResult.CONTINUE;
                        }
                    });
                }
                catch (IOException e) {
                    U.error(log, "Failed to walk directories from root [" + file + "]. Skipping this directory.", e);
                }

                continue;
            }

            addFileDescriptor(file, ioFactory, descriptors);
        }

        return new ArrayList<>(descriptors);
    }

    /**
     * @param file File.
     * @param ioFactory IO factory.
     * @param descriptors List of descriptors.
     */
    private void addFileDescriptor(File file, FileIOFactory ioFactory, TreeSet<FileDescriptor> descriptors) {
        if (file.length() < HEADER_RECORD_SIZE)
            return; // Filter out this segment as it is too short.

        String fileName = file.getName();

        if (!WAL_NAME_PATTERN.matcher(fileName).matches() &&
            !WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(fileName).matches())
            return;  // Filter out this because it is not segment file.

        FileDescriptor desc = readFileDescriptor(file, ioFactory);

        if (desc != null)
            descriptors.add(desc);
    }

    /**
     * @param file File to read.
     * @param ioFactory IO factory.
     */
    private FileDescriptor readFileDescriptor(File file, FileIOFactory ioFactory) {
        FileDescriptor ds = new FileDescriptor(file);

        try (
            FileIO fileIO = ds.isCompressed() ? new UnzipFileIO(file) : ioFactory.create(file);
            ByteBufferExpander buf = new ByteBufferExpander(HEADER_RECORD_SIZE, ByteOrder.nativeOrder())
        ) {
            final DataInput in = new FileInput(fileIO, buf);

            // Header record must be agnostic to the serializer version.
            final int type = in.readUnsignedByte();

            if (type == RecordType.STOP_ITERATION_RECORD_TYPE) {
                if (log.isInfoEnabled())
                    log.info("Reached logical end of the segment for file " + file);

                return null;
            }

            FileWALPointer ptr = readPosition(in);

            return new FileDescriptor(file, ptr.index());
        }
        catch (IOException e) {
            U.warn(log, "Failed to scan index from file [" + file + "]. Skipping this file during iteration", e);

            return null;
        }
    }

    /**
     * @return Fake shared context required for create minimal services for record reading.
     */
    @NotNull private GridCacheSharedContext prepareSharedCtx(
        IteratorParametersBuilder iteratorParametersBuilder
    ) throws IgniteCheckedException {
        GridKernalContext kernalCtx = new StandaloneGridKernalContext(log,
            iteratorParametersBuilder.binaryMetadataFileStoreDir,
            iteratorParametersBuilder.marshallerMappingFileStoreDir
        );

        StandaloneIgniteCacheDatabaseSharedManager dbMgr = new StandaloneIgniteCacheDatabaseSharedManager();

        dbMgr.setPageSize(iteratorParametersBuilder.pageSize);

        return new GridCacheSharedContext<>(
            kernalCtx, null, null, null,
            null, null, null, dbMgr, null,
            null, null, null, null,
            null, null,null, null
        );
    }

    /**
     * Wal iterator parameter builder.
     */
    public static class IteratorParametersBuilder {
        /** */
        public static final FileWALPointer DFLT_LOW_BOUND = new FileWALPointer(Long.MIN_VALUE, 0, 0);

        /** */
        public static final FileWALPointer DFLT_HIGH_BOUND = new FileWALPointer(Long.MAX_VALUE, Integer.MAX_VALUE, 0);

        /** */
        private File[] filesOrDirs;

        /** */
        private int pageSize = DataStorageConfiguration.DFLT_PAGE_SIZE;

        /** Wal records iterator buffer size. */
        private int bufferSize = StandaloneWalRecordsIterator.DFLT_BUF_SIZE;

        /** Keep binary. This flag disables converting of non primitive types (BinaryObjects). */
        private boolean keepBinary;

        /** Factory to provide I/O interfaces for read/write operations with files. */
        private FileIOFactory ioFactory = new DataStorageConfiguration().getFileIOFactory();

        /**
         * Folder specifying location of metadata File Store. {@code null} means no specific folder is configured. <br>
         * This folder should be specified for converting data entries into BinaryObjects
         */
        @Nullable private File binaryMetadataFileStoreDir;

        /**
         * Folder specifying location of marshaller mapping file store. {@code null} means no specific folder is configured.
         * <br> This folder should be specified for converting data entries into BinaryObjects. Providing {@code null} will
         * disable unmarshall for non primitive objects, BinaryObjects will be provided
         */
        @Nullable private File marshallerMappingFileStoreDir;

        /** */
        @Nullable private IgniteBiPredicate<RecordType, WALPointer> filter;

        /** */
        private FileWALPointer lowBound = DFLT_LOW_BOUND;

        /** */
        private FileWALPointer highBound = DFLT_HIGH_BOUND;

        /**
         * @param filesOrDirs Paths to files or directories.
         * @return IteratorParametersBuilder Self reference.
         */
        public IteratorParametersBuilder filesOrDirs(String... filesOrDirs) {
            File[] filesOrDirs0 = new File[filesOrDirs.length];

            for (int i = 0; i < filesOrDirs.length; i++) {
                filesOrDirs0[i] = new File(filesOrDirs[i]);
            }

            return filesOrDirs(filesOrDirs0);
        }

        /**
         * @param filesOrDirs Files or directories.
         * @return IteratorParametersBuilder Self reference.
         */
        public IteratorParametersBuilder filesOrDirs(File... filesOrDirs) {
            if (this.filesOrDirs == null)
                this.filesOrDirs = filesOrDirs;
            else
                this.filesOrDirs = merge(this.filesOrDirs, filesOrDirs);

            return this;
        }

        /**
         * @param pageSize Page size.
         * @return IteratorParametersBuilder Self reference.
         */
        public IteratorParametersBuilder pageSize(int pageSize) {
            this.pageSize = pageSize;

            return this;
        }

        /**
         * @param bufferSize Initial size of buffer for reading segments.
         * @return IteratorParametersBuilder Self reference.
         */
        public IteratorParametersBuilder bufferSize(int bufferSize) {
            this.bufferSize = bufferSize;

            return this;
        }

        /**
         * @return IteratorParametersBuilder Self reference.
         */
        public IteratorParametersBuilder keepBinary(boolean keepBinary) {
            this.keepBinary = keepBinary;

            return this;
        }

        /**
         * @param ioFactory Custom IO factory for reading files.
         * @return IteratorParametersBuilder Self reference.
         */
        public IteratorParametersBuilder ioFactory(FileIOFactory ioFactory) {
            this.ioFactory = ioFactory;

            return this;
        }

        /**
         * @param binaryMetadataFileStoreDir Path to the binary metadata.
         * @return IteratorParametersBuilder Self reference.
         */
        public IteratorParametersBuilder binaryMetadataFileStoreDir(File binaryMetadataFileStoreDir) {
            this.binaryMetadataFileStoreDir = binaryMetadataFileStoreDir;

            return this;
        }

        /**
         * @param marshallerMappingFileStoreDir Path to the marshaller mapping.
         * @return IteratorParametersBuilder Self reference.
         */
        public IteratorParametersBuilder marshallerMappingFileStoreDir(File marshallerMappingFileStoreDir) {
            this.marshallerMappingFileStoreDir = marshallerMappingFileStoreDir;

            return this;
        }

        /**
         * @param filter Record filter for skip records during iteration.
         * @return IteratorParametersBuilder Self reference.
         */
        public IteratorParametersBuilder filter(IgniteBiPredicate<RecordType, WALPointer> filter) {
            this.filter = filter;

            return this;
        }

        /**
         * @param lowBound WAL pointer to start from.
         * @return IteratorParametersBuilder Self reference.
         */
        public IteratorParametersBuilder from(FileWALPointer lowBound) {
            this.lowBound = lowBound;

            return this;
        }

        /**
         * @param highBound WAL pointer to end of.
         * @return IteratorParametersBuilder Self reference.
         */
        public IteratorParametersBuilder to(FileWALPointer highBound) {
            this.highBound = highBound;

            return this;
        }

        /**
         * Copy current state of builder to new instance.
         *
         * @return IteratorParametersBuilder Self reference.
         */
        public IteratorParametersBuilder copy() {
            return new IteratorParametersBuilder()
                .filesOrDirs(filesOrDirs)
                .pageSize(pageSize)
                .bufferSize(bufferSize)
                .keepBinary(keepBinary)
                .ioFactory(ioFactory)
                .binaryMetadataFileStoreDir(binaryMetadataFileStoreDir)
                .marshallerMappingFileStoreDir(marshallerMappingFileStoreDir)
                .from(lowBound)
                .to(highBound)
                .filter(filter);
        }

        /**
         * @throws IllegalArgumentException If validation failed.
         */
        public void validate() throws IllegalArgumentException {
            A.ensure(pageSize >= 1024 && pageSize <= 16 * 1024, "Page size must be between 1kB and 16kB.");
            A.ensure(U.isPow2(pageSize), "Page size must be a power of 2.");

            A.ensure(bufferSize >= pageSize * 2, "Buffer to small.");
        }

        /**
         * Merge file arrays.
         *
         * @param f1 Files array one.
         * @param f2 Files array two.
         * @return Merged arrays from one and two arrays.
         */
        private File[] merge(File[] f1, File[] f2) {
            File[] merged = new File[f1.length + f2.length];

            arraycopy(f1, 0, merged, 0, f1.length);
            arraycopy(f2, 0, merged, f1.length, f2.length);

            return merged;
        }
    }

    /**
     *
     */
    private static class ConsoleLogger implements IgniteLogger {
        /** */
        private static final ConsoleLogger INSTANCE = new ConsoleLogger();

        /** */
        private static final PrintStream OUT = System.out;

        /** */
        private static final PrintStream ERR = System.err;

        /** */
        private ConsoleLogger() {

        }

        /** {@inheritDoc} */
        @Override public IgniteLogger getLogger(Object ctgr) {
            return this;
        }

        /** {@inheritDoc} */
        @Override public void trace(String msg) {

        }

        /** {@inheritDoc} */
        @Override public void debug(String msg) {

        }

        /** {@inheritDoc} */
        @Override public void info(String msg) {
            OUT.println(msg);
        }

        /** {@inheritDoc} */
        @Override public void warning(String msg, @Nullable Throwable e) {
            OUT.println(msg);

            if (e != null)
                e.printStackTrace(OUT);
        }

        /** {@inheritDoc} */
        @Override public void error(String msg, @Nullable Throwable e) {
            ERR.println(msg);

            if (e != null)
                e.printStackTrace(ERR);
        }

        /** {@inheritDoc} */
        @Override public boolean isTraceEnabled() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isDebugEnabled() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isInfoEnabled() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isQuiet() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public String fileName() {
            return "SYSTEM.OUT";
        }
    }
}
