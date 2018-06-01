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
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.UnzipFileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferExpander;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.FileDescriptor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
     * @param filesOrDirs files to scan. Order is not important, but it is significant to provide all segments without omissions.
     * Parameter should contain direct file links to '.wal' files from work directory.
     * 'Consistent ID'-based subfolder name (if any) should not contain special symbols.
     * Special symbols should be already masked.
     * @return closable WAL records iterator, should be closed when non needed
     * @throws IgniteCheckedException if failed to read files
     */
    public WALIterator iterator(@NotNull File... filesOrDirs) throws IgniteCheckedException {
        return iterator(new IteratorParametersBuilder().filesOrDirs(filesOrDirs));
    }

    public WALIterator iterator(
        @NotNull IteratorParametersBuilder iteratorParametersBuilder
    ) throws IgniteCheckedException {
        iteratorParametersBuilder.validate();

        return new StandaloneWalRecordsIterator(log,
            prepareSharedCtx(iteratorParametersBuilder),
            iteratorParametersBuilder.ioFactory,
            resolveWalFiles(
                iteratorParametersBuilder.filesOrDirs,
                iteratorParametersBuilder
            ),
            iteratorParametersBuilder.filter,
            iteratorParametersBuilder.keepBinary,
            iteratorParametersBuilder.bufferSize
        );
    }

    public List<T2<Long, Long>> hasGaps(@NotNull File... filesOrDirs) throws IgniteCheckedException {
        return hasGaps(new IteratorParametersBuilder().filesOrDirs(filesOrDirs));
    }

    public List<T2<Long, Long>> hasGaps(
        @NotNull IteratorParametersBuilder iteratorParametersBuilder
    ) throws IgniteCheckedException {
        iteratorParametersBuilder.validate();

        List<T2<Long, Long>> gaps = new ArrayList<>();

        List<FileDescriptor> descriptors = resolveWalFiles(
            iteratorParametersBuilder.filesOrDirs,
            iteratorParametersBuilder
        );

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
    private List<FileDescriptor> resolveWalFiles(File[] filesOrDirs, IteratorParametersBuilder iteratorParametersBuilder) {
        if (filesOrDirs == null || filesOrDirs.length == 0)
            return Collections.emptyList();

        final List<FileDescriptor> descriptors = new ArrayList<>();

        for (File file : filesOrDirs) {
            if (file.isDirectory()) {
                descriptors.addAll(resolveWalFiles(file.listFiles(), iteratorParametersBuilder));

                continue;
            }

            if (file.length() < HEADER_RECORD_SIZE)
                continue;  // Filter out this segment as it is too short.

            FileIOFactory ioFactory = iteratorParametersBuilder.ioFactory;

            FileDescriptor ds = new FileDescriptor(file);

            FileWALPointer ptr;

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

                    continue; // Filter out this segment
                }

                ptr = readPosition(in);
            }
            catch (IOException e) {
                U.warn(log, "Failed to scan index from file [" + file + "]. Skipping this file during iteration", e);

                continue; // Filter out this segment.
            }

            descriptors.add(new FileDescriptor(file, ptr.index()));
        }

        Collections.sort(descriptors);

        return descriptors;
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
            null, null, null
        );
    }

    public static class IteratorParametersBuilder {
        /** */
        private File[] filesOrDirs;

        /** */
        private int pageSize;

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

        /**
         *
         */
        public IteratorParametersBuilder filesOrDirs(File... filesOrDirs) {
            this.filesOrDirs = filesOrDirs;

            return this;
        }

        /**
         *
         */
        public IteratorParametersBuilder pageSize(int pageSize) {
            this.pageSize = pageSize;

            return this;
        }

        /**
         *
         */
        public IteratorParametersBuilder bufferSize(int bufferSize) {
            this.bufferSize = bufferSize;

            return this;
        }

        /**
         *
         */
        public IteratorParametersBuilder keepBinary(boolean keepBinary) {
            this.keepBinary = keepBinary;

            return this;
        }

        /**
         *
         */
        public IteratorParametersBuilder ioFactory(FileIOFactory ioFactory) {
            this.ioFactory = ioFactory;

            return this;
        }

        /**
         *
         */
        public IteratorParametersBuilder binaryMetadataFileStoreDir(File binaryMetadataFileStoreDir) {
            this.binaryMetadataFileStoreDir = binaryMetadataFileStoreDir;

            return this;
        }

        /**
         *
         */
        public IteratorParametersBuilder marshallerMappingFileStoreDir(File marshallerMappingFileStoreDir) {
            this.marshallerMappingFileStoreDir = marshallerMappingFileStoreDir;

            return this;
        }

        /**
         *
         */
        public IteratorParametersBuilder filter(IgniteBiPredicate<RecordType, WALPointer> filter) {
            this.filter = filter;

            return this;
        }

        /**
         *
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
                .filter(filter);
        }

        /**
         *
         */
        public void validate() throws IgniteCheckedException {

        }
    }
}
