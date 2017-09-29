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

import java.io.File;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for creating iterator over WAL files
 */
public class IgniteWalIteratorFactory {
    /** Logger. */
    private final IgniteLogger log;

    /** Page size, in standalone iterator mode this value can't be taken from memory configuration. */
    private final int pageSize;

    /**
     * Folder specifying location of metadata File Store. {@code null} means no specific folder is configured. <br>
     * This folder should be specified for converting data entries into BinaryObjects
     */
    @Nullable private File binaryMetadataFileStoreDir;

    /**
     * Folder specifying location of marshaller mapping file store. {@code null} means no specific folder is configured. <br>
     * This folder should be specified for converting data entries into BinaryObjects.
     * Providing {@code null} will disable unmarshall for non primitive objects, BinaryObjects will be provided
     */
    @Nullable private File marshallerMappingFileStoreDir;

    /** Keep binary. This flag disables converting of non primitive types (BinaryObjects) */
    private boolean keepBinary;

    /** Factory to provide I/O interfaces for read/write operations with files */
    private final FileIOFactory ioFactory;

    /**
     * Creates WAL files iterator factory.
     * WAL iterator supports automatic converting from CacheObjects and KeyCacheObject into BinaryObjects
     *
     * @param log Logger.
     * @param pageSize Page size which was used in Ignite Persistent Data store to read WAL from, size is validated
     * according its boundaries.
     * @param binaryMetadataFileStoreDir folder specifying location of metadata File Store. Should include "binary_meta"
     * subfolder and consistent ID subfolder. Note Consistent ID should be already masked and should not contain special
     * symbols. Providing {@code null} means no specific folder is configured. <br>
     * @param marshallerMappingFileStoreDir Folder specifying location of marshaller mapping file store. Should include
     * "marshaller" subfolder. Providing {@code null} will disable unmarshall for non primitive objects,
     * BinaryObjects will be provided
     * @param keepBinary {@code true} disables complex object unmarshall into source classes
     */
    public IgniteWalIteratorFactory(
        @NotNull final IgniteLogger log,
        final int pageSize,
        @Nullable final File binaryMetadataFileStoreDir,
        @Nullable final File marshallerMappingFileStoreDir,
        final boolean keepBinary) {
        this.log = log;
        this.pageSize = pageSize;
        this.binaryMetadataFileStoreDir = binaryMetadataFileStoreDir;
        this.marshallerMappingFileStoreDir = marshallerMappingFileStoreDir;
        this.keepBinary = keepBinary;
        this.ioFactory = new PersistentStoreConfiguration().getFileIOFactory();
        new MemoryConfiguration().setPageSize(pageSize); // just for validate
    }

    /**
     * Creates WAL files iterator factory.
     * WAL iterator supports automatic converting from CacheObjects and KeyCacheObject into BinaryObjects
     *
     * @param log Logger.
     * @param pageSize Page size which was used in Ignite Persistent Data store to read WAL from, size is validated
     * according its boundaries.
     * @param binaryMetadataFileStoreDir folder specifying location of metadata File Store. Should include "binary_meta"
     * subfolder and consistent ID subfolder. Note Consistent ID should be already masked and should not contain special
     * symbols. Providing {@code null} means no specific folder is configured. <br>
     * @param marshallerMappingFileStoreDir Folder specifying location of marshaller mapping file store. Should include
     * "marshaller" subfolder. Providing {@code null} will disable unmarshall for non primitive objects, BinaryObjects
     * will be provided
     */
    public IgniteWalIteratorFactory(
        @NotNull final IgniteLogger log,
        final int pageSize,
        @Nullable final File binaryMetadataFileStoreDir,
        @Nullable final File marshallerMappingFileStoreDir) {
        this(log, pageSize, binaryMetadataFileStoreDir, marshallerMappingFileStoreDir, false);
    }

    /**
     * Creates WAL files iterator factory. This constructor does not allow WAL iterators access to data entries key and value.
     *
     * @param log Logger.
     * @param ioFactory Custom factory for non-standard file API to be used in WAL reading.
     * @param pageSize Page size which was used in Ignite Persistent Data store to read WAL from, size is validated
     * according its boundaries.
     */
    public IgniteWalIteratorFactory(@NotNull final IgniteLogger log, @NotNull final FileIOFactory ioFactory, int pageSize) {
        this.log = log;
        this.pageSize = pageSize;
        this.ioFactory = ioFactory;
        new MemoryConfiguration().setPageSize(pageSize); // just for validate
    }

    /**
     * Creates WAL files iterator factory. This constructor does not allow WAL iterators access to data entries key and
     * value.
     *
     * @param log Logger.
     * @param pageSize Page size which was used in Ignite Persistent Data store to read WAL from, size is validated
     * according its boundaries.
     */
    public IgniteWalIteratorFactory(@NotNull final IgniteLogger log, int pageSize) {
        this(log, new PersistentStoreConfiguration().getFileIOFactory(), pageSize);
    }

    /**
     * Creates iterator for (archive) directory scan mode.
     * Note in this mode total scanned files at end of iteration may be wider that initial files in directory.
     * This mode does not support work directory scan because work directory contains unpredictable number in file name.
     * Such file may broke iteration.
     *
     * @param walDirWithConsistentId directory with WAL files. Should already contain node consistent ID as subfolder.
     * Note: 'Consistent ID'-based subfolder name (if any) should not contain special symbols.
     * @return closable WAL records iterator, should be closed when non needed
     * @throws IgniteCheckedException if failed to read folder
     */
    public WALIterator iteratorArchiveDirectory(@NotNull final File walDirWithConsistentId) throws IgniteCheckedException {
        return new StandaloneWalRecordsIterator(walDirWithConsistentId, log, prepareSharedCtx(), ioFactory, keepBinary);
    }

    /**
     * Creates iterator for file by file scan mode.
     * This method may be used only for archive folder (not for work).
     * In this mode only provided WAL segments will be scanned. New WAL files created during iteration will be ignored
     *
     * @param files files to scan. Order is not important, but it is significant to provide all segments without omissions.
     * Parameter should contain direct file links to '.wal' files from archive directory.
     * 'Consistent ID'-based subfolder name (if any) should not contain special symbols.
     * Special symbols should be already masked.
     *
     * @return closable WAL records iterator, should be closed when non needed
     * @throws IgniteCheckedException if failed to read files
     */
    public WALIterator iteratorArchiveFiles(@NotNull final File... files) throws IgniteCheckedException {
        return new StandaloneWalRecordsIterator(log, prepareSharedCtx(), ioFactory, false, keepBinary, files);
    }

    /**
     * Creates iterator for file by file scan mode.
     * This method may be used for work folder, file indexes are scanned from the file context.
     * In this mode only provided WAL segments will be scanned. New WAL files created during iteration will be ignored.
     *
     * @param files files to scan. Order is not important, but it is significant to provide all segments without omissions.
     * Parameter should contain direct file links to '.wal' files from work directory.
     * 'Consistent ID'-based subfolder name (if any) should not contain special symbols.
     * Special symbols should be already masked.
     *
     * @return closable WAL records iterator, should be closed when non needed
     * @throws IgniteCheckedException if failed to read files
     */
    public WALIterator iteratorWorkFiles(@NotNull final File... files) throws IgniteCheckedException {
        return new StandaloneWalRecordsIterator(log, prepareSharedCtx(), ioFactory, true, keepBinary, files);
    }

    /**
     * @return fake shared context required for create minimal services for record reading
     */
    @NotNull private GridCacheSharedContext prepareSharedCtx() throws IgniteCheckedException {
        final GridKernalContext kernalCtx = new StandaloneGridKernalContext(log, binaryMetadataFileStoreDir, marshallerMappingFileStoreDir);

        final StandaloneIgniteCacheDatabaseSharedManager dbMgr = new StandaloneIgniteCacheDatabaseSharedManager();

        dbMgr.setPageSize(pageSize);

        return new GridCacheSharedContext<>(
            kernalCtx, null, null, null,
            null, null, dbMgr, null,
            null, null, null, null,
            null, null, null);
    }
}
