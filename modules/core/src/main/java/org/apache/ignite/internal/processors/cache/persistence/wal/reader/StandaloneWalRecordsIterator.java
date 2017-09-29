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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.LazyDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.UnwrapDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.AbstractWalRecordsIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferExpander;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentEofException;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.HEADER_RECORD_SIZE;

/**
 * WAL reader iterator, for creation in standalone WAL reader tool
 * Operates over one directory, does not provide start and end boundaries
 */
class StandaloneWalRecordsIterator extends AbstractWalRecordsIterator {
    /** */
    private static final long serialVersionUID = 0L;

    /** Record buffer size */
    private static final int BUF_SIZE = 2 * 1024 * 1024;

    /**
     * WAL files directory. Should already contain 'consistent ID' as subfolder.
     * <code>null</code> value means file-by-file iteration mode
     */
    @Nullable
    private File walFilesDir;

    /**
     * File descriptors remained to scan.
     * <code>null</code> value means directory scan mode
     */
    @Nullable
    private List<FileWriteAheadLogManager.FileDescriptor> walFileDescriptors;

    /**
     * True if this iterator used for work dir, false for archive.
     * In work dir mode exceptions come from record reading are ignored (file may be not completed).
     * Index of file is taken from file itself, not from file name
     */
    private boolean workDir;

    /** Keep binary. This flag disables converting of non primitive types (BinaryObjects) */
    private boolean keepBinary;

    /**
     * Creates iterator in directory scan mode
     *  @param walFilesDir Wal files directory. Should already contain node consistent ID as subfolder
     * @param log Logger.
     * @param sharedCtx Shared context. Cache processor is to be configured if Cache Object Key & Data Entry is
 * required.
     * @param ioFactory File I/O factory.
     * @param keepBinary  Keep binary. This flag disables converting of non primitive types
     * (BinaryObjects will be used instead)
     */
    StandaloneWalRecordsIterator(
        @NotNull File walFilesDir,
        @NotNull IgniteLogger log,
        @NotNull GridCacheSharedContext sharedCtx,
        @NotNull FileIOFactory ioFactory,
        boolean keepBinary
    ) throws IgniteCheckedException {
        super(log,
            sharedCtx,
            FileWriteAheadLogManager.forVersion(sharedCtx, FileWriteAheadLogManager.LATEST_SERIALIZER_VERSION),
            ioFactory,
            BUF_SIZE);
        this.keepBinary = keepBinary;
        init(walFilesDir, false, null);
        advance();
    }

    /**
     * Creates iterator in file-by-file iteration mode. Directory
     *  @param log Logger.
     * @param sharedCtx Shared context. Cache processor is to be configured if Cache Object Key & Data Entry is
     * required.
     * @param ioFactory File I/O factory.
     * @param workDir Work directory is scanned, false - archive
     * @param keepBinary Keep binary. This flag disables converting of non primitive types
     * (BinaryObjects will be used instead)
     * @param walFiles Wal files.
     */
    StandaloneWalRecordsIterator(
            @NotNull IgniteLogger log,
            @NotNull GridCacheSharedContext sharedCtx,
            @NotNull FileIOFactory ioFactory,
            boolean workDir,
            boolean keepBinary,
            @NotNull File... walFiles) throws IgniteCheckedException {
        super(log,
            sharedCtx,
            FileWriteAheadLogManager.forVersion(sharedCtx, FileWriteAheadLogManager.LATEST_SERIALIZER_VERSION),
            ioFactory,
            BUF_SIZE);

        this.workDir = workDir;
        this.keepBinary = keepBinary;
        init(null, workDir, walFiles);
        advance();
    }

    /**
     * For directory mode sets oldest file as initial segment,
     * for file by file mode, converts all files to descriptors and gets oldest as initial.
     *
     * @param walFilesDir directory for directory scan mode
     * @param workDir work directory, only for file-by-file mode
     * @param walFiles files for file-by-file iteration mode
     */
    private void init(
        @Nullable final File walFilesDir,
        final boolean workDir,
        @Nullable final File[] walFiles) throws IgniteCheckedException {
        if (walFilesDir != null) {
            FileWriteAheadLogManager.FileDescriptor[] descs = loadFileDescriptors(walFilesDir);
            curWalSegmIdx = !F.isEmpty(descs) ? descs[0].getIdx() : 0;
            this.walFilesDir = walFilesDir;
            this.workDir = false;
        }
        else {
            this.workDir = workDir;

            if (workDir)
                walFileDescriptors = scanIndexesFromFileHeaders(walFiles);
            else
                walFileDescriptors = new ArrayList<>(Arrays.asList(FileWriteAheadLogManager.scan(walFiles)));

            curWalSegmIdx = !walFileDescriptors.isEmpty() ? walFileDescriptors.get(0).getIdx() : 0;
        }
        curWalSegmIdx--;

        if (log.isDebugEnabled())
            log.debug("Initialized WAL cursor [curWalSegmIdx=" + curWalSegmIdx + ']');
    }

    /**
     * This methods checks all provided files to be correct WAL segment.
     * Header record and its position is checked. WAL position is used to determine real index.
     * File index from file name is ignored.
     *
     * @param allFiles files to scan
     * @return list of file descriptors with checked header records, file index is set
     * @throws IgniteCheckedException if IO error occurs
     */
    private List<FileWriteAheadLogManager.FileDescriptor> scanIndexesFromFileHeaders(
        @Nullable final File[] allFiles) throws IgniteCheckedException {
        if (allFiles == null || allFiles.length == 0)
            return Collections.emptyList();

        final List<FileWriteAheadLogManager.FileDescriptor> resultingDescs = new ArrayList<>();

        for (File file : allFiles) {
            if (file.length() < HEADER_RECORD_SIZE)
                continue;

            FileWALPointer ptr;

            try (
                FileIO fileIO = ioFactory.create(file);
                ByteBufferExpander buf = new ByteBufferExpander(HEADER_RECORD_SIZE, ByteOrder.nativeOrder())
            ) {
                final DataInput in = new FileInput(fileIO, buf);

                // Header record must be agnostic to the serializer version.
                final int type = in.readUnsignedByte();

                if (type == WALRecord.RecordType.STOP_ITERATION_RECORD_TYPE)
                    throw new SegmentEofException("Reached logical end of the segment", null);
                ptr = RecordV1Serializer.readPosition(in);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to scan index from file [" + file + "]", e);
            }

            resultingDescs.add(new FileWriteAheadLogManager.FileDescriptor(file, ptr.index()));
        }
        Collections.sort(resultingDescs);
        return resultingDescs;
    }

    /** {@inheritDoc} */
    @Override protected FileWriteAheadLogManager.ReadFileHandle advanceSegment(
        @Nullable final FileWriteAheadLogManager.ReadFileHandle curWalSegment) throws IgniteCheckedException {

        if (curWalSegment != null)
            curWalSegment.close();

        curWalSegmIdx++;
        // curHandle.workDir is false
        final FileWriteAheadLogManager.FileDescriptor fd;

        if (walFilesDir != null) {
            fd = new FileWriteAheadLogManager.FileDescriptor(
                new File(walFilesDir,
                    FileWriteAheadLogManager.FileDescriptor.fileName(curWalSegmIdx)));
        }
        else {
            if (walFileDescriptors.isEmpty())
                return null; //no files to read, stop iteration

            fd = walFileDescriptors.remove(0);
        }

        if (log.isDebugEnabled())
            log.debug("Reading next file [absIdx=" + curWalSegmIdx + ", file=" + fd.getAbsolutePath() + ']');

        assert fd != null;

        curRec = null;
        try {
            return initReadHandle(fd, null);
        }
        catch (FileNotFoundException e) {
            if (log.isInfoEnabled())
                log.info("Missing WAL segment in the archive: " + e.getMessage());

            return null;
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override protected WALRecord postProcessRecord(@NotNull final WALRecord rec) {
        final GridKernalContext kernalCtx = sharedCtx.kernalContext();
        final IgniteCacheObjectProcessor processor = kernalCtx.cacheObjects();

        if (processor != null && rec.type() == WALRecord.RecordType.DATA_RECORD) {
            try {
                return postProcessDataRecord((DataRecord)rec, kernalCtx, processor);
            }
            catch (Exception e) {
                log.error("Failed to perform post processing for data record ", e);
            }
        }
        return super.postProcessRecord(rec);
    }

    /**
     * Performs post processing of lazy data record, converts it to unwrap record.
     *
     * @param dataRec data record to post process records.
     * @param kernalCtx kernal context.
     * @param processor processor to convert binary form from WAL into CacheObject/BinaryObject.
     * @return post-processed record.
     * @throws IgniteCheckedException if failed.
     */
    @NotNull private WALRecord postProcessDataRecord(
        @NotNull final DataRecord dataRec,
        final GridKernalContext kernalCtx,
        final IgniteCacheObjectProcessor processor) throws IgniteCheckedException {
        final CacheObjectContext fakeCacheObjCtx = new CacheObjectContext(kernalCtx,
            null, null, false, false, false);

        final List<DataEntry> entries = dataRec.writeEntries();
        final List<DataEntry> postProcessedEntries = new ArrayList<>(entries.size());

        for (DataEntry dataEntry : entries) {
            final DataEntry postProcessedEntry = postProcessDataEntry(processor, fakeCacheObjCtx, dataEntry);

            postProcessedEntries.add(postProcessedEntry);
        }
        return new DataRecord(postProcessedEntries);
    }

    /**
     * Converts entry or lazy data entry into unwrapped entry
     * @param processor cache object processor for de-serializing objects.
     * @param fakeCacheObjCtx cache object context for de-serializing binary and unwrapping objects.
     * @param dataEntry entry to process
     * @return post precessed entry
     * @throws IgniteCheckedException if failed
     */
    @NotNull
    private DataEntry postProcessDataEntry(
        final IgniteCacheObjectProcessor processor,
        final CacheObjectContext fakeCacheObjCtx,
        final DataEntry dataEntry) throws IgniteCheckedException {

        final KeyCacheObject key;
        final CacheObject val;
        final File marshallerMappingFileStoreDir =
            fakeCacheObjCtx.kernalContext().marshallerContext().getMarshallerMappingFileStoreDir();

        if (dataEntry instanceof LazyDataEntry) {
            final LazyDataEntry lazyDataEntry = (LazyDataEntry)dataEntry;
            key = processor.toKeyCacheObject(fakeCacheObjCtx,
                lazyDataEntry.getKeyType(),
                lazyDataEntry.getKeyBytes());
            val = processor.toCacheObject(fakeCacheObjCtx,
                lazyDataEntry.getValType(),
                lazyDataEntry.getValBytes());
        }
        else {
            key = dataEntry.key();
            val = dataEntry.value();
        }

        return new UnwrapDataEntry(
            dataEntry.cacheId(),
            key,
            val,
            dataEntry.op(),
            dataEntry.nearXidVersion(),
            dataEntry.writeVersion(),
            dataEntry.expireTime(),
            dataEntry.partitionId(),
            dataEntry.partitionCounter(),
            fakeCacheObjCtx,
            keepBinary || marshallerMappingFileStoreDir == null);
    }

    /** {@inheritDoc} */
    @Override protected void handleRecordException(
        @NotNull final Exception e,
        @Nullable final FileWALPointer ptr) {
        super.handleRecordException(e, ptr);
        final RuntimeException ex = new RuntimeException("Record reading problem occurred at file pointer [" + ptr + "]:" + e.getMessage(), e);

        ex.printStackTrace();
        if (!workDir)
            throw ex;
    }

    /** {@inheritDoc} */
    @Override protected void onClose() throws IgniteCheckedException {
        super.onClose();

        curRec = null;

        closeCurrentWalSegment();

        curWalSegmIdx = Integer.MAX_VALUE;
    }
}
