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
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.LazyDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.UnwrapDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.AbstractWalRecordsIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.ReadFileHandle;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * WAL reader iterator, for creation in standalone WAL reader tool
 * Operates over one directory, does not provide start and end boundaries
 */
class StandaloneWalRecordsIterator extends AbstractWalRecordsIterator {
    /** Record buffer size */
    public static final int DFLT_BUF_SIZE = 2 * 1024 * 1024;

    /** */
    private static final long serialVersionUID = 0L;
    /**
     * File descriptors remained to scan.
     * <code>null</code> value means directory scan mode
     */
    @Nullable
    private List<FileDescriptor> walFileDescriptors;

    private int curIdx = 0;

    /** Keep binary. This flag disables converting of non primitive types (BinaryObjects) */
    private boolean keepBinary;

    /**
     * Creates iterator in file-by-file iteration mode. Directory
     * @param log Logger.
     * @param sharedCtx Shared context. Cache processor is to be configured if Cache Object Key & Data Entry is
     * required.
     * @param ioFactory File I/O factory.
     * @param keepBinary Keep binary. This flag disables converting of non primitive types
     * (BinaryObjects will be used instead)
     * @param walFiles Wal files.
     */
    StandaloneWalRecordsIterator(
        @NotNull IgniteLogger log,
        @NotNull GridCacheSharedContext sharedCtx,
        @NotNull FileIOFactory ioFactory,
        @NotNull List<FileDescriptor> walFiles,
        IgniteBiPredicate<RecordType, WALPointer> readTypeFilter,
        boolean keepBinary,
        int bufSize
    ) throws IgniteCheckedException {
        super(
            log,
            sharedCtx,
            new RecordSerializerFactoryImpl(sharedCtx, readTypeFilter),
            ioFactory,
            bufSize
        );

        this.keepBinary = keepBinary;

        init(walFiles);

        advance();
    }

    /**
     * For directory mode sets oldest file as initial segment,
     * for file by file mode, converts all files to descriptors and gets oldest as initial.
     *
     * @param walFiles files for file-by-file iteration mode
     */
    private void init(List<FileDescriptor> walFiles) throws IgniteCheckedException {
        curWalSegmIdx = walFiles.get(curIdx).idx();

        walFileDescriptors = walFiles;

        if (log.isDebugEnabled())
            log.debug("Initialized WAL cursor [curWalSegmIdx=" + curWalSegmIdx + ']');
    }

    /** {@inheritDoc} */
    @Override protected AbstractReadFileHandle advanceSegment(
        @Nullable final AbstractReadFileHandle curWalSegment
    ) throws IgniteCheckedException {

        if (curWalSegment != null)
            curWalSegment.close();

        curWalSegmIdx++;

        curIdx++;

        // curHandle.workDir is false
        final FileDescriptor fd = walFileDescriptors.get(curIdx);
/*
        if (walFilesDir != null) {
            File segmentFile = new File(walFilesDir,
                FileDescriptor.fileName(curWalSegmIdx));

            if (!segmentFile.exists())
                segmentFile = new File(walFilesDir,
                    FileDescriptor.fileName(curWalSegmIdx) + ".zip");

            fd = new FileDescriptor(segmentFile);
        }
        else {
            if (walFileDescriptors.isEmpty())
                return null; //no files to read, stop iteration

            fd = walFileDescriptors.remove(0);
        }*/

        if (log.isDebugEnabled())
            log.debug("Reading next file [absIdx=" + curWalSegmIdx + ", file=" + fd.file().getAbsolutePath() + ']');

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
         GridKernalContext kernalCtx = sharedCtx.kernalContext();
         IgniteCacheObjectProcessor processor = kernalCtx.cacheObjects();

        if (processor != null && rec.type() == RecordType.DATA_RECORD) {
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
        @NotNull DataRecord dataRec,
        GridKernalContext kernalCtx,
        IgniteCacheObjectProcessor processor
    ) throws IgniteCheckedException {
        final CacheObjectContext fakeCacheObjCtx = new CacheObjectContext(
            kernalCtx, null, null, false, false, false);

        final List<DataEntry> entries = dataRec.writeEntries();
        final List<DataEntry> postProcessedEntries = new ArrayList<>(entries.size());

        for (DataEntry dataEntry : entries) {
            final DataEntry postProcessedEntry = postProcessDataEntry(processor, fakeCacheObjCtx, dataEntry);

            postProcessedEntries.add(postProcessedEntry);
        }

        DataRecord res = new DataRecord(postProcessedEntries, dataRec.timestamp());

        res.size(dataRec.size());
        res.position(dataRec.position());

        return res;
    }

    /**
     * Converts entry or lazy data entry into unwrapped entry
     * @param processor cache object processor for de-serializing objects.
     * @param fakeCacheObjCtx cache object context for de-serializing binary and unwrapping objects.
     * @param dataEntry entry to process
     * @return post precessed entry
     * @throws IgniteCheckedException if failed
     */
    @NotNull private DataEntry postProcessDataEntry(
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

            final byte type = lazyDataEntry.getValType();

            val = type == 0 ? null :
                processor.toCacheObject(fakeCacheObjCtx,
                    type,
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
    @Override protected void onClose() throws IgniteCheckedException {
        super.onClose();

        curRec = null;

        closeCurrentWalSegment();

        curWalSegmIdx = Integer.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override protected AbstractReadFileHandle createReadFileHandle(
        FileIO fileIO, long idx, RecordSerializer ser, FileInput in
    ) {
        return new ReadFileHandle(fileIO, idx, ser, in);
    }
}
