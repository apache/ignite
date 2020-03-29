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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.FilteredRecord;
import org.apache.ignite.internal.pagemem.wal.record.MarshalledDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.MvccDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.MvccDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.UnwrapDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.UnwrapMvccDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.AbstractWalRecordsIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.ReadFileHandle;
import org.apache.ignite.internal.processors.cache.persistence.wal.WalSegmentTailReachedException;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentFileInputFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SimpleSegmentFileInputFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordDataV1Serializer.EncryptedDataEntry;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.SegmentHeader;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder.DFLT_HIGH_BOUND;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.readSegmentHeader;

/**
 * WAL reader iterator, for creation in standalone WAL reader tool Operates over one directory, does not provide start
 * and end boundaries
 */
class StandaloneWalRecordsIterator extends AbstractWalRecordsIterator {
    /** Record buffer size */
    public static final int DFLT_BUF_SIZE = 2 * 1024 * 1024;

    /** */
    private static final long serialVersionUID = 0L;

    /** Factory to provide I/O interfaces for read primitives with files. */
    private static final SegmentFileInputFactory FILE_INPUT_FACTORY = new SimpleSegmentFileInputFactory();

    /**
     * File descriptors remained to scan.
     * <code>null</code> value means directory scan mode
     */
    @Nullable
    private final List<FileDescriptor> walFileDescriptors;

    /** */
    private int curIdx = -1;

    /** Keep binary. This flag disables converting of non primitive types (BinaryObjects) */
    private boolean keepBinary;

    /** Replay from bound include. */
    private final FileWALPointer lowBound;

    /** Replay to bound include */
    private final FileWALPointer highBound;

    /**
     * Creates iterator in file-by-file iteration mode. Directory
     *
     * @param log Logger.
     * @param sharedCtx Shared context. Cache processor is to be configured if Cache Object Key & Data Entry is
     * required.
     * @param ioFactory File I/O factory.
     * @param keepBinary Keep binary. This flag disables converting of non primitive types (BinaryObjects will be used
     * instead)
     * @param walFiles Wal files.
     */
    StandaloneWalRecordsIterator(
        @NotNull IgniteLogger log,
        @NotNull GridCacheSharedContext sharedCtx,
        @NotNull FileIOFactory ioFactory,
        @NotNull List<FileDescriptor> walFiles,
        IgniteBiPredicate<RecordType, WALPointer> readTypeFilter,
        FileWALPointer lowBound,
        FileWALPointer highBound,
        boolean keepBinary,
        int initialReadBufferSize,
        boolean strictBoundsCheck
    ) throws IgniteCheckedException {
        super(
            log,
            sharedCtx,
            new RecordSerializerFactoryImpl(sharedCtx, readTypeFilter),
            ioFactory,
            initialReadBufferSize,
            FILE_INPUT_FACTORY
        );

        if (strictBoundsCheck)
            strictCheck(walFiles, lowBound, highBound);

        this.lowBound = lowBound;
        this.highBound = highBound;

        this.keepBinary = keepBinary;

        walFileDescriptors = walFiles;

        init(walFiles);

        advance();
    }

    /**
     * @param walFiles Wal files.
     * @return printable indexes of segment files.
     */
    private static String printIndexes(List<FileDescriptor> walFiles) {
        return "[" + String.join(",", walFiles.stream().map(f -> Long.toString(f.idx())).collect(Collectors.toList())) + "]";
    }

    /**
     * @param walFiles Wal files.
     * @param lowBound Low bound.
     * @param highBound High bound.
     *
     * @throws IgniteCheckedException if failed
     */
    private static void strictCheck(List<FileDescriptor> walFiles, FileWALPointer lowBound, FileWALPointer highBound) throws IgniteCheckedException {
        int idx = 0;

        if (lowBound.index() > Long.MIN_VALUE) {
            for (; idx < walFiles.size(); idx++) {
                FileDescriptor desc = walFiles.get(idx);

                assert desc != null;

                if (desc.idx() == lowBound.index())
                    break;
            }
        }

        if (idx == walFiles.size())
            throw new StrictBoundsCheckException("Wal segments not in bounds. loBoundIndex=" + lowBound.index() +
                                                ", indexes=" + printIndexes(walFiles));

        long curWalSegmIdx = walFiles.get(idx).idx();

        for (; idx < walFiles.size() && curWalSegmIdx <= highBound.index(); idx++, curWalSegmIdx++) {
            FileDescriptor desc = walFiles.get(idx);

            assert desc != null;

            if (curWalSegmIdx != desc.idx())
                throw new StrictBoundsCheckException("Wal segment " + curWalSegmIdx + " not found in files " + printIndexes(walFiles));
        }

        if (highBound.index() < Long.MAX_VALUE && curWalSegmIdx <= highBound.index())
            throw new StrictBoundsCheckException("Wal segments not in bounds. hiBoundIndex=" + highBound.index() +
                                                ", indexes=" + printIndexes(walFiles));
    }

    /**
     * For directory mode sets oldest file as initial segment, for file by file mode, converts all files to descriptors
     * and gets oldest as initial.
     *
     * @param walFiles files for file-by-file iteration mode
     */
    private void init(List<FileDescriptor> walFiles) {
        if (walFiles == null || walFiles.isEmpty())
            return;

        curWalSegmIdx = walFiles.get(curIdx + 1).idx() - 1;

        if (log.isDebugEnabled())
            log.debug("Initialized WAL cursor [curWalSegmIdx=" + curWalSegmIdx + ']');
    }

    /** {@inheritDoc} */
    @Override protected IgniteCheckedException validateTailReachedException(
        WalSegmentTailReachedException tailReachedException,
        AbstractReadFileHandle currWalSegment
    ) {
        FileDescriptor lastWALSegmentDesc = walFileDescriptors.get(walFileDescriptors.size() - 1);

        // Iterator can not be empty.
        assert lastWALSegmentDesc != null;

        return lastWALSegmentDesc.idx() != currWalSegment.idx() ?
            new IgniteCheckedException(
                "WAL tail reached not in the last available segment, " +
                    "potentially corrupted segment, last available segment idx=" + lastWALSegmentDesc.idx() +
                    ", path=" + lastWALSegmentDesc.file().getPath() +
                    ", last read segment idx=" + currWalSegment.idx(), tailReachedException
            ) : null;
    }

    /** {@inheritDoc} */
    @Override protected AbstractReadFileHandle advanceSegment(
        @Nullable final AbstractReadFileHandle curWalSegment
    ) throws IgniteCheckedException {

        if (curWalSegment != null)
            curWalSegment.close();

        FileDescriptor fd;

        do {
            curWalSegmIdx++;

            curIdx++;

            if (curIdx >= walFileDescriptors.size())
                return null;

            fd = walFileDescriptors.get(curIdx);
        }
        while (!checkBounds(fd.idx()));

        if (log.isDebugEnabled())
            log.debug("Reading next file [absIdx=" + curWalSegmIdx + ", file=" + fd.file().getAbsolutePath() + ']');

        assert fd != null;

        curRec = null;

        try {
            FileWALPointer initPtr = null;

            if (lowBound.index() == fd.idx())
                initPtr = lowBound;

            return initReadHandle(fd, initPtr);
        }
        catch (FileNotFoundException e) {
            if (log.isInfoEnabled())
                log.info("Missing WAL segment in the archive: " + e.getMessage());

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteBiTuple<WALPointer, WALRecord> advanceRecord(
        @Nullable AbstractReadFileHandle hnd
    ) throws IgniteCheckedException {
        IgniteBiTuple<WALPointer, WALRecord> tup = super.advanceRecord(hnd);

        if (tup == null)
            return tup;

        if (!checkBounds(tup.get1())) {
            if (curRec != null) {
                FileWALPointer prevRecPtr = (FileWALPointer)curRec.get1();

                // Fast stop condition, after high bound reached.
                if (prevRecPtr != null && prevRecPtr.compareTo(highBound) > 0)
                    return null;
            }

            return new T2<>(tup.get1(), FilteredRecord.INSTANCE); // FilteredRecord for mark as filtered.
        }

        return tup;
    }

    /**
     * @param ptr WAL pointer.
     * @return {@code True} If pointer between low and high bounds. {@code False} if not.
     */
    private boolean checkBounds(WALPointer ptr) {
        FileWALPointer ptr0 = (FileWALPointer)ptr;

        return ptr0.compareTo(lowBound) >= 0 && ptr0.compareTo(highBound) <= 0;
    }

    /**
     * @param idx WAL segment index.
     * @return {@code True} If pointer between low and high bounds. {@code False} if not.
     */
    private boolean checkBounds(long idx) {
        return idx >= lowBound.index() && idx <= highBound.index();
    }

    /** {@inheritDoc} */
    @Override protected AbstractReadFileHandle initReadHandle(
        @NotNull AbstractFileDescriptor desc,
        @Nullable FileWALPointer start
    ) throws IgniteCheckedException, FileNotFoundException {

        AbstractFileDescriptor fd = desc;
        SegmentIO fileIO = null;
        SegmentHeader segmentHeader;
        while (true) {
            try {
                fileIO = fd.toIO(ioFactory);

                segmentHeader = readSegmentHeader(fileIO, FILE_INPUT_FACTORY);

                break;
            }
            catch (IOException | IgniteCheckedException e) {
                log.error("Failed to init segment curWalSegmIdx=" + curWalSegmIdx + ", curIdx=" + curIdx, e);

                U.closeQuiet(fileIO);

                curIdx++;

                if (curIdx >= walFileDescriptors.size())
                    return null;

                fd = walFileDescriptors.get(curIdx);
            }
        }

        return initReadHandle(fd, start, fileIO, segmentHeader);
    }

    /** {@inheritDoc} */
    @Override protected @NotNull WALRecord postProcessRecord(@NotNull final WALRecord rec) {
        GridKernalContext kernalCtx = sharedCtx.kernalContext();
        IgniteCacheObjectProcessor processor = kernalCtx.cacheObjects();

        if (processor != null && (rec.type() == RecordType.DATA_RECORD || rec.type() == RecordType.MVCC_DATA_RECORD)) {
            try {
                return postProcessDataRecord((DataRecord)rec, kernalCtx, processor);
            }
            catch (Exception e) {
                log.error("Failed to perform post processing for data record ", e);
            }
        }

        return super.postProcessRecord(rec);
    }

    /** {@inheritDoc} */
    @Override protected IgniteCheckedException handleRecordException(
        @NotNull Exception e,
        @Nullable FileWALPointer ptr
    ) {
        if (e instanceof IgniteCheckedException)
            if (X.hasCause(e, IgniteDataIntegrityViolationException.class))
                // "curIdx" is an index in walFileDescriptors list.
                if (curIdx == walFileDescriptors.size() - 1)
                    // This means that there is no explicit last sengment, so we stop as if we reached the end
                    // of the WAL.
                    if (highBound.equals(DFLT_HIGH_BOUND))
                        return null;

        return super.handleRecordException(e, ptr);
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
            kernalCtx, null, null, false, false, false, false, false);

        final List<DataEntry> entries = dataRec.writeEntries();
        final List<DataEntry> postProcessedEntries = new ArrayList<>(entries.size());

        for (DataEntry dataEntry : entries) {
            final DataEntry postProcessedEntry = postProcessDataEntry(processor, fakeCacheObjCtx, dataEntry);

            postProcessedEntries.add(postProcessedEntry);
        }

        DataRecord res = dataRec instanceof MvccDataRecord ?
            new MvccDataRecord(postProcessedEntries, dataRec.timestamp()) :
            new DataRecord(postProcessedEntries, dataRec.timestamp());

        res.size(dataRec.size());
        res.position(dataRec.position());

        return res;
    }

    /**
     * Converts entry or lazy data entry into unwrapped entry
     *
     * @param processor cache object processor for de-serializing objects.
     * @param fakeCacheObjCtx cache object context for de-serializing binary and unwrapping objects.
     * @param dataEntry entry to process
     * @return post precessed entry
     * @throws IgniteCheckedException if failed
     */
    private @NotNull DataEntry postProcessDataEntry(
        final IgniteCacheObjectProcessor processor,
        final CacheObjectContext fakeCacheObjCtx,
        final DataEntry dataEntry) throws IgniteCheckedException {
        if (dataEntry instanceof EncryptedDataEntry)
            return dataEntry;

        final KeyCacheObject key;
        final CacheObject val;
        boolean keepBinary = this.keepBinary || !fakeCacheObjCtx.kernalContext().marshallerContext().initialized();

        if (dataEntry instanceof MarshalledDataEntry) {
            final MarshalledDataEntry lazyDataEntry = (MarshalledDataEntry)dataEntry;

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

        return unwrapDataEntry(fakeCacheObjCtx, dataEntry, key, val, keepBinary);
    }

    /**
     * Unwrap data entry.
     * @param coCtx CacheObject context.
     * @param dataEntry Data entry.
     * @param key Entry key.
     * @param val Entry value.
     * @param keepBinary Don't convert non primitive types.
     * @return Unwrapped entry.
     */
    private DataEntry unwrapDataEntry(CacheObjectContext coCtx, DataEntry dataEntry,
        KeyCacheObject key, CacheObject val, boolean keepBinary) {
        if (dataEntry instanceof MvccDataEntry)
            return new UnwrapMvccDataEntry(
                dataEntry.cacheId(),
                key,
                val,
                dataEntry.op(),
                dataEntry.nearXidVersion(),
                dataEntry.writeVersion(),
                dataEntry.expireTime(),
                dataEntry.partitionId(),
                dataEntry.partitionCounter(),
                ((MvccDataEntry)dataEntry).mvccVer(),
                coCtx,
                keepBinary);
        else
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
                coCtx,
                keepBinary);
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
        SegmentIO fileIO, RecordSerializer ser, FileInput in
    ) {
        return new ReadFileHandle(fileIO, ser, in, null);
    }
}
