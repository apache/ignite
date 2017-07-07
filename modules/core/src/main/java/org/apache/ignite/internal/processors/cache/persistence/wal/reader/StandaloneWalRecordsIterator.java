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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.AbstractWalRecordsIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentEofException;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
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

    /**
     * Creates iterator in directory scan mode
     *
     * @param walFilesDir Wal files directory. Should already contain node consistent ID as subfolder
     * @param log Logger.
     * @param sharedCtx Shared context.
     */
    StandaloneWalRecordsIterator(
        @NotNull final File walFilesDir,
        @NotNull final IgniteLogger log,
        @NotNull final GridCacheSharedContext sharedCtx) throws IgniteCheckedException {
        super(log,
            sharedCtx,
            new RecordV1Serializer(sharedCtx),
            BUF_SIZE);
        init(walFilesDir, false, null);
        advance();
    }

    /**
     * Creates iterator in file-by-file iteration mode. Directory
     *
     * @param log Logger.
     * @param sharedCtx Shared context.
     * @param workDir Work directory is scanned, false - archive
     * @param walFiles Wal files.
     */
    StandaloneWalRecordsIterator(
        @NotNull final IgniteLogger log,
        @NotNull final GridCacheSharedContext sharedCtx,
        final boolean workDir,
        @NotNull final File... walFiles) throws IgniteCheckedException {
        super(log,
            sharedCtx,
            new RecordV1Serializer(sharedCtx),
            BUF_SIZE);
        this.workDir = workDir;
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
     * Header record and its position is checked. WAL position is used to deremine real index.
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

            try (RandomAccessFile rf = new RandomAccessFile(file, "r");) {
                final FileChannel ch = rf.getChannel();
                final ByteBuffer buf = ByteBuffer.allocate(HEADER_RECORD_SIZE);

                buf.order(ByteOrder.nativeOrder());

                final DataInput in = new FileInput(ch, buf);
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
            log.info("Missing WAL segment in the archive: " + e.getMessage());
            return null;
        }
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
