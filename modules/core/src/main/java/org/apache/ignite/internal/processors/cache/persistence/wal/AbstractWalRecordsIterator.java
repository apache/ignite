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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.HeaderRecord;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

/**
 * Iterator over WAL segments.
 */
public abstract class AbstractWalRecordsIterator extends GridCloseableIteratorAdapter<IgniteBiTuple<WALPointer, WALRecord>>
    implements WALIterator {
    /** */
    private static final long serialVersionUID = 0L;

    /** Current record preloaded, to be returned on next() */
    protected IgniteBiTuple<WALPointer, WALRecord> curRec;

    /** */
    protected long curIdx = -1;

    /** */
    protected FileWriteAheadLogManager.ReadFileHandle curHandle;

    protected IgniteLogger log;

    @NotNull
    private GridCacheSharedContext sharedContext;

    @NotNull
    private RecordSerializer serializer;


    /** */
    private ByteBuffer buf;

    /**
     *  @param log
     * @param sharedCtx Shared context
     * @param serializer Serializer of current version to read headers.
     * @param bufSize
     */
    protected AbstractWalRecordsIterator(IgniteLogger log,
        @NotNull GridCacheSharedContext sharedCtx,
        @NotNull RecordSerializer serializer,
        int bufSize) {
        this.log = log;
        this.sharedContext = sharedCtx;
        this.serializer = serializer;

        // Do not allocate direct buffer for iterator.
        buf = ByteBuffer.allocate(bufSize);
        buf.order(ByteOrder.nativeOrder());

    }

    protected static FileWriteAheadLogManager.FileDescriptor[] loadFileDescriptors(File walFilesDir) {
        return FileWriteAheadLogManager.scan(walFilesDir.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER));
    }

    /** {@inheritDoc} */
    @Override protected IgniteBiTuple<WALPointer, WALRecord> onNext() throws IgniteCheckedException {
        IgniteBiTuple<WALPointer, WALRecord> ret = curRec;

        advance();

        return ret;
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() throws IgniteCheckedException {
        return curRec != null;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void advance() throws IgniteCheckedException {
        while (true) {
            advanceRecord();

            if (curRec != null)
                return;
            else {
                advanceSegment();

                if (curHandle == null)
                    return;
            }
        }
    }

    protected abstract void advanceSegment() throws IgniteCheckedException;

    /**
     *
     */
    private void advanceRecord() {
        try {
            FileWriteAheadLogManager.ReadFileHandle hnd = curHandle;

            if (hnd != null) {
                RecordSerializer ser = hnd.ser;

                int pos = (int)hnd.in.position();

                FileWALPointer ptr = new FileWALPointer(hnd.idx, pos, 0);

                WALRecord rec = ser.readRecord(hnd.in, ptr);

                ptr.length(rec.size());

                //using diamond operator here can break compile for 7
                curRec = new IgniteBiTuple<WALPointer, WALRecord>(ptr, rec);
            }
        }
        catch (IOException | IgniteCheckedException e) {
            if (!(e instanceof SegmentEofException)) {
                if (log.isInfoEnabled())
                    log.info("Stopping WAL iteration due to an exception: " + e.getMessage());
            }

            curRec = null;
        }
    }


    /**
     * @param desc File descriptor.
     * @param start Optional start pointer.
     * @return Initialized file handle.
     * @throws FileNotFoundException If segment file is missing.
     * @throws IgniteCheckedException If initialized failed due to another unexpected error.
     */
    protected FileWriteAheadLogManager.ReadFileHandle initReadHandle(FileWriteAheadLogManager.FileDescriptor desc, FileWALPointer start)
        throws IgniteCheckedException, FileNotFoundException {
        try {
            RandomAccessFile rf = new RandomAccessFile(desc.file, "r");

            try {
                FileChannel channel = rf.getChannel();
                FileInput in = new FileInput(channel, buf);

                // Header record must be agnostic to the serializer version.
                WALRecord rec = serializer.readRecord(in,
                    new FileWALPointer(desc.idx, (int)channel.position(), 0));

                if (rec == null)
                    return null;

                if (rec.type() != WALRecord.RecordType.HEADER_RECORD)
                    throw new IOException("Missing file header record: " + desc.file.getAbsoluteFile());

                int ver = ((HeaderRecord)rec).version();

                RecordSerializer ser = FileWriteAheadLogManager.forVersion(sharedContext, ver);

                if (start != null && desc.idx == start.index())
                    in.seek(start.fileOffset());

                return new FileWriteAheadLogManager.ReadFileHandle(rf, desc.idx, sharedContext.igniteInstanceName(), ser, in);
            }
            catch (SegmentEofException | EOFException ignore) {
                try {
                    rf.close();
                }
                catch (IOException ce) {
                    throw new IgniteCheckedException(ce);
                }

                return null;
            }
            catch (IOException | IgniteCheckedException e) {
                try {
                    rf.close();
                }
                catch (IOException ce) {
                    e.addSuppressed(ce);
                }

                throw e;
            }
        }
        catch (FileNotFoundException e) {
            throw e;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(
                "Failed to initialize WAL segment: " + desc.file.getAbsolutePath(), e);
        }
    }

}
