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

package org.apache.ignite.internal.processors.igfs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.igfs.IgfsMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * IGFS metrics adapter.
 */
public class IgfsMetricsAdapter implements IgfsMetrics, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Used space on local node. */
    private long locSpaceSize;

    /** Maximum space. */
    private long maxSpaceSize;

    /** Secondary file system used space. */
    private long secondarySpaceSize;

    /** Number of directories. */
    private int dirsCnt;

    /** Number of files. */
    private int filesCnt;

    /** Number of files opened for read. */
    private int filesOpenedForRead;

    /** Number of files opened for write. */
    private int filesOpenedForWrite;

    /** Total blocks read. */
    private long blocksReadTotal;

    /** Total blocks remote read. */
    private long blocksReadRmt;

    /** Total blocks write. */
    private long blocksWrittenTotal;

    /** Total blocks write remote. */
    private long blocksWrittenRmt;

    /** Total bytes read. */
    private long bytesRead;

    /** Total bytes read time. */
    private long bytesReadTime;

    /** Total bytes write. */
    private long bytesWritten;

    /** Total bytes write time. */
    private long bytesWriteTime;

    /**
     * {@link Externalizable} support.
     */
    public IgfsMetricsAdapter() {
        // No-op.
    }

    /**
     * @param locSpaceSize Used space on local node.
     * @param maxSpaceSize Maximum space size.
     * @param secondarySpaceSize Secondary space size.
     * @param dirsCnt Number of directories.
     * @param filesCnt Number of files.
     * @param filesOpenedForRead Number of files opened for read.
     * @param filesOpenedForWrite Number of files opened for write.
     * @param blocksReadTotal Total blocks read.
     * @param blocksReadRmt Total blocks read remotely.
     * @param blocksWrittenTotal Total blocks written.
     * @param blocksWrittenRmt Total blocks written remotely.
     * @param bytesRead Total bytes read.
     * @param bytesReadTime Total bytes read time.
     * @param bytesWritten Total bytes written.
     * @param bytesWriteTime Total bytes write time.
     */
    public IgfsMetricsAdapter(long locSpaceSize, long maxSpaceSize, long secondarySpaceSize, int dirsCnt,
        int filesCnt, int filesOpenedForRead, int filesOpenedForWrite, long blocksReadTotal, long blocksReadRmt,
        long blocksWrittenTotal, long blocksWrittenRmt, long bytesRead, long bytesReadTime, long bytesWritten,
        long bytesWriteTime) {
        this.locSpaceSize = locSpaceSize;
        this.maxSpaceSize = maxSpaceSize;
        this.secondarySpaceSize = secondarySpaceSize;
        this.dirsCnt = dirsCnt;
        this.filesCnt = filesCnt;
        this.filesOpenedForRead = filesOpenedForRead;
        this.filesOpenedForWrite = filesOpenedForWrite;
        this.blocksReadTotal = blocksReadTotal;
        this.blocksReadRmt = blocksReadRmt;
        this.blocksWrittenTotal = blocksWrittenTotal;
        this.blocksWrittenRmt = blocksWrittenRmt;
        this.bytesRead = bytesRead;
        this.bytesReadTime = bytesReadTime;
        this.bytesWritten = bytesWritten;
        this.bytesWriteTime = bytesWriteTime;
    }

    /** {@inheritDoc} */
    @Override public long localSpaceSize() {
        return locSpaceSize;
    }

    /** {@inheritDoc} */
    @Override public long maxSpaceSize() {
        return maxSpaceSize;
    }

    /** {@inheritDoc} */
    @Override public long secondarySpaceSize() {
        return secondarySpaceSize;
    }

    /** {@inheritDoc} */
    @Override public int directoriesCount() {
        return dirsCnt;
    }

    /** {@inheritDoc} */
    @Override public int filesCount() {
        return filesCnt;
    }

    /** {@inheritDoc} */
    @Override public int filesOpenedForRead() {
        return filesOpenedForRead;
    }

    /** {@inheritDoc} */
    @Override public int filesOpenedForWrite() {
        return filesOpenedForWrite;
    }

    /** {@inheritDoc} */
    @Override public long blocksReadTotal() {
        return blocksReadTotal;
    }

    /** {@inheritDoc} */
    @Override public long blocksReadRemote() {
        return blocksReadRmt;
    }

    /** {@inheritDoc} */
    @Override public long blocksWrittenTotal() {
        return blocksWrittenTotal;
    }

    /** {@inheritDoc} */
    @Override public long blocksWrittenRemote() {
        return blocksWrittenRmt;
    }

    /** {@inheritDoc} */
    @Override public long bytesRead() {
        return bytesRead;
    }

    /** {@inheritDoc} */
    @Override public long bytesReadTime() {
        return bytesReadTime;
    }

    /** {@inheritDoc} */
    @Override public long bytesWritten() {
        return bytesWritten;
    }

    /** {@inheritDoc} */
    @Override public long bytesWriteTime() {
        return bytesWriteTime;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(locSpaceSize);
        out.writeLong(maxSpaceSize);
        out.writeLong(secondarySpaceSize);
        out.writeInt(dirsCnt);
        out.writeInt(filesCnt);
        out.writeInt(filesOpenedForRead);
        out.writeInt(filesOpenedForWrite);
        out.writeLong(blocksReadTotal);
        out.writeLong(blocksReadRmt);
        out.writeLong(blocksWrittenTotal);
        out.writeLong(blocksWrittenRmt);
        out.writeLong(bytesRead);
        out.writeLong(bytesReadTime);
        out.writeLong(bytesWritten);
        out.writeLong(bytesWriteTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        locSpaceSize = in.readLong();
        maxSpaceSize = in.readLong();
        secondarySpaceSize = in.readLong();
        dirsCnt = in.readInt();
        filesCnt = in.readInt();
        filesOpenedForRead = in.readInt();
        filesOpenedForWrite = in.readInt();
        blocksReadTotal = in.readLong();
        blocksReadRmt = in.readLong();
        blocksWrittenTotal = in.readLong();
        blocksWrittenRmt = in.readLong();
        bytesRead = in.readLong();
        bytesReadTime = in.readLong();
        bytesWritten = in.readLong();
        bytesWriteTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetricsAdapter.class, this);
    }
}