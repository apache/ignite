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

package org.apache.ignite.internal.visor.igfs;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object.
 */
@Deprecated
public class VisorIgfsMetrics extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Maximum amount of data that can be stored on local node. */
    private long totalSpaceSz;

    /** Local used space in bytes on local node. */
    private long usedSpaceSz;

    /** Number of directories created in file system. */
    private int foldersCnt;

    /** Number of files stored in file system. */
    private int filesCnt;

    /** Number of files that are currently opened for reading on local node. */
    private int filesOpenedForRd;

    /** Number of files that are currently opened for writing on local node. */
    private int filesOpenedForWrt;

    /** Total blocks read, local and remote. */
    private long blocksRd;

    /** Total remote blocks read. */
    private long blocksRdRmt;

    /** Total blocks write, local and remote. */
    private long blocksWrt;

    /** Total remote blocks write. */
    private long blocksWrtRmt;

    /** Total bytes read. */
    private long bytesRd;

    /** Total bytes read time. */
    private long bytesRdTm;

    /** Total bytes write. */
    private long bytesWrt;

    /** Total bytes write time. */
    private long bytesWrtTm;

    /**
     * Create data transfer object for given IGFS metrics.
     */
    public VisorIgfsMetrics() {
        // No-op.
    }

    /**
     * Add given metrics.
     *
     * @param m Metrics to add.
     * @return Self for method chaining.
     */
    public VisorIgfsMetrics add(VisorIgfsMetrics m) {
        assert m != null;

        totalSpaceSz += m.totalSpaceSz;
        usedSpaceSz += m.usedSpaceSz;
        foldersCnt += m.foldersCnt;
        filesCnt += m.filesCnt;
        filesOpenedForRd += m.filesOpenedForRd;
        filesOpenedForWrt += m.filesOpenedForWrt;
        blocksRd += m.blocksRd;
        blocksRdRmt += m.blocksRdRmt;
        blocksWrt += m.blocksWrt;
        blocksWrtRmt += m.blocksWrtRmt;
        bytesRd += m.bytesRd;
        bytesRdTm += m.bytesRdTm;
        bytesWrt += m.bytesWrt;
        bytesWrtTm += m.bytesWrtTm;

        return this;
    }

    /**
     * Aggregate metrics.
     *
     * @param n Nodes count.
     * @return Self for method chaining.
     */
    public VisorIgfsMetrics aggregate(int n) {
        if (n > 0) {
            foldersCnt /= n;
            filesCnt /= n;
        }

        return this;
    }

    /**
     * @return Maximum amount of data that can be stored on local node.
     */
    public long getTotalSpaceSize() {
        return totalSpaceSz;
    }

    /**
     * @return Local used space in bytes on local node.
     */
    public long getUsedSpaceSize() {
        return usedSpaceSz;
    }

    /**
     * @return Local free space in bytes on local node.
     */
    public long getFreeSpaceSize() {
        return totalSpaceSz - usedSpaceSz;
    }

    /**
     * @return Number of directories created in file system.
     */
    public int getFoldersCount() {
        return foldersCnt;
    }

    /**
     * @return Number of files stored in file system.
     */
    public int getFilesCount() {
        return filesCnt;
    }

    /**
     * @return Number of files that are currently opened for reading on local node.
     */
    public int getFilesOpenedForRead() {
        return filesOpenedForRd;
    }

    /**
     * @return Number of files that are currently opened for writing on local node.
     */
    public int getFilesOpenedForWrite() {
        return filesOpenedForWrt;
    }

    /**
     * @return Total blocks read, local and remote.
     */
    public long getBlocksRead() {
        return blocksRd;
    }

    /**
     * @return Total remote blocks read.
     */
    public long getBlocksReadRemote() {
        return blocksRdRmt;
    }

    /**
     * @return Total blocks write, local and remote.
     */
    public long getBlocksWritten() {
        return blocksWrt;
    }

    /**
     * @return Total remote blocks write.
     */
    public long getBlocksWrittenRemote() {
        return blocksWrtRmt;
    }

    /**
     * @return Total bytes read.
     */
    public long getBytesRead() {
        return bytesRd;
    }

    /**
     * @return Total bytes read time.
     */
    public long getBytesReadTime() {
        return bytesRdTm;
    }

    /**
     * @return Total bytes write.
     */
    public long getBytesWritten() {
        return bytesWrt;
    }

    /**
     * @return Total bytes write time.
     */
    public long getBytesWriteTime() {
        return bytesWrtTm;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(totalSpaceSz);
        out.writeLong(usedSpaceSz);
        out.writeInt(foldersCnt);
        out.writeInt(filesCnt);
        out.writeInt(filesOpenedForRd);
        out.writeInt(filesOpenedForWrt);
        out.writeLong(blocksRd);
        out.writeLong(blocksRdRmt);
        out.writeLong(blocksWrt);
        out.writeLong(blocksWrtRmt);
        out.writeLong(bytesRd);
        out.writeLong(bytesRdTm);
        out.writeLong(bytesWrt);
        out.writeLong(bytesWrtTm);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        totalSpaceSz = in.readLong();
        usedSpaceSz = in.readLong();
        foldersCnt = in.readInt();
        filesCnt = in.readInt();
        filesOpenedForRd = in.readInt();
        filesOpenedForWrt = in.readInt();
        blocksRd = in.readLong();
        blocksRdRmt = in.readLong();
        blocksWrt = in.readLong();
        blocksWrtRmt = in.readLong();
        bytesRd = in.readLong();
        bytesRdTm = in.readLong();
        bytesWrt = in.readLong();
        bytesWrtTm = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIgfsMetrics.class, this);
    }
}
