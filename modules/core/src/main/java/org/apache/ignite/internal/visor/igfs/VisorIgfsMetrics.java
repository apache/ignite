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

import java.io.Serializable;
import org.apache.ignite.igfs.IgfsMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for {@link IgfsMetrics}.
 */
public class VisorIgfsMetrics implements Serializable {
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
     * @param m IGFS metrics.
     * @return Data transfer object for given IGFS metrics.
     */
    public static VisorIgfsMetrics from(IgfsMetrics m) {
        assert m != null;

        VisorIgfsMetrics metrics = new VisorIgfsMetrics();

        metrics.totalSpaceSz = m.maxSpaceSize();
        metrics.usedSpaceSz = m.localSpaceSize();
        metrics.foldersCnt = m.directoriesCount();
        metrics.filesCnt = m.filesCount();
        metrics.filesOpenedForRd = m.filesOpenedForRead();
        metrics.filesOpenedForWrt = m.filesOpenedForWrite();
        metrics.blocksRd = m.blocksReadTotal();
        metrics.blocksRdRmt = m.blocksReadRemote();
        metrics.blocksWrt = m.blocksWrittenTotal();
        metrics.blocksWrtRmt = m.blocksWrittenRemote();
        metrics.bytesRd = m.bytesRead();
        metrics.bytesRdTm = m.bytesReadTime();
        metrics.bytesWrt = m.bytesWritten();
        metrics.bytesWrtTm = m.bytesWriteTime();

        return metrics;
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
    public long totalSpaceSize() {
        return totalSpaceSz;
    }

    /**
     * @return Local used space in bytes on local node.
     */
    public long usedSpaceSize() {
        return usedSpaceSz;
    }

    /**
     * @return Local free space in bytes on local node.
     */
    public long freeSpaceSize() {
        return totalSpaceSz - usedSpaceSz;
    }

    /**
     * @return Number of directories created in file system.
     */
    public int foldersCount() {
        return foldersCnt;
    }

    /**
     * @return Number of files stored in file system.
     */
    public int filesCount() {
        return filesCnt;
    }

    /**
     * @return Number of files that are currently opened for reading on local node.
     */
    public int filesOpenedForRead() {
        return filesOpenedForRd;
    }

    /**
     * @return Number of files that are currently opened for writing on local node.
     */
    public int filesOpenedForWrite() {
        return filesOpenedForWrt;
    }

    /**
     * @return Total blocks read, local and remote.
     */
    public long blocksRead() {
        return blocksRd;
    }

    /**
     * @return Total remote blocks read.
     */
    public long blocksReadRemote() {
        return blocksRdRmt;
    }

    /**
     * @return Total blocks write, local and remote.
     */
    public long blocksWritten() {
        return blocksWrt;
    }

    /**
     * @return Total remote blocks write.
     */
    public long blocksWrittenRemote() {
        return blocksWrtRmt;
    }

    /**
     * @return Total bytes read.
     */
    public long bytesRead() {
        return bytesRd;
    }

    /**
     * @return Total bytes read time.
     */
    public long bytesReadTime() {
        return bytesRdTm;
    }

    /**
     * @return Total bytes write.
     */
    public long bytesWritten() {
        return bytesWrt;
    }

    /**
     * @return Total bytes write time.
     */
    public long bytesWriteTime() {
        return bytesWrtTm;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIgfsMetrics.class, this);
    }
}