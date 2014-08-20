/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for {@link GridGgfsMetrics}.
 */
public class VisorGgfsMetrics implements Serializable {
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
     * @param m GGFS metrics.
     * @return Data transfer object for given GGFS metrics.
     */
    public static VisorGgfsMetrics from(GridGgfsMetrics m) {
        assert m != null;

        VisorGgfsMetrics metrics = new VisorGgfsMetrics();

        metrics.totalSpaceSize(m.maxSpaceSize());
        metrics.usedSpaceSize(m.localSpaceSize());
        metrics.foldersCount(m.directoriesCount());
        metrics.filesCount(m.filesCount());
        metrics.filesOpenedForRead(m.filesOpenedForRead());
        metrics.filesOpenedForWrite(m.filesOpenedForWrite());
        metrics.blocksRead(m.blocksReadTotal());
        metrics.blocksReadRemote(m.blocksReadRemote());
        metrics.blocksWritten(m.blocksWrittenTotal());
        metrics.blocksWrittenRemote(m.blocksWrittenRemote());
        metrics.bytesRead(m.bytesRead());
        metrics.bytesReadTime(m.bytesReadTime());
        metrics.bytesWritten(m.bytesWritten());
        metrics.bytesWriteTime(m.bytesWriteTime());

        return metrics;
    }

    /**
     * Add given metrics.
     *
     * @param m Metrics to add.
     * @return Self for method chaining.
     */
    public VisorGgfsMetrics add(VisorGgfsMetrics m) {
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
    public VisorGgfsMetrics aggregate(int n) {
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
     * @param totalSpaceSz New maximum amount of data that can be stored on local node.
     */
    public void totalSpaceSize(long totalSpaceSz) {
        this.totalSpaceSz = totalSpaceSz;
    }

    /**
     * @return Local used space in bytes on local node.
     */
    public long usedSpaceSize() {
        return usedSpaceSz;
    }

    /**
     * @param usedSpaceSz New local used space in bytes on local node.
     */
    public void usedSpaceSize(long usedSpaceSz) {
        this.usedSpaceSz = usedSpaceSz;
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
     * @param foldersCnt New number of directories created in file system.
     */
    public void foldersCount(int foldersCnt) {
        this.foldersCnt = foldersCnt;
    }

    /**
     * @return Number of files stored in file system.
     */
    public int filesCount() {
        return filesCnt;
    }

    /**
     * @param filesCnt New number of files stored in file system.
     */
    public void filesCount(int filesCnt) {
        this.filesCnt = filesCnt;
    }

    /**
     * @return Number of files that are currently opened for reading on local node.
     */
    public int filesOpenedForRead() {
        return filesOpenedForRd;
    }

    /**
     * @param filesOpenedForRd New number of files that are currently opened for reading on local node.
     */
    public void filesOpenedForRead(int filesOpenedForRd) {
        this.filesOpenedForRd = filesOpenedForRd;
    }

    /**
     * @return Number of files that are currently opened for writing on local node.
     */
    public int filesOpenedForWrite() {
        return filesOpenedForWrt;
    }

    /**
     * @param filesOpenedForWrt New number of files that are currently opened for writing on local node.
     */
    public void filesOpenedForWrite(int filesOpenedForWrt) {
        this.filesOpenedForWrt = filesOpenedForWrt;
    }

    /**
     * @return Total blocks read, local and remote.
     */
    public long blocksRead() {
        return blocksRd;
    }

    /**
     * @param blocksRd New total blocks read, local and remote.
     */
    public void blocksRead(long blocksRd) {
        this.blocksRd = blocksRd;
    }

    /**
     * @return Total remote blocks read.
     */
    public long blocksReadRemote() {
        return blocksRdRmt;
    }

    /**
     * @param blocksRdRmt New total remote blocks read.
     */
    public void blocksReadRemote(long blocksRdRmt) {
        this.blocksRdRmt = blocksRdRmt;
    }

    /**
     * @return Total blocks write, local and remote.
     */
    public long blocksWritten() {
        return blocksWrt;
    }

    /**
     * @param blocksWrt New total blocks write, local and remote.
     */
    public void blocksWritten(long blocksWrt) {
        this.blocksWrt = blocksWrt;
    }

    /**
     * @return Total remote blocks write.
     */
    public long blocksWrittenRemote() {
        return blocksWrtRmt;
    }

    /**
     * @param blocksWrtRmt New total remote blocks write.
     */
    public void blocksWrittenRemote(long blocksWrtRmt) {
        this.blocksWrtRmt = blocksWrtRmt;
    }

    /**
     * @return Total bytes read.
     */
    public long bytesRead() {
        return bytesRd;
    }

    /**
     * @param bytesRd New total bytes read.
     */
    public void bytesRead(long bytesRd) {
        this.bytesRd = bytesRd;
    }

    /**
     * @return Total bytes read time.
     */
    public long bytesReadTime() {
        return bytesRdTm;
    }

    /**
     * @param bytesRdTm New total bytes read time.
     */
    public void bytesReadTime(long bytesRdTm) {
        this.bytesRdTm = bytesRdTm;
    }

    /**
     * @return Total bytes write.
     */
    public long bytesWritten() {
        return bytesWrt;
    }

    /**
     * @param bytesWrt New total bytes write.
     */
    public void bytesWritten(long bytesWrt) {
        this.bytesWrt = bytesWrt;
    }

    /**
     * @return Total bytes write time.
     */
    public long bytesWriteTime() {
        return bytesWrtTm;
    }

    /**
     * @param bytesWrtTm New total bytes write time.
     */
    public void bytesWriteTime(long bytesWrtTm) {
        this.bytesWrtTm = bytesWrtTm;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGgfsMetrics.class, this);
    }
}
