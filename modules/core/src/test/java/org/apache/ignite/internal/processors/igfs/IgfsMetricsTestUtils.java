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

import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsMetrics;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.local.LocalIgfsSecondaryFileSystem;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

/**
 * Static metrics tests to be reused in various test classes.
 */
public class IgfsMetricsTestUtils {
    /**
     * Basic metrics test. (Static delegate).
     *
     * @throws Exception If failed.
     */
    public static void testMetrics0(IgniteFileSystem fs, IgfsSecondaryFileSystemTestAdapter sec) throws Exception {
        Assert.assertNotNull(fs);

        fs.format();

        fs.resetMetrics();

        IgfsMetrics m = fs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(0, m.directoriesCount());
        Assert.assertEquals(0, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        fs.mkdirs(new IgfsPath("/primary/dir1"));

        m = fs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(2, m.directoriesCount());
        Assert.assertEquals(0, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        fs.mkdirs(new IgfsPath("/primary/dir1/dir2/dir3"));
        fs.mkdirs(new IgfsPath("/primary/dir4"));

        m = fs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(0, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        IgfsOutputStream out1 = fs.create(new IgfsPath("/primary/dir1/file1"), false);
        IgfsOutputStream out2 = fs.create(new IgfsPath("/primary/dir1/file2"), false);
        IgfsOutputStream out3 = fs.create(new IgfsPath("/primary/dir1/dir2/file"), false);

        m = fs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(3, m.filesOpenedForWrite());

        out1.write(new byte[10]);
        out2.write(new byte[20]);
        out3.write(new byte[30]);

        out1.close();

        m = fs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(2, m.filesOpenedForWrite());

        out2.close();
        out3.close();

        m = fs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        IgfsOutputStream out = fs.append(new IgfsPath("/primary/dir1/file1"), false);

        out.write(new byte[20]);

        m = fs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(1, m.filesOpenedForWrite());

        out.write(new byte[20]);

        out.close();

        m = fs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        IgfsInputStream in1 = fs.open(new IgfsPath("/primary/dir1/file1"));
        IgfsInputStream in2 = fs.open(new IgfsPath("/primary/dir1/file2"));

        m = fs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(2, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        in1.close();
        in2.close();

        m = fs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        fs.delete(new IgfsPath("/primary/dir1/file1"), false);
        fs.delete(new IgfsPath("/primary/dir1/dir2"), true);

        m = fs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(3, m.directoriesCount());
        Assert.assertEquals(1, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        fs.format();

        // NB: format does not clear secondary file system.
        if (sec != null)
            sec.format();

        m = fs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(0, m.directoriesCount());
        Assert.assertEquals(0, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());
    }

    /**
     * Test for multiple closings. (Static delegate).
     *
     * @throws Exception If failed.
     */
    public static void testMultipleClose0(IgniteFileSystem fs) throws Exception {
        fs.format();

        fs.resetMetrics();

        IgfsOutputStream out = fs.create(new IgfsPath("/primary/file"), false);

        out.close();
        out.close();

        IgfsInputStream in = fs.open(new IgfsPath("/primary/file"));

        in.close();
        in.close();

        IgfsMetrics m = fs.metrics();

        Assert.assertEquals(0, m.filesOpenedForWrite());
        Assert.assertEquals(0, m.filesOpenedForRead());
    }

    /**
     *
     * @param initMetrics
     * @param metrics
     * @param e
     * @throws Exception
     */
    private static void checkMetricExpectations(IgfsMetrics initMetrics, IgfsMetrics metrics, MetricExpectations e)
        throws Exception {
        checkBlockMetrics(initMetrics, metrics,
            e.totalBlocksRead(), e.rmtBlocksRead(), e.bytesRead(),
            e.totalBlocksWritten(), e.rmtBlocksWritten(), e.bytesWritten());
    }


    /**
     * Test block metrics. (Static delegate).
     *
     * @param igfs The primary IGFS.
     * @param secFileSys The secondary file system represented as {@link IgfsSecondaryFileSystem}.
     * @param igfsSecondary The secondary file system represented as {@link IgfsImpl}, if there is such.
     * @param dual If the secondary file system is present. Note that {@code dual} may not always the same as
     * {@code igfsSecondary != null}, because in case of Hadoop we have secondary file system, but do not have
     * direct access to IgfsImpl instance that implements it. Also some PRIMARY mode tests ({@code dual} is
     * {@code false}) have {@code secFileSys != null}.
     * @throws Exception If failed.
     */
    public static void testBlockMetrics0(IgfsEx igfs, @Nullable IgfsSecondaryFileSystem secFileSys,
        @Nullable IgfsImpl igfsSecondary, boolean dual, boolean proxy) throws Exception {
        assert !dual || (secFileSys != null);
        assert !proxy || (secFileSys != null);
        assert !(dual && proxy);

        int prefetch = igfs.configuration().getPrefetchBlocks();
        int seqBeforePrefetch = igfs.configuration().getSequentialReadsBeforePrefetch();

        IgfsPath file00 = new IgfsPath("/file00");

        // Create empty file.
        igfs.create(file00, 256, true, null, 1, 256, null).close();

        final int blockSize = igfs.info(file00).blockSize();

        final MetricExpectations e = new MetricExpectations(dual, proxy, blockSize);

        // TODO: not disable prefetch, but consider it in assertions.
        igfs.configuration().setPrefetchBlocks(0);

        Assert.assertTrue("https://issues.apache.org/jira/browse/IGNITE-3664", !(secFileSys instanceof
            LocalIgfsSecondaryFileSystem));

        igfs.format();

        igfs.resetMetrics();

        IgfsPath fileRemote = null;

        IgfsPath file1 = new IgfsPath("/file1");
        IgfsPath file2 = new IgfsPath("/file2");

        // TODO:
        assert dual == IgfsUtils.isDualMode(igfs.mode(file1)) || igfs.mode(file1) == IgfsMode.PROXY;
        assert dual == IgfsUtils.isDualMode(igfs.mode(file2)) || igfs.mode(file2) == IgfsMode.PROXY;

        int rmtBlockSize = -1;

        if (igfsSecondary != null) {
            igfsSecondary.format();

            igfsSecondary.resetMetrics();

            fileRemote = new IgfsPath("/fileRemote");

            // Create remote file and write some data to it.
            try (IgfsOutputStream out = igfsSecondary.create(fileRemote, 256, true, null, 1, 256, null)) {
                rmtBlockSize = igfsSecondary.info(fileRemote).blockSize();

                out.write(new byte[rmtBlockSize * 5]);
            }

            assert rmtBlockSize == blockSize;
        }

        // Start metrics measuring.
        final IgfsMetrics initMetrics = igfs.metrics();
//
//        assert e.blockSize == blockSize;

        assert blockSize > 0 : "Unexpected block size: " + blockSize;

        checkBlockMetrics(initMetrics, igfs.metrics(), 0, 0, 0, 0, 0, 0);

        e.primBlocksWritten = 7;

        // Write two blocks to the file.
        try (IgfsOutputStream os = igfs.append(file1, true)) {
            os.write(new byte[blockSize * (int)e.primBlocksWritten]);
        }

        checkMetricExpectations(initMetrics, igfs.metrics(), e);

        // Write one more file (one block).
        try (IgfsOutputStream os = igfs.create(file2, 256, true, null, 1, 256, null)) {
            os.write(new byte[blockSize]);
        }

        e.primBlocksWritten++;

        checkMetricExpectations(initMetrics, igfs.metrics(), e);

        e.primBlocksRead = 2;

        // Read data from the first file.
        try (IgfsInputStream is = igfs.open(file1)) {
            is.readFully(0, new byte[blockSize * (int)e.primBlocksRead]);
        }

        checkMetricExpectations(initMetrics, igfs.metrics(), e);

        // Read data from the second file with hits.
        try (IgfsInputStream is = igfs.open(file2)) {
            ((IgfsInputStreamImpl)is).readChunks(0, blockSize);
        }

        e.primBlocksRead++;

//        checkBlockMetrics(initMetrics, igfs.metrics(),
//            (int)m.blocksRead, m.totalBlocksRead(), m.rmtBlocksRead(),
//            blocksWritten, (dual || proxy) ? blocksWritten : 0, blockSize * blocksWritten);
        checkMetricExpectations(initMetrics, igfs.metrics(), e);

        // Clear the first file.
        igfs.create(file1, true).close();

//        checkBlockMetrics(initMetrics, igfs.metrics(),
//            blocksRead, proxy ? blocksRead : 0, blockSize * blocksRead,
//            blocksWritten, (dual || proxy) ? blocksWritten : 0, blockSize * blocksWritten);
        checkMetricExpectations(initMetrics, igfs.metrics(), e);

        // Delete the second file.
        igfs.delete(file2, false);

//        checkBlockMetrics(initMetrics, igfs.metrics(),
//            blocksRead, proxy ? blocksRead : 0, blockSize * blocksRead,
//            blocksWritten, (dual || proxy) ? blocksWritten : 0, blockSize * blocksWritten);
        checkMetricExpectations(initMetrics, igfs.metrics(), e);

        IgfsMetrics metrics;

        //int rmtBlocksRead = 0;

        if (fileRemote != null) {
            // Read remote file.
            try (IgfsInputStream is = igfs.open(fileRemote)) {
                ((IgfsInputStreamImpl)is).readChunks(0, rmtBlockSize);
            }

            e.rmtBlocksRead++;

//            checkBlockMetrics(initMetrics, igfs.metrics(),
//                blocksRead + rmtBlocksRead, proxy ? (blocksRead + rmtBlocksRead) : 0, blockSize * blocksRead + rmtBlockSize * rmtBlocksRead,
//                blocksWritten, (dual || proxy) ? blocksWritten : 0, blockSize * blocksWritten);
            checkMetricExpectations(initMetrics, igfs.metrics(), e);

            // Lets wait for blocks will be placed to cache
            U.sleep(300);

            // Read remote file again.
            try (IgfsInputStream is = igfs.open(fileRemote)) {
                ((IgfsInputStreamImpl)is).readChunks(0, rmtBlockSize);
            }

            if (dual)
                e.primBlocksRead++;
            else
                e.rmtBlocksRead++;

            //checkBlockMetrics(initMetrics, igfs.metrics(), 5, 1, blockSize * 3 + rmtBlockSize * 2, 3, 3, blockSize * 3);
//            checkBlockMetrics(initMetrics, igfs.metrics(),
//                blocksRead + rmtBlocksRead, proxy ? (blocksRead + rmtBlocksRead) : 0, blockSize * blocksRead + rmtBlockSize * rmtBlocksRead,
//                blocksWritten, (dual || proxy) ? blocksWritten : 0, blockSize * blocksWritten);
            checkMetricExpectations(initMetrics, igfs.metrics(), e);

            metrics = igfs.metrics();

            //long rmtBlocks0 = metrics.secondarySpaceSize() / rmtBlockSize;

            // TODO
            // Assert.assertEquals(rmtBlockSize * blocksWritten, metrics.secondarySpaceSize());

            // Write some data to the file working in DUAL mode.
            try (IgfsOutputStream os = igfs.append(fileRemote, false)) {
                os.write(new byte[rmtBlockSize]);
            }

            e.primBlocksWritten++;

            checkMetricExpectations(initMetrics, igfs.metrics(), e);

            metrics = igfs.metrics();

            // assert metrics.secondarySpaceSize() == rmtBlockSize * blocksWritten;

            igfs.delete(fileRemote, false);

            U.sleep(300);

            assert igfs.metrics().secondarySpaceSize() == 0;

//            // Write partial block to the first file.
//            try (IgfsOutputStream os = igfs.append(file1, false)) {
//                os.write(new byte[blockSize / 2]);
//            }
//
//            e.primBlocksWritten += 0.5;
//
//            checkMetricExpectations(initMetrics, igfs.metrics(), e);
//
//            // Now read partial block.
//            // Read remote file again.
//            try (IgfsInputStream is = igfs.open(file1)) {
//                is.seek(blockSize * (int)e.primBlocksWritten);
//
//                ((IgfsInputStreamImpl)is).readChunks(0, blockSize / 2);
//            }
//
//            e.primBlocksRead += 0.5;
//
//            checkMetricExpectations(initMetrics, igfs.metrics(), e);
        }

        igfs.resetMetrics();

        metrics = igfs.metrics();

        assert metrics.blocksReadTotal() == 0;
        assert metrics.blocksReadRemote() == 0;
        assert metrics.blocksWrittenTotal() == 0;
        assert metrics.blocksWrittenRemote() == 0;
        assert metrics.bytesRead() == 0;
        assert metrics.bytesReadTime() == 0;
        assert metrics.bytesWritten() == 0;
        assert metrics.bytesWriteTime() == 0;

        assert metrics.bytesReadTime() >= 0;
        assert metrics.bytesWriteTime() >= 0;
    }

    private static class MetricExpectations {
        private final boolean proxy;
        private final boolean dual;

        private final int blockSize;
        private final int rmtBlockSize;

        /** How many blocks read from the primary file system after primary write. */
        double primBlocksRead;

        /** How many blocks read read from primary if the
         * file existed in secondary but did not exist in primary (fetched up). */
        double rmtBlocksRead;

        /** How many blocks written to the primary file system. */
        double primBlocksWritten;

        MetricExpectations(boolean dual, boolean proxy, int blockSize) {
            this.dual = dual;
            this.proxy = proxy;

            this.blockSize = blockSize;
            this.rmtBlockSize = blockSize; // NB: currently same value
        }

        double totalBlocksRead0() {
            if (proxy || dual)
                return primBlocksRead + rmtBlocksRead;

//            if (dual)
//                return rmtBlocksRead;
//
//            return 0;
            return primBlocksRead;
        }

        int totalBlocksRead() {
            return (int)Math.ceil(totalBlocksRead0());
        }

        double rmtBlocksRead0() {
//            if (proxy)
//                return primBlocksRead;

            return rmtBlocksRead;
        }

        int rmtBlocksRead() {
            if (proxy)
                return totalBlocksRead();

            return (int)Math.ceil(rmtBlocksRead0());
        }

        long bytesRead() {
            return (long)(blockSize * primBlocksRead + rmtBlockSize * rmtBlocksRead);
        }

        int totalBlocksWritten() {
            System.out.println("primBlocksWritten = " + primBlocksWritten);

            return (int)primBlocksWritten;
        }

        int rmtBlocksWritten() {
            return (dual || proxy) ? (int)primBlocksWritten : 0;
        }

        long bytesWritten() {
            return (long)(blockSize * primBlocksWritten);
        }
    }

    /**
     * Ensure overall block-related metrics correctness.
     *
     * @param initMetrics Initial metrics.
     * @param metrics Metrics to check.
     * @param blocksRead Blocks read remote.
     * @param blocksReadRemote Blocks read remote.
     * @param bytesRead Bytes read.
     * @param blocksWrite Blocks write.
     * @param blocksWriteRemote Blocks write remote.
     * @param bytesWrite Bytes write.
     * @throws Exception If failed.
     */
    private static void checkBlockMetrics(IgfsMetrics initMetrics, IgfsMetrics metrics, long blocksRead,
        long blocksReadRemote, long bytesRead, long blocksWrite, long blocksWriteRemote, long bytesWrite)
        throws Exception {
        assert metrics != null;

        Assert.assertEquals(blocksRead, metrics.blocksReadTotal() - initMetrics.blocksReadTotal());
        Assert.assertEquals(blocksReadRemote, metrics.blocksReadRemote() - initMetrics.blocksReadRemote());
        Assert.assertEquals(bytesRead, metrics.bytesRead() - initMetrics.bytesRead());

        Assert.assertEquals(blocksWrite, metrics.blocksWrittenTotal() - initMetrics.blocksWrittenTotal());
        Assert.assertEquals(blocksWriteRemote, metrics.blocksWrittenRemote() - initMetrics.blocksWrittenRemote());
        Assert.assertEquals(bytesWrite, metrics.bytesWritten() - initMetrics.bytesWritten());
    }
}