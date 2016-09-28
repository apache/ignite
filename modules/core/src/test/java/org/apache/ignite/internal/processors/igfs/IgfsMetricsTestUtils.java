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
    public static void testMetrics0(IgniteFileSystem fs) throws Exception {
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
     **/
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
        @Nullable IgfsImpl igfsSecondary, boolean dual) throws Exception {
        assert !dual || (secFileSys != null);

        Assert.assertTrue("https://issues.apache.org/jira/browse/IGNITE-3872", !(secFileSys instanceof
            LocalIgfsSecondaryFileSystem));

        igfs.format();

        igfs.resetMetrics();

        IgfsPath fileRemote = null;

        IgfsPath file1 = new IgfsPath("/file1");
        IgfsPath file2 = new IgfsPath("/file2");

        assert dual == IgfsUtils.isDualMode(igfs.mode(file1));
        assert dual == IgfsUtils.isDualMode(igfs.mode(file2));

        int rmtBlockSize = -1;

        if (igfsSecondary != null) {
            igfsSecondary.format();

            igfsSecondary.resetMetrics();

            fileRemote = new IgfsPath("/fileRemote");

            // Create remote file and write some data to it.
            IgfsOutputStream out = igfsSecondary.create(fileRemote, 256, true, null, 1, 256, null);

            rmtBlockSize = igfsSecondary.info(fileRemote).blockSize();

            out.write(new byte[rmtBlockSize]);
            out.close();
        }

        // Start metrics measuring.
        final IgfsMetrics initMetrics = igfs.metrics();

        // Create empty file.
        igfs.create(file1, 256, true, null, 1, 256, null).close();

        int blockSize = igfs.info(file1).blockSize();

        assert blockSize > 0 : "Unexpected block size: " + blockSize;

        checkBlockMetrics(initMetrics, igfs.metrics(), 0, 0, 0, 0, 0, 0);

        // Write two blocks to the file.
        IgfsOutputStream os = igfs.append(file1, false);

        os.write(new byte[blockSize * 2]);
        os.close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 0, 0, 0, 2, dual ? 2 : 0, blockSize * 2);

        // Write one more file (one block).
        os = igfs.create(file2, 256, true, null, 1, 256, null);
        os.write(new byte[blockSize]);
        os.close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 0, 0, 0, 3, dual ? 3 : 0, blockSize * 3);

        // Read data from the first file.
        IgfsInputStream is = igfs.open(file1);
        is.readFully(0, new byte[blockSize * 2]);
        is.close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 2, 0, blockSize * 2, 3, dual ? 3 : 0,
            blockSize * 3);

//        // Read data from the second file with hits.
//        is = igfs.open(file2);
//        is.readChunks(0, blockSize);
//        is.close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 3, 0, blockSize * 3, 3, dual ? 3 : 0,
            blockSize * 3);

        // Clear the first file.
        igfs.create(file1, true).close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 3, 0, blockSize * 3, 3, dual ? 3 : 0,
            blockSize * 3);

        // Delete the second file.
        igfs.delete(file2, false);

        checkBlockMetrics(initMetrics, igfs.metrics(), 3, 0, blockSize * 3, 3, dual ? 3 : 0,
            blockSize * 3);

        IgfsMetrics metrics;

        if (fileRemote != null) {
//            // Read remote file.
//            is = igfs.open(fileRemote);
//            is.readChunks(0, rmtBlockSize);
//            is.close();

            checkBlockMetrics(initMetrics, igfs.metrics(), 4, 1, blockSize * 3 + rmtBlockSize, 3,
                3, blockSize * 3);

            // Lets wait for blocks will be placed to cache
            U.sleep(300);

//            // Read remote file again.
//            is = igfs.open(fileRemote);
//            is.readChunks(0, rmtBlockSize);
//            is.close();

            checkBlockMetrics(initMetrics, igfs.metrics(), 5, 1, blockSize * 3 + rmtBlockSize * 2, 3, 3, blockSize * 3);

            metrics = igfs.metrics();

            Assert.assertEquals(rmtBlockSize, metrics.secondarySpaceSize());

            // Write some data to the file working in DUAL mode.
            os = igfs.append(fileRemote, false);
            os.write(new byte[rmtBlockSize]);
            os.close();

            // Additional block read here due to file ending synchronization.
            checkBlockMetrics(initMetrics, igfs.metrics(), 5, 1, blockSize * 3 + rmtBlockSize * 2, 4, 4,
                blockSize * 3 + rmtBlockSize);

            metrics = igfs.metrics();

            assert metrics.secondarySpaceSize() == rmtBlockSize * 2;

            igfs.delete(fileRemote, false);

            U.sleep(300);

            assert igfs.metrics().secondarySpaceSize() == 0;

            // Write partial block to the first file.
            os = igfs.append(file1, false);
            os.write(new byte[blockSize / 2]);
            os.close();

            checkBlockMetrics(initMetrics, igfs.metrics(), 5, 1, blockSize * 3 + rmtBlockSize * 2, 5, 5,
                blockSize * 7 / 2 + rmtBlockSize);

//            // Now read partial block.
//            // Read remote file again.
//            is = igfs.open(file1);
//            is.seek(blockSize * 2);
//            is.readChunks(0, blockSize / 2);
//            is.close();

            checkBlockMetrics(initMetrics, igfs.metrics(), 6, 1, blockSize * 7 / 2 + rmtBlockSize * 2, 5, 5,
                blockSize * 7 / 2 + rmtBlockSize);
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