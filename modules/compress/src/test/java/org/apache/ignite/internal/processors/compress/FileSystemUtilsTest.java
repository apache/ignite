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

package org.apache.ignite.internal.processors.compress;

import java.io.FileDescriptor;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.compress.CompressionProcessorImpl.allocateDirectBuffer;
import static org.apache.ignite.internal.processors.compress.FileSystemUtils.getFileSystemBlockSize;
import static org.apache.ignite.internal.processors.compress.FileSystemUtils.getSparseFileSize;
import static org.apache.ignite.internal.processors.compress.FileSystemUtils.punchHole;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 */
public class FileSystemUtilsTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSparseFiles() throws Exception {
        if (!U.isLinux())
            return;

        Path file = Files.createTempFile("test_sparse_file_", ".bin");

        try {
            doTestSparseFiles(file, false); // Ext4 expected as default FS.
        }
        finally {
            Files.delete(file);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testFileSystems() throws Exception {
        doTestSparseFiles(Paths.get("/ext4/test_file"), false);
        doTestSparseFiles(Paths.get("/btrfs/test_file"), false);
        doTestSparseFiles(Paths.get("/xfs/test_file"), true);
    }

    private static int getFD(FileChannel ch) throws IgniteCheckedException {
        return U.<Integer>field(U.<FileDescriptor>field(ch, "fd"), "fd");
    }

    /**
     * @param file File path.
     * @param reopen Reopen file after each hole punch. XFS needs it.
     * @throws Exception If failed.
     */
    private void doTestSparseFiles(Path file, boolean reopen) throws Exception {
        System.out.println(file);

        FileChannel ch = FileChannel.open(file,
            READ, WRITE, TRUNCATE_EXISTING);

        try {
            int fd = getFD(ch);

            int fsBlockSize = getFileSystemBlockSize(fd);

            System.out.println("fsBlockSize: " + fsBlockSize);

            assertTrue(fsBlockSize > 0);

            int pageSize = fsBlockSize * 4;

            ByteBuffer page = allocateDirectBuffer(pageSize);

            while (page.remaining() > 0)
                page.putLong(0xABCDEF7654321EADL);
            page.flip();

            int pages = 5;
            int blocks = pages * pageSize / fsBlockSize;
            int fileSize = pages * pageSize;
            int sparseSize = fileSize;

            for (int i = 0; i < pages; i++) {
                ch.write(page, i * pageSize);
                assertEquals(0, page.remaining());
                page.flip();
            }

            if (reopen) {
                ch.force(true);
                ch.close();
                ch = FileChannel.open(file, READ, WRITE);
                fd = getFD(ch);
            }

            assertEquals(fileSize, ch.size());
            assertEquals(fileSize, getSparseFileSize(fd));

            int off = fsBlockSize * 3 - (fsBlockSize >>> 2);
            int len = fsBlockSize;
            assertEquals(0, punchHole(fd, off, len, fsBlockSize));
            if (reopen) {
                ch.force(true);
                ch.close();
                ch = FileChannel.open(file, READ, WRITE);
                fd = getFD(ch);
            }
            assertEquals(fileSize, getSparseFileSize(fd));

            off = 2 * fsBlockSize - 3;
            len = 2 * fsBlockSize + 3;
            assertEquals(2 * fsBlockSize, punchHole(fd, off, len, fsBlockSize));
            if (reopen) {
                ch.force(true);
                ch.close();
                ch = FileChannel.open(file, READ, WRITE);
                fd = getFD(ch);
            }
            assertEquals(sparseSize -= 2 * fsBlockSize, getSparseFileSize(fd));

            off = 10 * fsBlockSize;
            len = 3 * fsBlockSize + 5;
            assertEquals(3 * fsBlockSize, punchHole(fd, off, len, fsBlockSize));
            if (reopen) {
                ch.force(true);
                ch.close();
                ch = FileChannel.open(file, READ, WRITE);
                fd = getFD(ch);
            }
            assertEquals(sparseSize -= 3 * fsBlockSize, getSparseFileSize(fd));

            off = 15 * fsBlockSize + 1;
            len = fsBlockSize;
            assertEquals(0, punchHole(fd, off, len, fsBlockSize));

            off = 15 * fsBlockSize - 1;
            len = fsBlockSize;
            assertEquals(0, punchHole(fd, off, len, fsBlockSize));

            off = 15 * fsBlockSize;
            len = fsBlockSize - 1;
            assertEquals(0, punchHole(fd, off, len, fsBlockSize));

            off = 15 * fsBlockSize;
            len = fsBlockSize;
            assertEquals(fsBlockSize, punchHole(fd, off, len, fsBlockSize));
            if (reopen) {
                ch.force(true);
                ch.close();
                ch = FileChannel.open(file, READ, WRITE);
                fd = getFD(ch);
            }
            assertEquals(sparseSize -= fsBlockSize, getSparseFileSize(fd));

            for (int i = 0; i < blocks - 1; i++)
                punchHole(fd, fsBlockSize * i, fsBlockSize, fsBlockSize);

            if (reopen) {
                ch.force(true);
                ch.close();
                ch = FileChannel.open(file, READ, WRITE);
                fd = getFD(ch);
            }

            assertEquals(fsBlockSize, getSparseFileSize(fd));
        }
        finally {
            ch.close();
        }
    }

}
