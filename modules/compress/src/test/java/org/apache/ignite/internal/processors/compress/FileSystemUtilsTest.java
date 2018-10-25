package org.apache.ignite.internal.processors.compress;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import junit.framework.TestCase;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Assume;

import static org.apache.ignite.internal.processors.compress.CompressionProcessorImpl.allocateDirectBuffer;
import static org.apache.ignite.internal.processors.compress.FileSystemUtils.getFileSystemBlockSize;
import static org.apache.ignite.internal.processors.compress.FileSystemUtils.getSparseFileSize;
import static org.apache.ignite.internal.processors.compress.FileSystemUtils.punchHole;

public class FileSystemUtilsTest extends TestCase {

    public void testSparseFiles() throws IOException {
        Assume.assumeTrue("Native file system API must be supported for " +
            U.getOsMx().getName() + " " + U.getOsMx().getVersion() + " " + U.getOsMx().getArch(),
            FileSystemUtils.isSupported());

        Path file = Files.createTempFile("test_sparse_file_", ".bin");

        RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rws");

        int fd = U.field(raf.getFD(), "fd");
        assertTrue(fd > 0);

        FileChannel ch = raf.getChannel();

        int fsBlockSize = getFileSystemBlockSize(file);
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

        assertEquals(fileSize, ch.size());
        assertEquals(fileSize, getSparseFileSize(file));

        int off = fsBlockSize * 3 - (fsBlockSize >>> 2);
        int len = fsBlockSize;
        assertEquals(0, punchHole(fd, off, len, fsBlockSize));
        assertEquals(fileSize, getSparseFileSize(file));

        off = 2 * fsBlockSize - 3;
        len = 2 * fsBlockSize + 3;
        assertEquals(2 * fsBlockSize, punchHole(fd, off, len, fsBlockSize));
        assertEquals(sparseSize -= 2 * fsBlockSize, getSparseFileSize(file));

        off = 10 * fsBlockSize;
        len = 3 * fsBlockSize + 5;
        assertEquals(3 * fsBlockSize, punchHole(fd, off, len, fsBlockSize));
        assertEquals(sparseSize -= 3 * fsBlockSize, getSparseFileSize(file));

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
        assertEquals(sparseSize -= fsBlockSize, getSparseFileSize(file));
    }

}
