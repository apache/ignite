package org.apache.ignite.internal.processors.compress;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import junit.framework.TestCase;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Assume;

import static org.apache.ignite.internal.processors.compress.CompressionProcessorImpl.allocateDirectBuffer;
import static org.apache.ignite.internal.processors.compress.FileSystemUtils.getFileSystemBlockSize;
import static org.apache.ignite.internal.processors.compress.FileSystemUtils.getSparseFileSize;

public class FileSystemUtilsTest extends TestCase {

    public void testSparseFiles() throws IOException {
        Assume.assumeTrue("Native file system API must be supported for " +
            U.getOsMx().getName() + " " + U.getOsMx().getVersion() + " " + U.getOsMx().getArch(),
            FileSystemUtils.isSupported());

        Random rnd = ThreadLocalRandom.current();

        Path file = Files.createTempFile("test_sparse_file_", ".bin");

        RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rws");

        int fd = U.field(raf.getFD(), "fd");
        assertTrue(fd > 0);

        FileChannel ch = raf.getChannel();

        int fsBlockSize = getFileSystemBlockSize(file);
        assertTrue(fsBlockSize > 0);

        int pageSize = fsBlockSize * 4;

        ByteBuffer page = allocateDirectBuffer(pageSize);

        while (page.remaining() > 0)
            page.putLong(0xABCDEF7654321EADL);
        page.flip();

        int pages = 17;

        for (int i = 0; i < pages; i++) {
            ch.write(page, i * pageSize);
            assertEquals(0, page.remaining());
            page.flip();
        }

        assertEquals(pages * pageSize, ch.size());
        assertEquals(pages * pageSize, getSparseFileSize(file));

//        long hole = FileSystemUtils.punchHole(fd, , , fsBlockSize);
    }

}
