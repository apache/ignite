package org.apache.ignite.internal.processors.compress;

import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import junit.framework.TestCase;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Assume;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.compress.CompressionProcessorImpl.allocateDirectBuffer;
import static org.apache.ignite.internal.processors.compress.FileSystemUtils.getFileSystemBlockSize;
import static org.apache.ignite.internal.processors.compress.FileSystemUtils.punchHole;

public class FileSystemUtilsTest extends TestCase {
    /** */
    private static POSIX posix = POSIXFactory.getPOSIX();

    /**
     * !!! May produce wrong results on some file systems:
     * Ext4 and Btrfs are known to work correctly, but XFS is broken.
     *
     * @param file File path.
     * @return Sparse size.
     */
    private static long getSparseFileSize(Path file) {
        if (U.isLinux()) {
            try {
                Process proc = new ProcessBuilder("stat", file.toString())
                    .inheritIO()
                    .start();

                proc.waitFor();
            }
            catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        return posix.stat(file.toString()).blocks() * 512;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSparseFiles() throws Exception {
        Assume.assumeTrue("Native file system API must be supported for " +
                U.getOsMx().getName() + " " + U.getOsMx().getVersion() + " " + U.getOsMx().getArch(),
            FileSystemUtils.isSupported());

        Path file = Files.createTempFile("test_sparse_file_", ".bin");

        try {
            doTestSparseFiles(file);
        }
        finally {
            Files.delete(file);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFileSystems() throws Exception {
        doTestSparseFiles(Paths.get("/ext4/test_file")); // OK
        doTestSparseFiles(Paths.get("/btrfs/test_file")); // OK
        doTestSparseFiles(Paths.get("/xfs/test_file")); // Fails due to getSparseFileSize instability
    }

    private static int getFD(FileChannel ch) throws IgniteCheckedException {
        return U.<Integer>field(U.<FileDescriptor>field(ch, "fd"), "fd");
    }

    /**
     * @param file File path.
     * @throws Exception If failed.
     */
    private void doTestSparseFiles(Path file) throws Exception {
        System.out.println(file);

        FileChannel ch = FileChannel.open(file,
            READ, WRITE, TRUNCATE_EXISTING, SPARSE, SYNC);
        int fd = getFD(ch);

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

        for (int i = 0; i < blocks - 1; i++) {
            punchHole(fd, fsBlockSize * i, fsBlockSize, fsBlockSize);
            getSparseFileSize(file);
        }
    }

}
