package org.apache.ignite.internal.processors.compress;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

public class CompressionProcessor extends GridProcessorAdapter {
    /** */
    public static final byte UNCOMPRESSED_PAGE = 0;

    /** */
    public static final byte COMPACTED_PAGE = 1;

    /** */
    public static final byte ZSTD_3_COMPRESSED_PAGE = 2;

    /** */
    private static final String NATIVE_FS_LINUX_CLASS =
        "org.apache.ignite.internal.processors.compress.NativeFileSystemLinux";

    /** */
    private static final NativeFileSystem fs;

    /** */
    static {
        try {
            NativeFileSystem x = null;

            if (IgniteComponentType.COMPRESSION.inClassPath()) {
                if (U.isLinux())
                    x = U.newInstance(NATIVE_FS_LINUX_CLASS);
            }

            fs = x;
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param ctx Kernal context.
     */
    public CompressionProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    public static int getFsBlockSize(Path file) {
        assert file != null;

        return fs == null ? -1 : fs.getFileBlockSize(file);
    }

    public static int punchHole(int fd, long off, int len) {
        return fs == null ? -1 : fs.punchHole(fd, off, len);
    }

    public boolean isPageCompressionEnabled() {
        return false;
    }

    public ByteBuffer compressPage(long pageId, ByteBuffer page, int fsBlockSize) throws IgniteCheckedException {
        return page;
    }

    public void decompressPage(ByteBuffer page) throws IgniteCheckedException {
        // No-op.
    }
}
