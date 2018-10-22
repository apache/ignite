package org.apache.ignite.internal.processors.compress;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.function.Function;
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
    private static final String GET_FS_BLOCK_SIZE_LINUX_CLASS =
        "org.apache.ignite.internal.processors.compress.GetFileSystemBlockSizeLinux";

    /** */
    private static final Function<Path, Integer> getFsBlockSize;

    /** */
    static {
        try {
            getFsBlockSize = !IgniteComponentType.COMPRESSION.inClassPath() || !U.isLinux() ? null :
                U.newInstance(GET_FS_BLOCK_SIZE_LINUX_CLASS);
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
        return getFsBlockSize == null ? -1 : getFsBlockSize.apply(file);
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
