package org.apache.ignite.internal.processors.compress;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;

public class CompressProcessor extends GridProcessorAdapter {
    /** */
    public static final byte UNCOMPRESSED_PAGE = 0;

    /** */
    public static final byte COMPACTED_PAGE = 1;

    /** */
    public static final byte ZSTD_3_COMPRESSED_PAGE = 2;

    /**
     * @param ctx Kernal context.
     */
    public CompressProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    public boolean isPageCompressionEnabled() {
        return false;
    }

    public ByteBuffer compressPage(long pageId, ByteBuffer page, Path file) throws IgniteCheckedException {
        return page;
    }

    public void decompressPage(ByteBuffer page) throws IgniteCheckedException {
        // No-op.
    }
}
