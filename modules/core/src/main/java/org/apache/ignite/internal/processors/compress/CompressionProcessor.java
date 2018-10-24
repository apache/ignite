package org.apache.ignite.internal.processors.compress;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.PageCompression;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

public class CompressionProcessor extends GridProcessorAdapter {
    /** */
    public static final byte UNCOMPRESSED_PAGE = 0;

    /** */
    public static final byte COMPACTED_PAGE = 1;

    /** */
    public static final byte ZSTD_COMPRESSED_PAGE = 2;

    /**
     * @param ctx Kernal context.
     */
    public CompressionProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    public ByteBuffer compressPage(long pageId, ByteBuffer page, int fsBlockSize, PageCompression compression)
        throws IgniteCheckedException {
        throw new IgniteException("Page compression failed: make sure that ignite-compression module is in classpath.");
    }

    public void decompressPage(ByteBuffer page) throws IgniteCheckedException {
        if (PageIO.getCompressionType(page) != UNCOMPRESSED_PAGE)
            throw new IgniteException("Page decompression failed: make sure that ignite-compression module is in classpath.");
    }
}
