package org.apache.ignite.internal.processors.compress;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
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

    /**
     * @param compression Compression algorithm.
     * @return Default compression level.
     */
    public static int getDefaultCompressionLevel(PageCompression compression) {
        switch (compression) {
            case ZSTD:
                return 3;

            case DROP_GARBAGE:
                return 0;
        }

        throw new IllegalArgumentException("Compression: " + compression);
    }

    /**
     * @param compressLevel Compression level.
     * @param compression Compression algorithm.
     * @return Compression level.
     */
    public static int checkCompressionLevelBounds(int compressLevel, PageCompression compression) {
        switch (compression) {
            case ZSTD:
                if (compressLevel < -3 || compressLevel > 22) {
                    throw new IllegalArgumentException("Compression level for " + compression +
                        " must be between -3 and 22." );
                }

                return compressLevel;

            case DROP_GARBAGE:
                return 0;
        }

        throw new IllegalArgumentException("Compression: " + compression);
    }

    /**
     * @param page Page buffer.
     * @param storeBlockSize Store block size.
     * @param compression Compression algorithm.
     * @param compressLevel Compression level.
     * @return Possibly compressed buffer.
     * @throws IgniteCheckedException If failed.
     */
    public ByteBuffer compressPage(
        ByteBuffer page,
        int storeBlockSize,
        PageCompression compression,
        int compressLevel
    ) throws IgniteCheckedException {
        throw new IgniteCheckedException("Page compression failed: make sure that ignite-compression module is in classpath.");
    }

    /**
     * @param page Possibly compressed page buffer.
     * @throws IgniteCheckedException If failed.
     */
    public void decompressPage(ByteBuffer page) throws IgniteCheckedException {
        if (PageIO.getCompressionType(page) != UNCOMPRESSED_PAGE)
            throw new IgniteCheckedException("Page decompression failed: make sure that ignite-compression module is in classpath.");
    }
}
