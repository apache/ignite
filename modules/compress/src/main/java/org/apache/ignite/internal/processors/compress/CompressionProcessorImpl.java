package org.apache.ignite.internal.processors.compress;

import com.github.luben.zstd.Zstd;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.PageCompression;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CompactablePageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

import static org.apache.ignite.configuration.PageCompression.DROP_GARBAGE;
import static org.apache.ignite.internal.util.GridUnsafe.NATIVE_BYTE_ORDER;

/**
 * Compression processor.
 */
@SuppressWarnings("unused")
public class CompressionProcessorImpl extends CompressionProcessor {
    /** */
    private final ThreadLocal<ByteBuffer> tmp = new ThreadLocal<ByteBuffer>() {
        @Override protected ByteBuffer initialValue() {
            return allocateDirectBuffer(32 * 1024);
        }

        @Override public ByteBuffer get() {
            ByteBuffer buf = super.get();
            buf.clear();
            return buf;
        }
    };

    /**
     * @param ctx Kernal context.
     */
    public CompressionProcessorImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param cap Capacity.
     * @return Direct byte buffer.
     */
    static ByteBuffer allocateDirectBuffer(int cap) {
        return ByteBuffer.allocateDirect(cap).order(NATIVE_BYTE_ORDER);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer compressPage(
        ByteBuffer page,
        int fsBlockSize,
        PageCompression compression,
        int compressLevel
    ) throws IgniteCheckedException {
        assert compression != null;

        PageIO io = PageIO.getPageIO(page);

        if (!(io instanceof CompactablePageIO))
            return page;

        int pageSize = page.remaining();

        if (pageSize < fsBlockSize * 2 || pageSize % fsBlockSize != 0) // TODO check
            return page; // Makes no sense to compress the page, we will not free any disk space.

        ByteBuffer compact = tmp.get();

        try {
            ((CompactablePageIO)io).compactPage(page, compact);
        }
        finally {
            page.clear();
        }

        int compactedSize = compact.limit();

        if (compactedSize < fsBlockSize || compression == DROP_GARBAGE) {
            // No need to compress further.
            PageIO.setCompressionType(compact, COMPACTED_PAGE);
            PageIO.setCompressedSize(compact, (short)compactedSize);

            // Can not return thread local buffer, because the actual write may be async.
            return (ByteBuffer)allocateDirectBuffer(compactedSize).put(compact).flip();
        }

        ByteBuffer compressed = allocateDirectBuffer((int)(PageIO.COMMON_HEADER_END +
            Zstd.compressBound(compactedSize - PageIO.COMMON_HEADER_END)));

        compressed.put((ByteBuffer)compact.limit(PageIO.COMMON_HEADER_END));
        Zstd.compress(compressed, (ByteBuffer)compact.limit(compactedSize), compressLevel);

        compressed.flip();

        int compressedSize = compressed.limit();

        if (pageSize - compressedSize < fsBlockSize)
            return page; // Were not able to release file blocks.

        PageIO.setCompressionType(compressed, getCompressionType(compression));
        PageIO.setCompressedSize(compressed, (short)compressedSize);

        return compressed;
    }

    /**
     * @param compression Compression.
     * @return Level.
     */
    private static byte getCompressionType(PageCompression compression) {
        switch (compression) {
            case ZSTD:
                return ZSTD_COMPRESSED_PAGE;

            default:
                throw new IllegalStateException("Unexpected compression: " + compression);
        }
    }

    /** {@inheritDoc} */
    @Override public void decompressPage(ByteBuffer page) throws IgniteCheckedException {
        final int pageSize = page.capacity();

        byte compressType = PageIO.getCompressionType(page);
        short compressSize = PageIO.getCompressedSize(page);

        switch (compressType) {
            case UNCOMPRESSED_PAGE:
                return;

            case COMPACTED_PAGE:
                break;

            case ZSTD_COMPRESSED_PAGE:
                assert page.isDirect();

                ByteBuffer dst = tmp.get();

                page.position(PageIO.COMMON_HEADER_END).limit(compressSize);
                Zstd.decompress(dst, page);
                dst.flip();

                page.position(PageIO.COMMON_HEADER_END).limit(pageSize);
                page.put(dst).position(0);

                break;

            default:
                assert false: compressType;
        }

        CompactablePageIO io = PageIO.getPageIO(page);

        io.restorePage(page, pageSize);

        PageIO.setCompressionType(page, (byte)0);
        PageIO.setCompressedSize(page, (short)0);
    }
}
