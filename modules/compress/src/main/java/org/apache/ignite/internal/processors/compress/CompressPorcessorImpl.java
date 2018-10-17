package org.apache.ignite.internal.processors.compress;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CompactablePageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

public class CompressPorcessorImpl extends CompressProcessor {
    /** */
    private final ThreadLocal<ByteBuffer> tmp = new ThreadLocal<ByteBuffer>() {
        @Override protected ByteBuffer initialValue() {
            return ByteBuffer.allocateDirect(64 * 1024);
        }

        @Override public ByteBuffer get() {
            ByteBuffer buf = super.get();
            buf.clear();
            return buf;
        }
    };

    /** */
    private final ConcurrentHashMap<Path, Integer> fsBlockSizeCache = new ConcurrentHashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public CompressPorcessorImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean isPageCompressionEnabled() {
        return true;
    }

    private int getFileSystemBlockSize(Path file) throws IgniteCheckedException {
        Path root;

        try {
            root = file.toRealPath().getRoot();
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        Integer blockSize = fsBlockSizeCache.get(root);

        if (blockSize == null)
            fsBlockSizeCache.putIfAbsent(root, blockSize = LinuxFileSystemLibrary.getFileSystemBlockSize(root));

        return blockSize;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer compressPage(long pageId, ByteBuffer page, Path file) throws IgniteCheckedException {
        PageIO io = PageIO.getPageIO(page);

        if (!(io instanceof CompactablePageIO))
            return page;

        int pageSize = page.remaining();
        int fsBlockSize = getFileSystemBlockSize(file);

        if (pageSize < fsBlockSize * 2) // TODO assume page and block alignment??
            return page; // Makes no sense to compress the page, we will not free any disk space.

        ByteBuffer compact = tmp.get();

        int limit = page.limit();
        page.mark();
        try {
            ((CompactablePageIO)io).compactPage(page, compact);
        }
        finally {
            page.reset();
            page.limit(limit);
        }

        int compactedSize = compact.limit();

        if (compactedSize < fsBlockSize) {
            // No need to compress further.
            PageIO.setCompressionType(compact, COMPACTED_PAGE);
            PageIO.setCompressedSize(compact, (short)compactedSize);

            // Can not return thread local buffer, because the actual write may be async.
            return (ByteBuffer)ByteBuffer.allocateDirect(compactedSize).put(compact).flip();
        }

        ByteBuffer compressed = ByteBuffer.allocateDirect((int)(PageIO.COMMON_HEADER_END +
            Zstd.compressBound(compactedSize - PageIO.COMMON_HEADER_END)));

        compressed.put((ByteBuffer)compact.limit(PageIO.COMMON_HEADER_END));
        Zstd.compress(compressed, (ByteBuffer)compact.limit(compactedSize), 3);

        compressed.flip();

        int compressedSize = compressed.limit();

        if (pageSize - compressedSize < fsBlockSize) // TODO assume page and block alignment??
            return page; // Were not able to release file blocks.

        PageIO.setCompressionType(compressed, ZSTD_3_COMPRESSED_PAGE);
        PageIO.setCompressedSize(compressed, (short)compressedSize);

        return compressed;
    }

    /** {@inheritDoc} */
    @Override public void decompressPage(ByteBuffer page) throws IgniteCheckedException {
        assert page.isDirect();

        final int pos = page.position();
        final int pageSize = page.remaining();

        byte compressType = PageIO.getCompressionType(page);
        short compressSize = PageIO.getCompressedSize(page);

        switch (compressType) {
            case UNCOMPRESSED_PAGE:
                return;

            case ZSTD_3_COMPRESSED_PAGE:
                ByteBuffer dst = tmp.get();

                Zstd.decompress(dst, (ByteBuffer)page
                    .position(pos + PageIO.COMMON_HEADER_END)
                    .limit(pos + compressSize));

                page.position(pos + PageIO.COMMON_HEADER_END)
                    .limit(pos + pageSize);
                page.put((ByteBuffer)dst.flip())
                    .position(pos);

                break;

            default:
                assert false: compressType;
        }

        CompactablePageIO io = PageIO.getPageIO(page);

        io.restorePage(page, pageSize);
    }
}
