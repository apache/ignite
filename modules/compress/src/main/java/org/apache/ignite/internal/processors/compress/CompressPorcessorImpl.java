package org.apache.ignite.internal.processors.compress;

import com.github.luben.zstd.Zstd;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.GridKernalContext;

public class CompressPorcessorImpl extends CompressProcessor {
    /**
     * @param ctx Kernal context.
     */
    public CompressPorcessorImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer compressPage(long pageId, ByteBuffer page) {
        assert page.position() == 0;
        assert page.limit() == page.capacity();

        ByteBuffer dst = ByteBuffer.allocateDirect(page.capacity() * 3);

        // TODO drop garbage

        long compressedSize = Zstd.compress(dst, toDirect(page));

        dst.rewind();
        page.clear();

        if (Zstd.isError(compressedSize))
            throw new IllegalStateException(Zstd.getErrorName(compressedSize));

        return dst;
    }

    private ByteBuffer toDirect(ByteBuffer page) {
        if (page.isDirect())
            return page;

        ByteBuffer res = ByteBuffer.allocateDirect(page.capacity());
        res.put(page);
        return res;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer uncompressPage(ByteBuffer compressedPage, int pageSize) {
        assert compressedPage.position() == 0;
        assert compressedPage.limit() == compressedPage.capacity();

        ByteBuffer dst = ByteBuffer.allocateDirect(pageSize);

        Zstd.decompress(dst, toDirect(compressedPage));

        // TODO resurrect garbage

        return compressedPage;
    }
}
