package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.PageCompression;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;

public class CacheCompressionManager extends GridCacheManagerAdapter {
    /**
     * @return Page compression for this cache.
     */
    public PageCompression getPageCompression() {
        return cctx.dataRegion().config().getPageCompression();
    }

    /**
     * @param pageId Page id.
     * @param page Page buffer.
     * @param store Page store.
     * @param compression The compression to apply to the page.
     * @return Compressed or the same buffer.
     * @throws IgniteCheckedException If failed.
     */
    public ByteBuffer compressPage(long pageId, ByteBuffer page, PageStore store, PageCompression compression) throws IgniteCheckedException {
        assert compression != null;

        int blockSize = store.getBlockSize();

        if (blockSize <= 0)
            throw new IgniteCheckedException("Failed to detect file system block size. Page compression is unsupported on this file system.");

        return cctx.kernalContext().compress().compressPage(pageId, page, blockSize, compression);
    }

    /**
     * @param page Page buffer.
     * @throws IgniteCheckedException If failed.
     */
    public void decompressPage(ByteBuffer page) throws IgniteCheckedException {
        cctx.kernalContext().compress().decompressPage(page);
    }
}
