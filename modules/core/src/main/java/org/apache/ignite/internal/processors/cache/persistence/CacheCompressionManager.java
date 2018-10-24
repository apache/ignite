package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.PageCompression;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;

public class CacheCompressionManager extends GridCacheManagerAdapter {
    /** */
    private PageCompression pageCompression;

    /** */
    private int pageCompressLevel;

    /** */
    private CompressionProcessor compressProc;

    /** */
    @Override protected void start0() throws IgniteCheckedException {
        compressProc = cctx.kernalContext().compress();

        DataRegionConfiguration cfg = cctx.dataRegion().config();

        pageCompression = cfg.getPageCompression();
        Integer lvl = cfg.getPageCompressionLevel();
        pageCompressLevel = lvl != null ? lvl :
            CompressionProcessor.getDefaultCompressionLevel(pageCompression);

        if (pageCompression != null && !cfg.isPersistenceEnabled())
            throw new IgniteCheckedException("Page compression makes sense only with enabled persistence.");
    }

    /**
     * @param pageId Page id.
     * @param page Page buffer.
     * @param store Page store.
     * @return Compressed or the same buffer.
     * @throws IgniteCheckedException If failed.
     */
    public ByteBuffer compressPage(long pageId, ByteBuffer page, PageStore store) throws IgniteCheckedException {
        if (pageCompression == null)
            return page;

        int blockSize = store.getBlockSize();

        if (blockSize <= 0)
            throw new IgniteCheckedException("Failed to detect file system block size. Page compression is unsupported on this file system.");

        return compressProc.compressPage(pageId, page, blockSize, pageCompression, pageCompressLevel);
    }

    /**
     * @param page Page buffer.
     * @throws IgniteCheckedException If failed.
     */
    public void decompressPage(ByteBuffer page) throws IgniteCheckedException {
        compressProc.decompressPage(page);
    }
}
