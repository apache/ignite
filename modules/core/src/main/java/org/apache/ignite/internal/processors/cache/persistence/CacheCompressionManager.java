package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.PageCompression;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;

import static org.apache.ignite.internal.processors.compress.CompressionProcessor.checkCompressionLevelBounds;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.getDefaultCompressionLevel;

public class CacheCompressionManager extends GridCacheManagerAdapter {
    /** */
    private PageCompression pageCompression;

    /** */
    private int pageCompressLevel;

    /** */
    private CompressionProcessor compressProc;

    /** */
    @Override protected void start0() throws IgniteCheckedException {
        DataRegionConfiguration cfg = cctx.dataRegion().config();

        pageCompression = cfg.getPageCompression();

        if (pageCompression != null) {
            if (!cfg.isPersistenceEnabled())
                throw new IgniteCheckedException("Page compression makes sense only with enabled persistence.");

            Integer lvl = cfg.getPageCompressionLevel();
            pageCompressLevel = lvl != null ?
                checkCompressionLevelBounds(lvl, pageCompression) :
                getDefaultCompressionLevel(pageCompression);

            compressProc = cctx.kernalContext().compress();
        }
    }

    /**
     * @param page Page buffer.
     * @param store Page store.
     * @return Compressed or the same buffer.
     * @throws IgniteCheckedException If failed.
     */
    public ByteBuffer compressPage(ByteBuffer page, PageStore store) throws IgniteCheckedException {
        if (compressProc == null)
            return page;

        int blockSize = store.getBlockSize();

        if (blockSize <= 0) {
            throw new IgniteCheckedException("Failed to detect file system block size." +
                " Page compression is unsupported on this file system.");
        }

        return compressProc.compressPage(page, blockSize, pageCompression, pageCompressLevel);
    }

    /**
     * @param page Page buffer.
     * @throws IgniteCheckedException If failed.
     */
    public void decompressPage(ByteBuffer page) throws IgniteCheckedException {
        if (compressProc != null)
            compressProc.decompressPage(page);
    }
}
