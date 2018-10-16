package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;

public class CacheCompressionManager extends GridCacheManagerAdapter {

    private boolean isPageCompressionEnabled() {
        return cctx.kernalContext().compress().isPageCompressionEnabled(); // TODO config
    }

    public ByteBuffer compressPage(long pageId, ByteBuffer page, Path file) throws IgniteCheckedException {
        if (file != null && isPageCompressionEnabled())
            return cctx.kernalContext().compress().compressPage(pageId, page, file);

        return page;
    }

    public void decompressPage(ByteBuffer page) throws IgniteCheckedException {
        if (isPageCompressionEnabled())
            cctx.kernalContext().compress().decompressPage(page);
    }
}
