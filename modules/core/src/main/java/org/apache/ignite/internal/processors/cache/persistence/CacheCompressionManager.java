package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;

public class CacheCompressionManager extends GridCacheManagerAdapter {

    private boolean isPageCompressionEnabled() {
        return cctx.kernalContext().compress().isPageCompressionEnabled(); // TODO config
    }

    public ByteBuffer compressPage(long pageId, ByteBuffer page) {
        if (isPageCompressionEnabled())
            return cctx.kernalContext().compress().compressPage(pageId, page);

        return page;
    }

    public void decompressPage(ByteBuffer page) {
        if (isPageCompressionEnabled())
            cctx.kernalContext().compress().decompressPage(page);
    }
}
