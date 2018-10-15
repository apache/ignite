package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;

public class CacheCompressionManager extends GridCacheManagerAdapter {

    public boolean isPageCompressionEnabled() {
        return cctx.kernalContext().compress().isPageCompressionEnabled(); // TODO config
    }

    public ByteBuffer compressPage(long pageId, ByteBuffer buf) {
        return cctx.kernalContext().compress().compressPage(pageId, buf);
    }

    public void decompressPage(ByteBuffer page) {
        cctx.kernalContext().compress().decompressPage(page);
    }
}
