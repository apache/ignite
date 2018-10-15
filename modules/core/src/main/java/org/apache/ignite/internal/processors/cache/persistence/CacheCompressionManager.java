package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;

public class CacheCompressionManager extends GridCacheManagerAdapter {

    public boolean isPageCompressionEnabled() {
        return true;
    }

    public ByteBuffer compressPage(long pageId, ByteBuffer buf) {
        return cctx.kernalContext().compress().compressPage(pageId, buf);
    }
}
