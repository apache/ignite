package org.apache.ignite.internal.processors.compress;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

public class CompressProcessor extends GridProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    public CompressProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    public boolean isPageCompressionEnabled() {
        return false;
    }

    public long compressPage(PageIO io, long pageAddr, int pageSize) {
        throw new UnsupportedOperationException();
    }

    public long uncompressPage(long inAddr, long outAddr) {
        throw new UnsupportedOperationException();
    }
}
