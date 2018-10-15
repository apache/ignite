package org.apache.ignite.internal.processors.compress;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;

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

    public ByteBuffer compressPage(long pageId, ByteBuffer page) {
        return page;
    }

    public void decompressPage(ByteBuffer page) {
        // No-op.
    }
}
