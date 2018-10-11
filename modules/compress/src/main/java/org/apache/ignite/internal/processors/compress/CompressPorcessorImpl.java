package org.apache.ignite.internal.processors.compress;

import com.github.luben.zstd.Zstd;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;

public class CompressPorcessorImpl extends CompressProcessor {
    /** */
    private final ThreadLocal<Buf> buf = new ThreadLocal<>();

    /** */
    private int compressLvl = 3;

    /**
     * @param ctx Kernal context.
     */
    public CompressPorcessorImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean isPageCompressionEnabled() {
        return true; // TODO
    }

    /** {@inheritDoc} */
    @Override public long compressPage(PageIO io, long pageAddr, int pageSize) {
        int destSize = pageSize * 3;
        Buf dest = buf.get();

        if (dest != null && dest.size < destSize) {
            GridUnsafe.freeMemory(dest.addr);
            dest = null;
        }

        if (dest == null)
            buf.set(dest = new Buf(GridUnsafe.allocateMemory(destSize), destSize));

        // TODO drop garbage

        long size = Zstd.compressUnsafe(dest.addr, destSize, dest.addr, pageSize, compressLvl);

        if (Zstd.isError(size))
            throw new IllegalStateException(Zstd.getErrorName(size));

        return dest.addr;
    }

    /** {@inheritDoc} */
    @Override public long uncompressPage(long inAddr, long outAddr) {
        return inAddr;
    }

    static final class Buf {
        long addr;
        int size;

        Buf(long addr, int size) {
            this.addr = addr;
            this.size = size;
        }
    }
}
