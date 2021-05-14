package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.AbstractDataPageIterator;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IncompleteObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.toDetailString;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getType;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;

/**
 * Ves pokrit assertami absolutely ves,
 * PageScan iterator in the ignite core est.
 */
public class FileDataPageIterator extends AbstractDataPageIterator {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Page store to iterate over. */
    @GridToStringExclude
    private final PageStore store;

    /** Buffer to read pages. */
    private final ByteBuffer locBuff;

    /** Buffer to read the rest part of fragmented rows. */
    private final ByteBuffer fragmentBuff;

    /**
     * During scanning a cache partition presented as {@code PageStore} we must guarantee the following:
     * all the pages of this storage remains unchanged during the Iterator remains opened, the stored data
     * keeps its consistency. We can't read the {@code PageStore} during an ongoing checkpoint over it.
     *
     * @param coctx Cache object context.
     * @param store Page store to read.
     * @param partId Partition id.
     */
    public FileDataPageIterator(
        GridCacheSharedContext<?, ?> sctx,
        CacheObjectContext coctx,
        PageStore store,
        int partId
    ) {
        super(sctx, coctx, CacheDataRowAdapter.RowData.FULL, false, false, store.pages(),
            store.getPageSize(), partId);

        this.store = store;

        locBuff = ByteBuffer.allocateDirect(store.getPageSize())
            .order(ByteOrder.nativeOrder());
        fragmentBuff = ByteBuffer.allocateDirect(store.getPageSize())
            .order(ByteOrder.nativeOrder());
    }

    /** {@inheritDoc} */
    @Override public boolean readHeaderPage(long pageId, LongConsumer reader) throws IgniteCheckedException {
        if (!readPageFromStore(pageId, locBuff))
            return false;

        reader.accept(bufferAddress(locBuff));

        return true;
    }

    /** {@inheritDoc} */
    @Override public IncompleteObject<?> readFragmentPage(
        long pageId,
        LongFunction<IncompleteObject<?>> reader
    ) throws IgniteCheckedException {
        boolean success = readPageFromStore(pageId, fragmentBuff);

        assert success : "Only FLAG_DATA pages allowed: " + toDetailString(pageId);

        return reader.apply(GridUnsafe.bufferAddress(fragmentBuff));
    }

    /**
     * @param pageId Page id to read from store.
     * @param buff Buffer to read page into.
     * @return {@code true} if page read with given type flag.
     * @throws IgniteCheckedException If fails.
     */
    private boolean readPageFromStore(long pageId, ByteBuffer buff) throws IgniteCheckedException {
        buff.clear();

        boolean read = store.read(pageId, buff, true);

        assert read : toDetailString(pageId);

        return getType(buff) == flag(pageId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileDataPageIterator.class, this, super.toString());
    }
}
