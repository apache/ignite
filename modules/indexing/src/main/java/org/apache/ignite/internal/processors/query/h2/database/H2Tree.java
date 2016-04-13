package org.apache.ignite.internal.processors.query.h2.database;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.h2.result.SearchRow;

/**
 */
public abstract class H2Tree extends BPlusTree<SearchRow, GridH2Row> {
    /** */
    private final int cacheId;

    /** */
    private final H2RowStore rowStore;

    /** */
    private final PageMemory pageMem;

    /**
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param rowStore Row data store.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    public H2Tree(int cacheId, PageMemory pageMem, H2RowStore rowStore, FullPageId metaPageId, boolean initNew)
        throws IgniteCheckedException {
        super(metaPageId);

        assert pageMem != null;
        assert rowStore != null;

        this.cacheId = cacheId;
        this.pageMem = pageMem;
        this.rowStore = rowStore;

        if (initNew)
            initNew();
    }

    /**
     * @return Row store.
     */
    public H2RowStore getRowStore() {
        return rowStore;
    }

    /** {@inheritDoc} */
    @Override protected Page page(long pageId) throws IgniteCheckedException {
        return pageMem.page(new FullPageId(pageId, cacheId));
    }

    /** {@inheritDoc} */
    @Override protected Page allocatePage() throws IgniteCheckedException {
        FullPageId pageId = pageMem.allocatePage(cacheId, -1, PageIdAllocator.FLAG_IDX);

        return pageMem.page(pageId);
    }

    /** {@inheritDoc} */
    @Override protected BPlusIO<SearchRow> io(int type, int ver) {
        if (type == PageIO.T_H2_REF_INNER)
            return H2InnerIO.VERSIONS.forVersion(ver);

        assert type == PageIO.T_H2_REF_LEAF: type;

        return H2LeafIO.VERSIONS.forVersion(ver);
    }

    /** {@inheritDoc} */
    @Override protected BPlusInnerIO<SearchRow> latestInnerIO() {
        return H2InnerIO.VERSIONS.latest();
    }

    /** {@inheritDoc} */
    @Override protected BPlusLeafIO<SearchRow> latestLeafIO() {
        return H2LeafIO.VERSIONS.latest();
    }

    /** {@inheritDoc} */
    @Override protected GridH2Row getRow(BPlusIO<SearchRow> io, ByteBuffer buf, int idx)
        throws IgniteCheckedException {
        return (GridH2Row)io.getLookupRow(this, buf, idx);
    }
}


