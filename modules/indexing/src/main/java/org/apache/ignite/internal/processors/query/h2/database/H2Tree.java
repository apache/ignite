package org.apache.ignite.internal.processors.query.h2.database;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.h2.result.SearchRow;

/**
 */
public abstract class H2Tree extends BPlusTree<SearchRow, GridH2Row> {
    /** */
    private final H2RowStore rowStore;

    /**
     * @param name Tree name.
     * @param reuseList Reuse list.
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param rowStore Row data store.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    public H2Tree(
        String name,
        ReuseList reuseList,
        int cacheId,
        PageMemory pageMem,
        H2RowStore rowStore,
        FullPageId metaPageId,
        boolean initNew
    ) throws IgniteCheckedException {
        super(name, cacheId, pageMem, metaPageId, reuseList, H2InnerIO.VERSIONS, H2LeafIO.VERSIONS);

        assert rowStore != null;

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
    @Override protected GridH2Row getRow(BPlusIO<SearchRow> io, ByteBuffer buf, int idx)
        throws IgniteCheckedException {
        return (GridH2Row)io.getLookupRow(this, buf, idx);
    }
}


