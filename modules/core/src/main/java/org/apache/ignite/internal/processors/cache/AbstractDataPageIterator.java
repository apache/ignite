package org.apache.ignite.internal.processors.cache;

import java.util.BitSet;
import java.util.Deque;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.T_DATA;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getPageIO;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getPageId;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getVersion;

/** */
public abstract class AbstractDataPageIterator extends GridCloseableIteratorAdapter<CacheDataRow> {
    /** Page store partition id. */
    protected final int partId;

    /** Grid cache shared context. */
    private final GridCacheSharedContext<?, ?> sctx;

    /** Cache object context for key/value deserialization. */
    private final CacheObjectContext coctx;

    /** Scan data arguments. */
    private final CacheDataRowAdapter.RowData rowData;

    /** Whether version read should be skipped. */
    private final boolean skipVer;

    /** {@code true} if cache id can be read due to it is stored on page. */
    private final boolean readCacheId;

    /** Total pages in the page store. */
    private final int pages;

    /** Size of pages in given region. */
    private final int pageSize;

    /**
     * Data row greater than page size contains with header and tail parts. Such pages with tails contain only part
     * of a cache key-value pair. These pages will be marked and skipped at the first partition iteration and
     * will be processed on the second partition iteration when all the pages with key-value headers defined.
     */
    private final BitSet tailPages;

    /** Pages which already read and must be skipped. */
    private final BitSet readPages;

    /** Batch of rows read through iteration. */
    private final Deque<CacheDataRow> rows = new LinkedList<>();

    /** {@code true} if the iteration though partition reached its end. */
    private boolean secondScanComplete;

    /**
     * Current partition page index for read. Due to we read the partition twice it
     * can't be greater that 2 * store.size().
     */
    private int currIdx;

    /**
     * During scanning a cache partition presented as {@code PageStore} we must guarantee the following:
     * all the pages of this storage remains unchanged during the Iterator remains opened, the stored data
     * keeps its consistency. We can't read the {@code PageStore} during an ongoing checkpoint over it.
     *
     * @param coctx Cache object context.
     * @param partId Partition id.
     */
    protected AbstractDataPageIterator(
        GridCacheSharedContext<?, ?> sctx,
        CacheObjectContext coctx,
        CacheDataRowAdapter.RowData rowData,
        boolean readCacheId,
        boolean skipVer,
        int pages,
        int pageSize,
        int partId
    ) {
        this.partId = partId;
        this.coctx = coctx;
        this.sctx = sctx;
        this.rowData = rowData;
        this.readCacheId = readCacheId;
        this.skipVer = skipVer;
        this.pages = pages;
        this.pageSize = pageSize;

        tailPages = new BitSet(pages);
        readPages = new BitSet(pages);
    }

    /**
     * @param pageId Page id.
     * @param reader Data page processor.
     * @throws IgniteCheckedException If fails.
     * @return
     */
    public abstract boolean readHeaderPage(
        long pageId,
        LongConsumer reader
    ) throws IgniteCheckedException;

    /**
     * @param pageId Page id.
     * @param reader Data page processor.
     * @return Incomplete data object.
     * @throws IgniteCheckedException If fails.
     */
    public abstract IncompleteObject<?> readFragmentPage(
        long pageId,
        LongFunction<IncompleteObject<?>> reader
    ) throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override protected CacheDataRow onNext() throws IgniteCheckedException {
        if (secondScanComplete && rows.isEmpty())
            throw new NoSuchElementException("[partId=" + partId + ", pages=" + pages + ", skipPages=" + readPages + ']');

        return rows.poll();
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() throws IgniteCheckedException {
        if (secondScanComplete && rows.isEmpty())
            return false;

        try {
            for (; currIdx < 2 * pages && rows.isEmpty(); currIdx++) {
                boolean first = currIdx < pages;
                int pageIdx = currIdx % pages;

                if (readPages.get(pageIdx) || (!first && tailPages.get(pageIdx)))
                    continue;

                boolean dataPage = readHeaderPage(pageId(partId, FLAG_DATA, pageIdx), pageAddr -> {
                    try {
                        assert flag(getPageId(pageAddr)) == FLAG_DATA;

                        DataPageIO io = getPageIO(T_DATA, getVersion(pageAddr));
                        int freeSpace = io.getFreeSpace(pageAddr);
                        int rowsCnt = io.getDirectCount(pageAddr);

                        if (currIdx < pages) {
                            // Skip empty pages.
                            if (rowsCnt == 0) {
                                setBit(readPages, pageIdx);

                                return;
                            }

                            // There is no difference between a page containing an incomplete DataRow fragment and
                            // the page where DataRow takes up all the free space. There is no a dedicated
                            // flag for this case in page header.
                            // During the storage scan we can skip such pages at the first iteration over the partition file,
                            // since all the fragmented pages will be marked by BitSet array we will safely read the others
                            // on the second iteration.
                            if (freeSpace == 0 && rowsCnt == 1) {
                                DataPagePayload payload = io.readPayload(pageAddr, 0, pageSize);

                                long link = payload.nextLink();

                                if (link != 0)
                                    setBit(tailPages, pageIndex(pageId(link)));

                                return;
                            }
                        }

                        setBit(readPages, pageIdx);

                        for (int itemId = 0; itemId < rowsCnt; itemId++) {
                            DataRow row = new DataRow().partition(partId);

                            IncompleteObject<?> incomplete = row.readIncomplete(null, sctx, coctx, pageSize, pageSize, pageAddr,
                                itemId, PageIO.getPageIO(T_DATA, PageIO.getVersion(pageAddr)), rowData,
                                readCacheId, skipVer);

                            long nextLink;

                            while (incomplete != null && (nextLink = incomplete.getNextLink()) != 0) {
                                assert itemId(nextLink) == 0 : "Only one item is possible on the fragmented page: " + PageIdUtils.toDetailString(nextLink);

                                long finalNextLink = nextLink;
                                IncompleteObject<?> finalIncomplete = incomplete;

                                incomplete = readFragmentPage(pageId(finalNextLink), nextAddr -> {
                                    assert flag(getPageId(nextAddr)) == FLAG_DATA;

                                    try {
                                        IncompleteObject<?> incomplete0 = row.readIncomplete(finalIncomplete, sctx, coctx, pageSize, pageSize,
                                            nextAddr, 0, PageIO.getPageIO(T_DATA, PageIO.getVersion(pageAddr)),
                                            rowData, readCacheId, skipVer);

                                        setBit(readPages, pageIndex(pageId(finalNextLink)));

                                        return incomplete0;
                                    }
                                    catch (IgniteCheckedException e) {
                                        throw F.wrap(e);
                                    }
                                });
                            }

                            assert row.isReady() : "Entry must has the 'ready' state, when the init ends";

                            rows.add(row);
                        }
                    }
                    catch (IgniteCheckedException e) {
                        // TODO add here CacheDataRowAdapter.relatedPageIds
                        throw F.wrap(e);
                    }
                });

                // TODO does this boolean flag required?
                if (!dataPage)
                    setBit(readPages, pageIdx); // Skip not FLAG_DATA pages.
            }

            if (currIdx == 2 * pages) {
                secondScanComplete = true;

                boolean set = true;

                for (int j = 0; j < pages; j++)
                    set &= readPages.get(j);

                assert set : "readPages=" + readPages + ", pages=" + pages;
            }

            return !rows.isEmpty();
        }
        catch (IgniteCheckedException | GridClosureException e) {
            throw new IgniteCheckedException("Error during iteration through page store: " + this, e);
        }
    }

    /**
     * @param bitSet BitSet to change bit index.
     * @param idx Index of bit to change.
     */
    private static void setBit(BitSet bitSet, int idx) {
        boolean bit = bitSet.get(idx);

        assert !bit : "Bit with given index already set: " + idx;

        bitSet.set(idx);
    }

    /**
     * {@inheritDoc}
     */
    @Override public String toString() {
        return S.toString(AbstractDataPageIterator.class, this, super.toString());
    }
}
