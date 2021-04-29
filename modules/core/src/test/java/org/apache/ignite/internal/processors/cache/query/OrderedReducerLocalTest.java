package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class OrderedReducerLocalTest extends GridCommonAbstractTest {
    /** */
    private IgniteEx crd;

    /** */
    private OrderedReducer<Long> rdc;

    /** */
    private static final int PAGE_SIZE = 1024;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        crd = startGrid();

        CacheQueryResultFetcher noOpFetcher = new CacheQueryResultFetcher(null, null) {
            @Override public void fetchPages(long reqId, Collection<UUID> nodes, boolean all) {

            }

            @Override protected void sendLocal(UUID locNodeId, GridCacheQueryRequest req) {

            }
        };

        rdc = new OrderedReducer<>(crd.context(), 0L, noOpFetcher, Long::compare);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testSingleStreamWithNextPages()  {
        rdc.setSources(crd.cluster().nodes());

        for (int i = 0; i < 3; i++)
            checkSinglePage(i * PAGE_SIZE, 3 * PAGE_SIZE);

        assertFalse(rdc.hasNext());
    }

    /** */
    @Test(expected = IgniteException.class)
    public void testStreamWithError() {
        rdc.setSources(crd.cluster().nodes());

        CacheQueryResultPage errPage = failPage();

        rdc.addPage(errPage);

        rdc.hasNext();
    }

    /** */
    @Test(expected = IgniteException.class)
    public void testStreamWithErrorAfterData() {
        rdc.setSources(crd.cluster().nodes());

        checkSinglePage(0, PAGE_SIZE * 2);

        CacheQueryResultPage errPage = failPage();

        rdc.addPage(errPage);

        rdc.hasNext();
    }

    /** */
    @Test
    public void checkMultipleStreams() throws Exception {
        IgniteEx node = startGrid(1);

        rdc.setSources(crd.cluster().nodes());

        rdc.addPage(page(0, PAGE_SIZE));
        rdc.addPage(page(0, PAGE_SIZE, node.localNode().id()));

        // Should provide sorted result.
        for (int i = 0; i < PAGE_SIZE * 2; i++) {
            assertTrue(rdc.hasNext());
            assertEquals(i / 2, (long) rdc.next());
        }

        assertFalse(rdc.hasNext());
    }

    /** */
    @Test(expected = IgniteException.class)
    public void checkMultipleStreamsWithError() {
        rdc.setSources(crd.cluster().nodes());

        rdc.addPage(page(0, PAGE_SIZE));
        rdc.addPage(failPage());

        rdc.hasNext();
    }


    private void checkSinglePage(int offset, int totalSize) {
        rdc.addPage(page(offset, totalSize));

        for (int i = offset; i < offset + PAGE_SIZE; i++) {
            assertTrue(rdc.hasNext());
            assertEquals(i, (long) rdc.next());
        }
    }

    private CacheQueryResultPage failPage() {
        CacheQueryResultPage errPage = new CacheQueryResultPage(null, crd.localNode().id(), null);
        errPage.fail(new IgniteException());

        return errPage;
    }

    private CacheQueryResultPage page(int offset, int totalSize) {
        return page(offset, totalSize, crd.localNode().id());
    }

    private CacheQueryResultPage page(int offset, int totalSize, UUID nodeId) {
        Collection<Long> result = new ArrayList<>();

        int cnt = offset;

        for (; cnt < offset + PAGE_SIZE && cnt < totalSize; cnt++)
            result.add((long) cnt);

        GridCacheQueryResponse resp = new GridCacheQueryResponse();
        resp.data(result);
        resp.finished(cnt == totalSize);

        return new CacheQueryResultPage(crd.context(), nodeId, resp);
    }
}
