package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class LimitedReducerLocalTest extends GridCommonAbstractTest {
    /** */
    private IgniteEx crd;

    /** */
    private LimitedReducer<Long> rdc;

    /** */
    private static final int PAGE_SIZE = 1024;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        crd = startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testEmptyResultWithLimit0() throws IgniteCheckedException {
        rdc = reducer(0);

//        rdc.setSources(crd.cluster().nodes(), 1);

        rdc.addPage(page(0, 0));

        assertFalse(rdc.hasNext());
    }

    /** */
    @Test
    public void testLimit0() throws IgniteCheckedException {
        rdc = reducer(0);

//        rdc.setSources(crd.cluster().nodes(), 1);

        rdc.addPage(page(0, 1024));

        assertFalse(rdc.hasNext());
    }

    /** */
    @Test
    public void testLimitThatHigherThenDataLen() throws IgniteCheckedException {
        rdc = reducer(1000);

//        rdc.setSources(crd.cluster().nodes(), 1);

        rdc.addPage(page(0, 100));

        for (int i = 0; i < 100; i++) {
            assertTrue(rdc.hasNext());
            assertEquals(i, (long)rdc.next());
        }

        assertFalse(rdc.hasNext());
    }

    /** */
    @Test
    public void testSingleStreamWithNextPages() throws IgniteCheckedException {
        rdc = reducer(10000);

//        rdc.setSources(crd.cluster().nodes(), 1);

        for (int i = 0; i < 3; i++)
            checkSinglePage(i * PAGE_SIZE, 3 * PAGE_SIZE);

        assertFalse(rdc.hasNext());
    }

    /** */
    @Test(expected = IgniteException.class)
    public void testStreamWithError() throws IgniteCheckedException {
        rdc = reducer(2000);

//        rdc.setSources(crd.cluster().nodes(), 1);

        CacheQueryResultPage errPage = failPage();

        rdc.addPage(errPage);

        rdc.hasNext();
    }

    /** TODO: what to expect? */
    public void testStreamWithErrorAndLimit0() throws IgniteCheckedException {
        rdc = reducer(0);

//        rdc.setSources(crd.cluster().nodes(), 1);

        CacheQueryResultPage errPage = failPage();

        rdc.addPage(errPage);

        assertFalse(rdc.hasNext());
    }

    /** */
    @Test(expected = IgniteException.class)
    public void testStreamWithErrorAfterData() throws IgniteCheckedException {
        rdc = reducer(10000);

//        rdc.setSources(crd.cluster().nodes(), 1);

        checkSinglePage(0, PAGE_SIZE * 2);

        CacheQueryResultPage errPage = failPage();

        rdc.addPage(errPage);

        rdc.hasNext();
    }

    /** */
    @Test
    public void checkMultipleStreams() throws IgniteCheckedException {
        rdc = reducer(2000);

//        rdc.setSources(crd.cluster().nodes(), 2);

        rdc.addPage(page(0, PAGE_SIZE));
        rdc.addPage(page(0, PAGE_SIZE));

        for (int i = 0; i < 2000; i++) {
            assertTrue(rdc.hasNext());
            rdc.next();
        }

        assertFalse(rdc.hasNext());
    }

    /** */
    @Test(expected = IgniteException.class)
    public void checkMultipleStreamsWithError() throws IgniteCheckedException {
        rdc = reducer(2000);

//        rdc.setSources(crd.cluster().nodes(), 2);

        rdc.addPage(page(0, PAGE_SIZE));
        rdc.addPage(failPage());

        rdc.hasNext();
    }

    private LimitedReducer<Long> reducer(int limit) {
        return new LimitedReducer<>(new UnorderedLocalReducer<>(null), limit);
    }

    private void checkSinglePage(int offset, int totalSize) throws IgniteCheckedException {
        rdc.addPage(page(offset, totalSize));

        for (int i = offset; i < offset + PAGE_SIZE; i++) {
            assertTrue(rdc.hasNext());
            assertEquals(i, (long)rdc.next());
        }
    }

    private CacheQueryResultPage failPage() {
        CacheQueryResultPage errPage = new CacheQueryResultPage(null, crd.localNode().id(), null);
        errPage.fail(new IgniteException());

        return errPage;
    }

    private CacheQueryResultPage page(int offset, int totalSize) {
        Collection<Long> result = new ArrayList<>();

        int cnt = offset;

        for (; cnt < offset + PAGE_SIZE && cnt < totalSize; cnt++)
            result.add((long) cnt);

        GridCacheQueryResponse resp = new GridCacheQueryResponse();
        resp.data(result);
        resp.finished(cnt == totalSize);

        return new CacheQueryResultPage(crd.context(), crd.localNode().id(), resp);
    }
}
