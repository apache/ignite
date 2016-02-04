package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test for eviction with offHeap.
 */
public class GridCacheOffHeapValuesEvictionSelfTest extends GridCacheAbstractSelfTest {

    private static final int VAL_SIZE = 512 * 1024; // bytes
    private static final int MAX_VALS_AMOUNT = 100;
    private static final int MAX_MEMORY_SIZE = MAX_VALS_AMOUNT * VAL_SIZE;
    private static final int VALS_AMOUNT = MAX_VALS_AMOUNT * 2;
    private static final int THREAD_COUNT = 4;

    @Override protected int gridCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutOnHeap() throws Exception {
        CacheConfiguration<Integer, Object> ccfg = cacheConfiguration(grid(0).name());
        ccfg.setName("testPutOffHeapValues");
        ccfg.setStatisticsEnabled(true);
        ccfg.setOffHeapMaxMemory(MAX_MEMORY_SIZE);

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxMemorySize(MAX_MEMORY_SIZE);

        ccfg.setSwapEnabled(true);
        ccfg.setMemoryMode(CacheMemoryMode.OFFHEAP_VALUES);

        ccfg.setEvictionPolicy(plc);

        final IgniteCache<Integer, Object> cache = grid(0).getOrCreateCache(ccfg);

        fillCache(cache, getTestTimeout());

        assertEquals(VALS_AMOUNT * THREAD_COUNT, cache.size(CachePeekMode.ALL));
        assertEquals(0, cache.size(CachePeekMode.NEAR));
        assertEquals(0, cache.size(CachePeekMode.OFFHEAP));
        assertTrue(MAX_VALS_AMOUNT >= cache.size(CachePeekMode.ONHEAP));
        assertTrue(MAX_VALS_AMOUNT - 5 <= cache.size(CachePeekMode.ONHEAP));
        assertEquals(cache.size(CachePeekMode.ALL) - cache.size(CachePeekMode.ONHEAP), cache.size(CachePeekMode.SWAP));
    }

    /**
     * swap disabled -> entries discarded
     * @throws Exception If failed.
     */
    public void testPutOnHeapWithOffHeap() throws Exception {
        final int PLC_MAX_SIZE = 50;

        CacheConfiguration<Integer, Object> ccfg = cacheConfiguration(grid(0).name());
        ccfg.setName("testPutOnHeapWithOffHeap");
        ccfg.setStatisticsEnabled(true);
        ccfg.setOffHeapMaxMemory(MAX_MEMORY_SIZE);

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxMemorySize(MAX_MEMORY_SIZE);
        plc.setMaxSize(PLC_MAX_SIZE);

        ccfg.setSwapEnabled(false);
        ccfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);

        ccfg.setEvictionPolicy(plc);

        final IgniteCache<Integer, Object> cache = grid(0).getOrCreateCache(ccfg);

        fillCache(cache, getTestTimeout());

        assertEquals(cache.size(CachePeekMode.ONHEAP) + cache.size(CachePeekMode.OFFHEAP), cache.size(CachePeekMode.ALL));
        assertEquals(0, cache.size(CachePeekMode.NEAR));
        assertEquals(0, cache.size(CachePeekMode.SWAP));
        assertTrue(PLC_MAX_SIZE >= cache.size(CachePeekMode.ONHEAP));
        assertTrue(PLC_MAX_SIZE - 5 <= cache.size(CachePeekMode.ONHEAP));
        assertTrue(MAX_VALS_AMOUNT >= cache.size(CachePeekMode.OFFHEAP));
        assertTrue(MAX_VALS_AMOUNT - 5 <= cache.size(CachePeekMode.OFFHEAP));
        assertEquals(cache.size(CachePeekMode.ALL) - cache.size(CachePeekMode.ONHEAP) - cache.size(CachePeekMode.OFFHEAP),
            cache.size(CachePeekMode.SWAP));

        assertTrue((MAX_VALS_AMOUNT + 5) * VAL_SIZE > cache.metrics().getOffHeapAllocatedSize());
        assertTrue((MAX_VALS_AMOUNT - 5) * VAL_SIZE < cache.metrics().getOffHeapAllocatedSize());
    }

    /**
     * swap enabled -> entries are not discarded
     * @throws Exception
     */
    public void testOnHeapWithOffHeapSwap() throws Exception{
        final int PLC_MAX_SIZE = 50;

        CacheConfiguration<Integer, Object> ccfg = cacheConfiguration(grid(0).name());
        ccfg.setName("testOnHeapWithOffHeapSwap");
        ccfg.setStatisticsEnabled(true);
        ccfg.setOffHeapMaxMemory(MAX_MEMORY_SIZE);

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxMemorySize(MAX_MEMORY_SIZE);
        plc.setMaxSize(PLC_MAX_SIZE);

        ccfg.setSwapEnabled(true);
        ccfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);

        ccfg.setEvictionPolicy(plc);

        final IgniteCache<Integer, Object> cache = grid(0).getOrCreateCache(ccfg);

        fillCache(cache, getTestTimeout());

        assertEquals(cache.size(CachePeekMode.SWAP) + cache.size(CachePeekMode.ONHEAP) +
            cache.size(CachePeekMode.OFFHEAP), cache.size(CachePeekMode.ALL));

        assertTrue(PLC_MAX_SIZE >= cache.size(CachePeekMode.ONHEAP));
        assertTrue(PLC_MAX_SIZE - 5 <= cache.size(CachePeekMode.ONHEAP));
        assertTrue(MAX_VALS_AMOUNT >= cache.size(CachePeekMode.OFFHEAP));
        assertTrue(MAX_VALS_AMOUNT - 5 <= cache.size(CachePeekMode.OFFHEAP));

        assertTrue((MAX_VALS_AMOUNT + 5) * VAL_SIZE > cache.metrics().getOffHeapAllocatedSize());
        assertTrue((MAX_VALS_AMOUNT - 5) * VAL_SIZE < cache.metrics().getOffHeapAllocatedSize());
    }

    private static void fillCache(IgniteCache<Integer, Object> cache, long timeout) throws Exception{
        final byte[] val = new byte[VAL_SIZE];
        final AtomicInteger keyStart = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(4);

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                final int start = keyStart.addAndGet(VALS_AMOUNT);

                for (int i = start; i < start + VALS_AMOUNT; i++)
                    cache.put(i, val);

                latch.countDown();

                return null;
            }
        }, THREAD_COUNT, "test");

        latch.await(timeout, TimeUnit.MILLISECONDS);
    }
}
