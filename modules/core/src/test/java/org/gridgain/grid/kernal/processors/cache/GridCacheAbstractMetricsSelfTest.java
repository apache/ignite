/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Cache metrics test.
 */
public abstract class GridCacheAbstractMetricsSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int KEY_CNT = 50;

    /** {@inheritDoc} */
    @Override protected GridCacheWriteSynchronizationMode writeSynchronization() {
        return FULL_SYNC;
    }

    /** {@inheritDoc} */
    @Override protected boolean swapEnabled() {
        return false;
    }

    /**
     * @return Key count.
     */
    protected int keyCount() {
        return KEY_CNT;
    }

    /**
     * Gets number of inner reads per "put" operation.
     *
     * @param isPrimary {@code true} if local node is primary for current key, {@code false} otherwise.
     * @return Expected number of inner reads.
     */
    protected int expectedReadsPerPut(boolean isPrimary) {
        return isPrimary ? 1 : 2;
    }

    /**
     * Gets number of missed per "put" operation.
     *
     * @param isPrimary {@code true} if local node is primary for current key, {@code false} otherwise.
     * @return Expected number of misses.
     */
    protected int expectedMissesPerPut(boolean isPrimary) {
        return isPrimary ? 1 : 2;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (int i = 0; i < gridCount(); i++) {
            Grid g = grid(i);

            g.cache(null).removeAll();

            assert g.cache(null).isEmpty();

            g.cache(null).resetMetrics();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testWritesReads() throws Exception {
        GridCache<Integer, Integer> cache0 = grid(0).cache(null);

        int keyCnt = keyCount();

        int expReads = 0;
        int expMisses = 0;

        // Put and get a few keys.
        for (int i = 0; i < keyCnt; i++) {
            cache0.put(i, i); // +1 read

            boolean isPrimary = cache0.affinity().isPrimary(grid(0).localNode(), i);

            expReads += expectedReadsPerPut(isPrimary);
            expMisses += expectedMissesPerPut(isPrimary);

            info("Writes: " + cache0.metrics().writes());

            for (int j = 0; j < gridCount(); j++) {
                GridCache<Integer, Integer> cache = grid(j).cache(null);

                int cacheWrites = cache.metrics().writes();

                assertEquals("Wrong cache metrics [i=" + i + ", grid=" + j + ']', i + 1, cacheWrites);
            }

            assertEquals("Wrong value for key: " + i, Integer.valueOf(i), cache0.get(i)); // +1 read

            expReads++;
        }

        // Check metrics for the whole cache.
        long writes = 0;
        long reads = 0;
        long hits = 0;
        long misses = 0;

        for (int i = 0; i < gridCount(); i++) {
            GridCacheMetrics m = grid(i).cache(null).metrics();

            writes += m.writes();
            reads += m.reads();
            hits += m.hits();
            misses += m.misses();
        }

        info("Stats [reads=" + reads + ", hits=" + hits + ", misses=" + misses + ']');

        assertEquals(keyCnt * gridCount(), writes);
        assertEquals(expReads, reads);
        assertEquals(keyCnt, hits);
        assertEquals(expMisses, misses);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMisses() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        // TODO: GG-7578.
        if (cache.configuration().getCacheMode() == GridCacheMode.REPLICATED)
            return;

        int keyCnt = keyCount();

        int expReads = 0;

        // Get a few keys missed keys.
        for (int i = 0; i < keyCnt; i++) {
            assertNull("Value is not null for key: " + i, cache.get(i));

            if (cache.affinity().isPrimary(grid(0).localNode(), i))
                expReads++;
            else
                expReads += 2;
        }

        // Check metrics for the whole cache.
        long writes = 0;
        long reads = 0;
        long hits = 0;
        long misses = 0;

        for (int i = 0; i < gridCount(); i++) {
            GridCacheMetrics m = grid(i).cache(null).metrics();

            writes += m.writes();
            reads += m.reads();
            hits += m.hits();
            misses += m.misses();
        }

        assertEquals(0, writes);
        assertEquals(expReads, reads);
        assertEquals(0, hits);
        assertEquals(expReads, misses);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMissesOnEmptyCache() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        // TODO: GG-7578.
        if (cache.configuration().getCacheMode() == GridCacheMode.REPLICATED)
            return;

        Integer key =  null;

        for (int i = 0; i < 1000; i++) {
            if (cache.affinity().isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        cache.get(key);

        assertEquals("Expected 1 read", 1, cache.metrics().reads());
        assertEquals("Expected 1 miss", 1, cache.metrics().misses());

        cache.put(key, key); // +1 read, +1 miss.

        cache.get(key);

        assertEquals("Expected 1 write", 1, cache.metrics().writes());
        assertEquals("Expected 3 reads", 3, cache.metrics().reads());
        assertEquals("Expected 2 misses", 2, cache.metrics().misses());
        assertEquals("Expected 1 hit", 1, cache.metrics().hits());
    }
}
