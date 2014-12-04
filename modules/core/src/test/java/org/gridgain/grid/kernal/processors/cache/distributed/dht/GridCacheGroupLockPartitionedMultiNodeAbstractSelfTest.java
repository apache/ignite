/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Multi-node test for group locking.
 */
public abstract class GridCacheGroupLockPartitionedMultiNodeAbstractSelfTest extends
    GridCacheGroupLockPartitionedAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonLocalKeyOptimistic() throws Exception {
        checkNonLocalKey(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonLocalKeyPessimistic() throws Exception {
        checkNonLocalKey(PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNonLocalKey(GridCacheTxConcurrency concurrency) throws Exception {
        final UUID key = primaryKeyForCache(grid(1));

        GridCache<Object, Object> cache = grid(0).cache(null);

        GridCacheTx tx = null;
        try {
            tx = cache.txStartAffinity(key, concurrency, READ_COMMITTED, 0, 2);

            cache.put(new GridCacheAffinityKey<>("1", key), "2");

            tx.commit();

            fail("Exception should be thrown.");
        }
        catch (GridException ignored) {
            // Expected exception.
        }
        finally {
            if (tx != null)
                tx.close();

            assertNull(cache.tx());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearReadersUpdateWithAffinityReaderOptimistic() throws Exception {
        checkNearReadersUpdate(true, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearReadersUpdateWithAffinityReaderPessimistic() throws Exception {
        checkNearReadersUpdate(true, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearReaderUpdateWithoutAffinityReaderOptimistic() throws Exception {
        checkNearReadersUpdate(false, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearReaderUpdateWithoutAffinityReaderPessimistic() throws Exception {
        checkNearReadersUpdate(false, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNearReadersUpdate(boolean touchAffKey, GridCacheTxConcurrency concurrency) throws Exception {
        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);
        GridCacheAffinityKey<String> key2 = new GridCacheAffinityKey<>("key2", affinityKey);
        GridCacheAffinityKey<String> key3 = new GridCacheAffinityKey<>("key3", affinityKey);

        grid(0).cache(null).put(affinityKey, "aff");

        GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        cache.putAll(F.asMap(
            key1, "val1",
            key2, "val2",
            key3, "val3")
        );

        Ignite reader = null;

        for (int i = 0; i < gridCount(); i++) {
            if (!grid(i).cache(null).affinity().isPrimaryOrBackup(grid(i).localNode(), affinityKey))
                reader = grid(i);
        }

        assert reader != null;

        info(">>> Reader is " + reader.cluster().localNode().id());

        // Add reader.
        if (touchAffKey)
            assertEquals("aff", reader.cache(null).get(affinityKey));

        assertEquals("val1", reader.cache(null).get(key1));
        assertEquals("val2", reader.cache(null).get(key2));
        assertEquals("val3", reader.cache(null).get(key3));

        if (nearEnabled()) {
            assertEquals("val1", reader.cache(null).peek(key1));
            assertEquals("val2", reader.cache(null).peek(key2));
            assertEquals("val3", reader.cache(null).peek(key3));
        }

        try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 3)) {
            cache.putAll(F.asMap(
                key1, "val01",
                key2, "val02",
                key3, "val03")
            );

            tx.commit();
        }

        if (nearEnabled()) {
            assertEquals("val01", reader.cache(null).peek(key1));
            assertEquals("val02", reader.cache(null).peek(key2));
            assertEquals("val03", reader.cache(null).peek(key3));
        }
    }
}
