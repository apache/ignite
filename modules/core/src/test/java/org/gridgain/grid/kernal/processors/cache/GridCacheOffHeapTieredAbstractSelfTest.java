/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMemoryMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * TODO 9198: more tests.
 */
public class GridCacheOffHeapTieredAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setAtomicWriteOrderMode(PRIMARY);

        ccfg.setMemoryMode(OFFHEAP_TIERED);
        ccfg.setOffHeapMaxMemory(0);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransform() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        checkTransform(primaryKey(cache));

        checkTransform(backupKey(cache));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkTransform(Integer key) throws Exception {
        GridCache<Integer, Integer> c = grid(0).cache(null);

        c.transform(key, new C1<Integer, Integer>() {
            @Override public Integer apply(Integer val) {
                assertNull("Unexpected value: " + val, val);

                return null;
            }
        });

        c.putx(key, 1);

        c.transform(key, new C1<Integer, Integer>() {
            @Override public Integer apply(Integer val) {
                assertNotNull("Unexpected value: " + val, val);

                assertEquals((Integer) 1, val);

                return val + 1;
            }
        });

        assertEquals((Integer)2, c.get(key));

        c.transform(key, new C1<Integer, Integer>() {
            @Override public Integer apply(Integer val) {
                assertNotNull("Unexpected value: " + val, val);

                assertEquals((Integer)2, val);

                return val;
            }
        });

        assertEquals((Integer)2, c.get(key));

        c.transform(key, new C1<Integer, Integer>() {
            @Override public Integer apply(Integer val) {
                assertNotNull("Unexpected value: " + val, val);

                assertEquals((Integer)2, val);

                return null;
            }
        });

        assertNull(c.get(key));
    }
}
