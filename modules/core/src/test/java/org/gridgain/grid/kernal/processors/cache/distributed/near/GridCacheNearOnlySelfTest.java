/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Near only self test.
 */
public class GridCacheNearOnlySelfTest extends GridCacheClientModesAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected boolean clientOnly() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateNearOnlyReader() throws Exception {
        GridCache<Object, Object> dhtCache = dhtCache();

        final int keyCnt = 100;

        for (int i = 0; i < keyCnt; i++)
            dhtCache.put(i, i);

        GridCache<Object, Object> nearOnlyCache = nearOnlyCache();

        for (int i = 0; i < keyCnt; i++) {
            assertNull(nearOnlyCache.peek(i));

            assertEquals(i, nearOnlyCache.get(i));
            assertEquals(i, nearOnlyCache.peek(i));
        }

        for (int i = 0; i < keyCnt; i++)
            dhtCache.put(i, i * i);

        for (int i = 0; i < keyCnt; i++) {
            assertEquals(i * i, nearOnlyCache.peek(i));

            assertEquals(i * i, nearOnlyCache.get(i));
        }
    }
}
