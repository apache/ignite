/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query.continuous;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Continuous queries tests for replicated cache.
 */
public class GridCacheContinuousQueryReplicatedSelfTest extends GridCacheContinuousQueryAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteNodeCallback() throws Exception {
        GridCache<Integer, Integer> cache1 = grid(0).cache(null);

        GridCache<Integer, Integer> cache2 = grid(1).cache(null);

        GridCacheContinuousQuery<Integer, Integer> qry = cache2.queries().createContinuousQuery();

        final AtomicReference<Integer> val = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        qry.callback(new P2<UUID, Collection<Map.Entry<Integer, Integer>>>() {
            @Override public boolean apply(UUID uuid, Collection<Map.Entry<Integer, Integer>> entries) {
                assertEquals(1, entries.size());

                Map.Entry<Integer, Integer> e = entries.iterator().next();

                log.info("Entry: " + e);

                val.set(e.getValue());

                latch.countDown();

                return false;
            }
        });

        qry.execute();

        cache1.put(1, 10);

        latch.await(LATCH_TIMEOUT, MILLISECONDS);

        assertEquals(10, val.get().intValue());
    }
}
