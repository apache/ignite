/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.transactions.GridCacheTxConcurrency.*;
import static org.apache.ignite.transactions.GridCacheTxIsolation.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Abstract test for private cache interface.
 */
public abstract class GridCacheExAbstractFullApiSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetOutTx() throws Exception {
        final AtomicInteger lockEvtCnt = new AtomicInteger();

        IgnitePredicate<IgniteEvent> lsnr = new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                lockEvtCnt.incrementAndGet();

                return true;
            }
        };

        try {
            grid(0).events().localListen(lsnr, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);

            GridCache<String, Integer> cache = cache();

            try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                int key = 0;

                for (int i = 0; i < 1000; i++) {
                    if (cache.affinity().mapKeyToNode("key" + i).id().equals(grid(0).localNode().id())) {
                        key = i;

                        break;
                    }
                }

                cache.get("key" + key);

                for (int i = key + 1; i < 1000; i++) {
                    if (cache.affinity().mapKeyToNode("key" + i).id().equals(grid(0).localNode().id())) {
                        key = i;

                        break;
                    }
                }

                ((GridCacheProjectionEx<String, Integer>)cache).getAllOutTx(F.asList("key" + key));
            }

            assertTrue(GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    info("Lock event count: " + lockEvtCnt.get());

                    return lockEvtCnt.get() == (nearEnabled() ? 4 : 2);
                }
            }, 15000));
        }
        finally {
            grid(0).events().stopLocalListen(lsnr, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);
        }
    }
}
