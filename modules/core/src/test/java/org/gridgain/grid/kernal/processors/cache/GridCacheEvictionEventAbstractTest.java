/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.GridEventType.*;

/**
 * Eviction event self test.
 */
public abstract class GridCacheEvictionEventAbstractTest extends GridCommonAbstractTest {
    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    protected GridCacheEvictionEventAbstractTest() {
        super(true); // Start node.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration c = super.getConfiguration();

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setAtomicityMode(atomicityMode());
        cc.setEvictNearSynchronized(isNearEvictSynchronized());

        c.setCacheConfiguration(cc);

        c.setIncludeEventTypes(EVT_CACHE_ENTRY_EVICTED, EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        return c;
    }

    /**
     * @return Cache mode.
     */
    protected abstract GridCacheMode cacheMode();

    /**
     * @return Atomicity mode.
     */
    protected abstract GridCacheAtomicityMode atomicityMode();

    /**
     * @return {@code True} if near evicts synchronized.
     */
    protected boolean isNearEvictSynchronized() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictionEvent() throws Exception {
        Ignite g = grid();

        final CountDownLatch latch = new CountDownLatch(1);

        final AtomicReference<String> oldVal = new AtomicReference<>();

        g.events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                GridCacheEvent e = (GridCacheEvent) evt;

                oldVal.set((String) e.oldValue());

                latch.countDown();

                return true;
            }
        }, GridEventType.EVT_CACHE_ENTRY_EVICTED);

        GridCache<String, String> c = g.cache(null);

        c.put("1", "val1");

        c.evict("1");

        latch.await();

        assertNotNull(oldVal.get());
    }
}
