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
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test for {@link GridCacheConfiguration#isStoreValueBytes()}.
 */
public class GridCacheStoreValueBytesSelfTest extends GridCommonAbstractTest {
    /** */
    private boolean storeValBytes;

    /** VM ip finder for TCP discovery. */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(REPLICATED);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setStoreValueBytes(storeValBytes);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testDisabled() throws Exception {
        storeValBytes = false;

        Ignite g0 = startGrid(0);
        Ignite g1 = startGrid(1);

        GridCache<Integer, String> c = g0.cache(null);

        c.put(1, "Cached value");

        GridCacheEntryEx<Object, Object> entry = ((GridKernal)g1).internalCache().peekEx(1);

        assert entry != null;
        assert entry.valueBytes().isNull();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEnabled() throws Exception {
        storeValBytes = true;

        Ignite g0 = startGrid(0);
        Ignite g1 = startGrid(1);

        GridCache<Integer, String> c = g0.cache(null);

        c.put(1, "Cached value");

        GridCacheEntryEx<Object, Object> entry = ((GridKernal)g1).internalCache().peekEx(1);

        assert entry != null;
        assert entry.valueBytes() != null;
    }
}
