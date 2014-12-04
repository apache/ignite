/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Base class for various tests for byte array values.
 */
public abstract class GridCacheAbstractByteArrayValuesSelfTest extends GridCommonAbstractTest {
    /** Regular cache name. */
    protected static final String CACHE_REGULAR = "cache";

    /** Offheap cache name. */
    protected static final String CACHE_OFFHEAP = "cache_offheap";

    /** Offheap tiered cache name. */
    protected static final String CACHE_OFFHEAP_TIERED = "cache_offheap_tiered";

    /** Key 1. */
    protected static final Integer KEY_1 = 1;

    /** Key 2. */
    protected static final Integer KEY_2 = 2;

    /** Use special key for swap test, otherwise entry with readers is not evicted. */
    protected static final Integer SWAP_TEST_KEY = 3;

    /** Shared IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Wrap provided values into byte array.
     *
     * @param vals Values.
     * @return Byte array.
     */
    protected byte[] wrap(int... vals) {
        byte[] res = new byte[vals.length];

        for (int i = 0; i < vals.length; i++)
            res[i] = (byte)vals[i];

        return res;
    }
}
