/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.local;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Load-All self test.
 */
public class GridCacheLocalLoadAllSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridCacheLocalLoadAllSelfTest() {
        super(true);
    }

    /**
     *
     * @throws Exception If test failed.
     */
    public void testCacheGetAll() throws Exception {
        Ignite ignite = grid();

        assert ignite != null;

        ignite.cache("test-cache").getAll(Collections.singleton(1));
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg =  super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration cache = defaultCacheConfiguration();

        cache.setName("test-cache");
        cache.setCacheMode(LOCAL);
        cache.setStore(new TestStore());

        cfg.setCacheConfiguration(cache);

        return cfg;
    }

    /**
     *
     */
    private static class TestStore extends GridCacheStoreAdapter<Integer, Integer> {
        /** {@inheritDoc} */
        @SuppressWarnings({"TypeParameterExtendsFinalClass"})
        @Override public void loadAll(GridCacheTx tx, Collection<? extends Integer> keys,
            GridBiInClosure<Integer, Integer> c) throws GridException {
            assert keys != null;

            c.apply(1, 1);
            c.apply(2, 2);
            c.apply(3, 3);
        }

        /** {@inheritDoc} */
        @Override public Integer load(GridCacheTx tx, Integer key) throws GridException {
            // No-op.

            return null;
        }

        /** {@inheritDoc} */
        @Override public void put(GridCacheTx tx, Integer key, Integer val) throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheTx tx, Integer key) throws GridException {
            // No-op.
        }
    }
}
