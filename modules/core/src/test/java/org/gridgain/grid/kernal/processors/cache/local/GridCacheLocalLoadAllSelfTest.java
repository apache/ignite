/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.local;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

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
            IgniteBiInClosure<Integer, Integer> c) throws IgniteCheckedException {
            assert keys != null;

            c.apply(1, 1);
            c.apply(2, 2);
            c.apply(3, 3);
        }

        /** {@inheritDoc} */
        @Override public Integer load(GridCacheTx tx, Integer key) throws IgniteCheckedException {
            // No-op.

            return null;
        }

        /** {@inheritDoc} */
        @Override public void put(GridCacheTx tx, Integer key, Integer val) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheTx tx, Integer key) throws IgniteCheckedException {
            // No-op.
        }
    }
}
