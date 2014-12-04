/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests flags correctness.
 */
public class GridCacheMvccFlagsTest extends GridCommonAbstractTest {
    /** Grid. */
    private GridKernal grid;

    /**
     *
     */
    public GridCacheMvccFlagsTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        grid = (GridKernal)grid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     *
     */
    public void testAllTrueFlags() {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID id = UUID.randomUUID();

        GridCacheVersion ver = new GridCacheVersion(1, 0, 0, 0, 0);

        GridCacheMvccCandidate<String> c = new GridCacheMvccCandidate<>(
            entry,
            id,
            id,
            ver,
            1,
            ver,
            0,
            true,
            true,
            true,
            true,
            true,
            true
        );

        c.setOwner();
        c.setReady();
        c.setUsed();

        short flags = c.flags();

        info("Candidate: " + c);

        for (GridCacheMvccCandidate.Mask mask : GridCacheMvccCandidate.Mask.values())
            assertTrue("Candidate: " + c, mask.get(flags));
    }

    /**
     *
     */
    public void testAllFalseFlags() {
        GridCacheAdapter<String, String> cache = grid.internalCache();

        GridCacheTestEntryEx<String, String> entry = new GridCacheTestEntryEx<>(cache.context(), "1");

        UUID id = UUID.randomUUID();

        GridCacheVersion ver = new GridCacheVersion(1, 0, 0, 0, 0);

        GridCacheMvccCandidate<String> c = new GridCacheMvccCandidate<>(
            entry,
            id,
            id,
            ver,
            1,
            ver,
            0,
            false,
            false,
            false,
            false,
            false,
            false
        );

        short flags = c.flags();

        info("Candidate: " + c);

        for (GridCacheMvccCandidate.Mask mask : GridCacheMvccCandidate.Mask.values())
            assertFalse("Mask check failed [mask=" + mask + ", c=" + c + ']', mask.get(flags));
    }
}
