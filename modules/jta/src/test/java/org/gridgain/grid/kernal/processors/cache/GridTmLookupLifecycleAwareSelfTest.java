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
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.jta.GridCacheTmLookup;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import javax.transaction.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Test for {@link org.apache.ignite.lifecycle.LifecycleAware} support for {@link GridCacheTmLookup}.
 */
public class GridTmLookupLifecycleAwareSelfTest extends GridAbstractLifecycleAwareSelfTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private GridCacheDistributionMode distroMode;

    /**
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestTxLookup extends GridAbstractLifecycleAwareSelfTest.TestLifecycleAware
        implements GridCacheTmLookup {
        /**
         */
        public TestTxLookup() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Nullable @Override public TransactionManager getTm() throws GridException {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new GridTcpDiscoverySpi());

        GridCacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setDistributionMode(distroMode);

        ccfg.setCacheMode(GridCacheMode.PARTITIONED);

        ccfg.setName(CACHE_NAME);

        ccfg.setTransactionManagerLookupClassName(TestTxLookup.class.getName());

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterGridStart(Ignite ignite) {
        TestTxLookup tmLookup =
            (TestTxLookup)((GridKernal) ignite).context().cache().internalCache(CACHE_NAME).context().jta().tmLookup();

        assertNotNull(tmLookup);

        lifecycleAwares.add(tmLookup);
    }

    /** {@inheritDoc} */
    @Override public void testLifecycleAware() throws Exception {
        for (GridCacheDistributionMode mode : new GridCacheDistributionMode[] {PARTITIONED_ONLY, NEAR_PARTITIONED}) {
            distroMode = mode;

            super.testLifecycleAware();
        }
    }
}
