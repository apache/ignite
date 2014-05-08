/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

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
 * Test for {@link GridLifecycleAware} support for {@link GridCacheTmLookup}.
 */
public class GridTmLookupLifecycleAwareSelfTest extends GridAbstractLifecycleAwareSelfTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private GridCacheDistributionMode distroMode;

    /** */
    private boolean writeBehind;

    /**
     */
    private static class TestTxLookup extends GridAbstractLifecycleAwareSelfTest.TestLifecycleAware
        implements GridCacheTmLookup {
        /**
         */
        TestTxLookup() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Nullable @Override public TransactionManager getTm() throws GridException {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override protected final GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new GridTcpDiscoverySpi());

        GridCacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setDistributionMode(distroMode);

        ccfg.setWriteBehindEnabled(writeBehind);

        ccfg.setCacheMode(GridCacheMode.PARTITIONED);

        ccfg.setName(CACHE_NAME);

        ccfg.setTransactionManagerLookupClassName(TestTxLookup.class.getName());

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterGridStart(Grid grid) {
        TestTxLookup tmLookup =
            (TestTxLookup)((GridKernal)grid).context().cache().internalCache(null).context().jta().tmLookup();

        assertNotNull(tmLookup);

        lifecycleAwares.add(tmLookup);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ErrorNotRethrown")
    @Override public void testLifecycleAware() throws Exception {
        for (GridCacheDistributionMode mode : new GridCacheDistributionMode[] {PARTITIONED_ONLY, NEAR_PARTITIONED}) {
            distroMode = mode;

            writeBehind = false;

            try {
                super.testLifecycleAware();
            }
            catch (AssertionError e) {
                throw new AssertionError("Failed for [distroMode=" + distroMode + ", writeBehind=" + writeBehind + ']',
                    e);
            }

            writeBehind = true;

            try {
                super.testLifecycleAware();
            }
            catch (AssertionError e) {
                throw new AssertionError("Failed for [distroMode=" + distroMode + ", writeBehind=" + writeBehind + ']',
                    e);
            }
        }
    }
}
