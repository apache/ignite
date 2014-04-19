/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Abstract class for Hadoop tests.
 */
public class GridHadoopAbstractSelfTest extends GridCommonAbstractTest {
    /** Shared IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Hadoop system cache name. */
    protected static final String hadoopSysCacheName = "hadoop-system-cache";

    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setHadoopConfiguration(hadoopConfiguration(gridName));

        GridCacheConfiguration cacheCfg = new GridCacheConfiguration();

        cacheCfg.setCacheMode(GridCacheMode.REPLICATED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setName(hadoopSysCacheName);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Hadoop configuration.
     */
    public GridHadoopConfiguration hadoopConfiguration(String gridName) {
        GridHadoopConfiguration hadoopCfg = new GridHadoopConfiguration();

        hadoopCfg.setSystemCacheName(hadoopSysCacheName);

        return hadoopCfg;
    }

    /**
     * @return {@code True} if GGFS is enabled on Hadoop nodes.
     */
    protected boolean ggfsEnabled() {
        return false;
    }

    /**
     * @return Number of nodes to start.
     */
    protected int gridCount() {
        return 3;
    }
}
