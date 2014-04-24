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
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.TRANSACTIONAL;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Abstract class for Hadoop tests.
 */
public class GridHadoopAbstractSelfTest extends GridCommonAbstractTest {
    /** Hadoop system cache name. */
    protected static final String hadoopSysCacheName = "hadoop-system-cache";

    /** GGFS name. */
    protected static final String ggfsName = "ggfs";

    /** GGFS name. */
    protected static final String ggfsMetaCacheName = "meta";

    /** GGFS name. */
    protected static final String ggfsDataCacheName = "data";

    /** GGFS block size. */
    protected static final int ggfsBlockSize = 1024;

    /** GGFS block group size. */
    protected static final int ggfsBlockGroupSize = 8;

    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setHadoopConfiguration(hadoopConfiguration(gridName));

        GridCacheConfiguration cacheCfg = new GridCacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setAtomicWriteOrderMode(PRIMARY);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setName(hadoopSysCacheName);

        if (ggfsEnabled()) {
            cfg.setCacheConfiguration(cacheCfg, metaCacheConfiguration(), dataCacheConfiguration());

            cfg.setGgfsConfiguration(ggfsConfiguration());
        }

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
     * @return GGFS configuration.
     */
    public GridGgfsConfiguration ggfsConfiguration() {
        GridGgfsConfiguration cfg = new GridGgfsConfiguration();

        cfg.setName(ggfsName);
        cfg.setBlockSize(ggfsBlockSize);
        cfg.setDataCacheName(ggfsDataCacheName);
        cfg.setMetaCacheName(ggfsMetaCacheName);

        return cfg;
    }

    /**
     * @return GGFS meta cache configuration.
     */
    public GridCacheConfiguration metaCacheConfiguration() {
        GridCacheConfiguration cfg = new GridCacheConfiguration();

        cfg.setName(ggfsMetaCacheName);
        cfg.setCacheMode(REPLICATED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return GGFS data cache configuration.
     */
    private GridCacheConfiguration dataCacheConfiguration() {
        GridCacheConfiguration cfg = new GridCacheConfiguration();

        cfg.setName(ggfsDataCacheName);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(ggfsBlockGroupSize));
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
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
