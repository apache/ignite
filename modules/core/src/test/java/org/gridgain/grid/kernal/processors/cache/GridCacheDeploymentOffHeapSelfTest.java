/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.cache.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMemoryMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Tests deployment with off-heap storage.
 */
public class GridCacheDeploymentOffHeapSelfTest extends GridCacheDeploymentSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration() throws Exception {
        GridCacheConfiguration cacheCfg = super.cacheConfiguration();

        cacheCfg.setQueryIndexEnabled(false);
        cacheCfg.setMemoryMode(OFFHEAP_VALUES);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setDistributionMode(NEAR_PARTITIONED);

        return cacheCfg;
    }
}
