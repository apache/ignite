/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.dr.cache.receiver.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 *
 */
public class GridDrCachePartitionedNearDisabledFullApiSelfTest extends GridCachePartitionedNearDisabledFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataCenterId((byte)1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cc = super.cacheConfiguration(gridName);

        cc.setDrReceiverConfiguration(new GridDrReceiverCacheConfiguration());
        cc.setDistributionMode(PARTITIONED_ONLY);

        return cc;
    }
}
