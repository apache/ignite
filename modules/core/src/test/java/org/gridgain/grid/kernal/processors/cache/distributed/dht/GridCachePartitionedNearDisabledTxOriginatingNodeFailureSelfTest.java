/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.cache.*;

/**
 * Tests transaction consistency when originating node fails.
 */
public class GridCachePartitionedNearDisabledTxOriginatingNodeFailureSelfTest
    extends GridCachePartitionedTxOriginatingNodeFailureSelfTest {

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return GridCacheDistributionMode.PARTITIONED_ONLY;
    }
}
