/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Tests cache with near cache disabled.
 */
public class GridCachePartitionedOnlyProjectionSelfTest extends GridCachePartitionedProjectionSelfTest {
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }
}
