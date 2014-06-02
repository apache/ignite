/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.cache.*;

/**
 * Tests {@link GridCacheInterceptor}.
 */
public class GridCacheInterceptorNearEnabledSelfTest extends GridCacheInterceptorSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return GridCacheDistributionMode.NEAR_PARTITIONED;
    }
}
