/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 *
 */
public class GridCacheReplicatedTxPessimisticOriginatingNodeFailureSelfTest extends
    IgniteTxPessimisticOriginatingNodeFailureAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected Collection<Class<?>> ignoreMessageClasses() {
        return Collections.<Class<?>>singletonList(GridDistributedTxFinishRequest.class);
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
    }
}
