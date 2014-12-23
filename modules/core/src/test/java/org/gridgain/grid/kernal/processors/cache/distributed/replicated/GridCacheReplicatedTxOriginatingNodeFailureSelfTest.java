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

/**
 * Tests originating node failure in REPLICATED cache.
 */
public class GridCacheReplicatedTxOriginatingNodeFailureSelfTest extends
    IgniteTxOriginatingNodeFailureAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return GridCacheMode.REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected Class<?> ignoreMessageClass() {
        return GridDistributedTxPrepareRequest.class;
    }
}
