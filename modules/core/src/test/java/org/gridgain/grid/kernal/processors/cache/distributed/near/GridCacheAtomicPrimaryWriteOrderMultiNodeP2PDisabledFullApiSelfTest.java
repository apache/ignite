/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.cache.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;

/**
 * Multi-node tests for partitioned cache with primary write order.
 */
public class GridCacheAtomicPrimaryWriteOrderMultiNodeP2PDisabledFullApiSelfTest extends
        GridCacheAtomicMultiNodeP2PDisabledFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return PRIMARY;
    }
}
