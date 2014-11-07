/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

/**
 * Tests for byte array values in NEAR-PARTITIONED caches with P2P disabled.
 */
public class GridCacheNearPartitionedP2PDisabledByteArrayValuesSelfTest extends
    GridCacheAbstractNearPartitionedByteArrayValuesSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean peerClassLoading() {
        return false;
    }
}
