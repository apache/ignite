/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

/**
 * Tests for byte array values in REPLICATED caches with P2P enabled.
 */
public class GridCacheReplicatedP2PEnabledByteArrayValuesSelfTest extends
    GridCacheAbstractReplicatedByteArrayValuesSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean peerClassLoading() {
        return true;
    }
}
