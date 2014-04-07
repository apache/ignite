/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

/**
 * Tests for byte array values in PARTITIONED-ONLY caches with P2P disabled.
 */
public class GridCachePartitionedOnlyP2PDisabledByteArrayValuesSelfTest extends
    GridCacheAbstractPartitionedOnlyByteArrayValuesSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean peerClassLoading() {
        return false;
    }
}
