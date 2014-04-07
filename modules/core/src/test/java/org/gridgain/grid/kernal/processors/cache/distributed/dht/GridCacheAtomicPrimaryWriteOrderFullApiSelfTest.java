/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.cache.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;

/**
 * Test for atomic cache with primary write order mode.
 */
public class GridCacheAtomicPrimaryWriteOrderFullApiSelfTest extends GridCacheAtomicFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return PRIMARY;
    }
}
