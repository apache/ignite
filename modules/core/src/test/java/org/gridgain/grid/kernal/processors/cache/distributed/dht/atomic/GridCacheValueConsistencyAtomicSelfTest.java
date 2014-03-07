/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;

/**
 * Tests cache value consistency for ATOMIC mode.
 */
public class GridCacheValueConsistencyAtomicSelfTest extends GridCacheValueConsistencyAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheAtomicityMode atomicityMode() {
        return GridCacheAtomicityMode.ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected int iterationCount() {
        return 400_000;
    }
}
