/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures.local;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;

/**
 * Queue tests with local atomic cache.
 */
public class GridCacheLocalAtomicQueueApiSelfTest extends GridCacheLocalQueueApiSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        GridCacheConfiguration ccfg = cfg.getCacheConfiguration()[0];

        ccfg.setAtomicityMode(ATOMIC);

        return cfg;
    }
}
