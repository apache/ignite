/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.rendezvous.*;

/**
 * Multi-node tests for replicated cache with {@link GridCacheRendezvousAffinityFunction}.
 */
public class GridCacheReplicatedRendezvousAffinityMultiNodeFullApiSelfTest extends GridCacheReplicatedMultiNodeFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setAffinity(new GridCacheRendezvousAffinityFunction());

        return cfg;
    }
}
