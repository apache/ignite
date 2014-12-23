/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 *
 */
public class IgniteTxReentryColocatedSelfTest extends IgniteTxReentryAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int testKey() {
        int key = 0;

        GridCache<Object, Object> cache = grid(0).cache(null);

        while (true) {
            Collection<ClusterNode> nodes = cache.affinity().mapKeyToPrimaryAndBackups(key);

            if (nodes.contains(grid(0).localNode()))
                key++;
            else
                break;
        }

        return key;
    }

    /** {@inheritDoc} */
    @Override protected int expectedNearLockRequests() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int expectedDhtLockRequests() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected int expectedDistributedLockRequests() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected boolean nearEnabled() {
        return false;
    }
}
