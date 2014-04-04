/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.tests.p2p;

import org.gridgain.grid.cache.affinity.*;

/**
 * Test mapper for P2P class loading tests.
 */
public class GridExternalAffinityKeyMapper implements GridCacheAffinityKeyMapper {
    /** {@inheritDoc} */
    @Override public Object affinityKey(Object key) {
        if (key instanceof Integer)
            return 1 == (Integer)key ? key : 0;

        return key;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // This mapper is stateless and needs no initialization logic.
    }
}
