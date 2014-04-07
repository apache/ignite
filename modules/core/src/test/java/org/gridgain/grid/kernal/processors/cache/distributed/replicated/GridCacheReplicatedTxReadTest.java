/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.gridgain.grid.cache.GridCacheMode;
import org.gridgain.grid.kernal.processors.cache.GridCacheAbstractTxReadTest;

/**
 * Checks transactional reads for local cache.
 */
public class GridCacheReplicatedTxReadTest extends GridCacheAbstractTxReadTest {
    /**
     * @return {@code LOCAL} for this test.
     */
    @Override protected GridCacheMode cacheMode() {
        return GridCacheMode.REPLICATED;
    }
}
