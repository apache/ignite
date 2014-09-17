/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

/**
 * TODO
 */
public class GridCacheEvictNearEnabledTest extends GridCommonAbstractTest {
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = loadConfiguration(
            "modules/clients/src/test/dotnet/client-lib-test/config/native-client-test-cache.xml");

        cfg.setGridName(gridName);
        cfg.setGridLogger(log().getLogger(getClass()));

        return cfg;
    }

    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(3);
    }

    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvict() throws Exception {
        Grid grid = grid(0);

        GridCache<Integer, Integer> cache = grid.cache("partitioned_near");

        int key = 0;

        while (true) {
            if (cache.affinity().isPrimary(grid.localNode(), key))
                break;

            key++;
        }

        cache.putx(key, 1);

        assert cache.peek(key) == 1;

        assert cache.evict(key);

        U.debug("evicted");

        assert cache.isEmpty() : cache.entrySet();
    }
}
