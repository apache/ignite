/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Tests for fields queries.
 */
public class GridCacheReplicatedFieldsQuerySelfTest extends GridCacheAbstractFieldsQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeLeft() throws Exception {
        hasCache = true;

        try {
            final Map<UUID, Map<Long, GridFutureAdapter<GridIndexingFieldsResult>>> map =
                U.field(((GridKernal)grid(0)).internalCache().context().queries(), "fieldsQryRes");

            // Ensure that iterators map empty.
            map.clear();

            Ignite g = startGrid();

            GridCache<Integer, Integer> cache = g.cache(null);

            GridCacheQuery<List<?>> q = cache.queries().createSqlFieldsQuery("select _key from Integer where _key >= " +
                "0 order by _key");

            q.pageSize(50);

            ClusterGroup prj = g.cluster().forNodes(Arrays.asList(g.cluster().localNode(), grid(0).localNode()));

            q = q.projection(prj);

            GridCacheQueryFuture<List<?>> fut = q.execute();

            assertEquals(0, fut.next().get(0));

            // fut.nextX() does not guarantee the request has completed on remote node
            // (we could receive page from local one), so we need to wait.
            assertTrue(GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return map.size() == 1;
                }
            }, getTestTimeout()));

            Map<Long, GridFutureAdapter<GridIndexingFieldsResult>> futs = map.get(g.cluster().localNode().id());

            assertEquals(1, futs.size());

            final UUID nodeId = g.cluster().localNode().id();
            final CountDownLatch latch = new CountDownLatch(1);

            grid(0).events().localListen(new IgnitePredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    if (((GridDiscoveryEvent) evt).eventNode().id().equals(nodeId))
                        latch.countDown();

                    return true;
                }
            }, EVT_NODE_LEFT);

            stopGrid();

            latch.await();

            assertEquals(0, map.size());
        }
        finally {
            // Ensure that additional node is stopped.
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLostIterator() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        GridCacheQueryFuture<List<?>> fut = null;

        for (int i = 0; i < cache.configuration().getMaximumQueryIteratorCount() + 1; i++) {
            GridCacheQuery<List<?>> q = cache.queries().createSqlFieldsQuery(
                "select _key from Integer where _key >= 0 order by _key").projection(grid(0));

            q.pageSize(50);

            GridCacheQueryFuture<List<?>> f = q.execute();

            assertEquals(0, f.next().get(0));

            if (fut == null)
                fut = f;
        }

        final GridCacheQueryFuture<List<?>> fut0 = fut;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                int i = 0;

                List<?> next;

                while ((next = fut0.next()) != null)
                    assertEquals(++i % 50, next.get(0));

                return null;
            }
        }, GridRuntimeException.class, null);
    }
}
