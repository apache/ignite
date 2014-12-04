/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query.reducefields;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.lang.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Reduce fields queries tests for partitioned cache.
 */
public class GridCacheReduceFieldsQueryPartitionedSelfTest extends GridCacheAbstractReduceFieldsQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncludeBackups() throws Exception {
        GridCacheQuery<List<?>> qry = grid(0).cache(null).queries().createSqlFieldsQuery("select age from Person");

        qry.includeBackups(true);

        int sum = 0;

        for (IgniteBiTuple<Integer, Integer> tuple : qry.execute(new AverageRemoteReducer()).get())
            sum += tuple.get1();

        // One backup, so sum is two times greater
        assertEquals("Sum", 200, sum);
    }
}
