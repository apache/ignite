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
import org.gridgain.grid.cache.query.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
public class GridCacheQueryIndexingDisabledSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setCacheMode(GridCacheMode.PARTITIONED);
        ccfg.setQueryIndexEnabled(false);

        return ccfg;
    }

    /**
     * @param c Closure.
     */
    private void doTest(Callable<Object> c) {
        GridTestUtils.assertThrows(log, c, GridException.class, "Indexing is disabled for cache: null");
    }

    /**
     * @throws GridException If failed.
     */
    public void testSqlFieldsQuery() throws GridException {
        doTest(new Callable<Object>() {
            @Override public Object call() throws GridException {
                return cache().queries().createSqlFieldsQuery("select * from dual").execute()
                    .get();
            }
        });
    }

    /**
     * @throws GridException If failed.
     */
    public void testTextQuery() throws GridException {
        doTest(new Callable<Object>() {
            @Override public Object call() throws GridException {
                return cache().queries().createFullTextQuery(String.class, "text")
                    .execute().get();
            }
        });
    }

    /**
     * @throws GridException If failed.
     */
    public void testSqlQuery() throws GridException {
        doTest(new Callable<Object>() {
            @Override public Object call() throws GridException {
                return cache().queries().createSqlQuery(String.class, "1 = 1")
                    .execute().get();
            }
        });
    }

    /**
     * @throws GridException If failed.
     */
    public void testScanQuery() throws GridException {
        GridCacheQuery<Map.Entry<String, Integer>> qry = cache().queries().createScanQuery(null);

        qry.execute().get();
    }
}
