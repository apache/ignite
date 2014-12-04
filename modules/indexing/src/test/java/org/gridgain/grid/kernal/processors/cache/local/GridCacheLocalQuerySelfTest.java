/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.local;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests local query.
 */
public class GridCacheLocalQuerySelfTest extends GridCacheAbstractQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return LOCAL;
    }

    /**
     * @throws GridException If test failed.
     */
    public void testQueryLocal() throws Exception {
        GridCache<Integer, String> cache = ignite.cache(null);

        cache.put(1, "value1");
        cache.put(2, "value2");
        cache.put(3, "value3");
        cache.put(4, "value4");
        cache.put(5, "value5");

        // Tests equals query.
        GridCacheQuery<Map.Entry<Integer, String>> qry = cache.queries().createSqlQuery(String.class, "_val='value1'");

        GridCacheQueryFuture<Map.Entry<Integer,String>> iter = qry.execute();

        Map.Entry<Integer, String> entry = iter.next();

        assert iter.next() == null;

        assert entry != null;
        assert entry.getKey() == 1;
        assert "value1".equals(entry.getValue());

        // Tests like query.
        qry = cache.queries().createSqlQuery(String.class, "_val like 'value%'");

        iter = qry.execute();

        assert iter.next() != null;
        assert iter.next() != null;
        assert iter.next() != null;
        assert iter.next() != null;
        assert iter.next() != null;
        assert iter.next() == null;

        // Tests reducer.
        GridCacheQuery<Map.Entry<Integer, String>> rdcQry = cache.queries().createSqlQuery(String.class,
            "_val like 'value%' and _key != 2 and _val != 'value3' order by _val");

        Iterator<String> iter2 = rdcQry.
            projection(ignite.cluster().forLocal()).
            execute(new IgniteReducer<Map.Entry<Integer, String>, String>() {
                /** */
                private String res = "";

                @Override public boolean collect(Map.Entry<Integer, String> e) {
                    res += e.getValue();

                    return true;
                }

                @Override public String reduce() {
                    return res;
                }
            }).get().iterator();

        String res = iter2.next();

        assert res != null;
        assert "value1value4value5".equals(res);
    }
}
