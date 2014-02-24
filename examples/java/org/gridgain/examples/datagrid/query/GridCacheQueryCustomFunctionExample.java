// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.product.*;

import java.util.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Grid cache query custom function example. Demonstrates usage of custom java functions from SQL queries.
 * <p>
 * Remote nodes for this example should always be started using {@link GridCacheQueryCustomFunctionNodeStartup} class.
 * Otherwise the custom function class should be put on class path of the started nodes.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridCacheQueryCustomFunctionExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Puts data to cache and then queries them using custom functions in SQL.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("examples/config/example-cache-custom-functions.xml")) {
            print("Custom SQL function example started.");

            GridCache<Integer, String> cache = g.cache(CACHE_NAME);

            cache.put(8, "8");
            cache.put(4, "4");
            cache.put(30, "1d");
            cache.put(23, "17");
            cache.put(17, "11");
            cache.put(9, "9");

            GridCacheQuery<Integer, String> qry = cache.queries().createQuery(GridCacheQueryType.SQL, String.class,
                "to_hex(_key) <> _val");

            GridCacheQueryFuture<Map.Entry<Integer, String>> res = qry.execute();

            for (Map.Entry<Integer, String> entry : res.get())
                print("Hex value  '" + entry.getValue() + "' is not equal to key " + entry.getKey());


            GridFuture<List<Object>> res1 = cache.queries()
                .createFieldsQuery("select to_hex(sum(from_hex(_val))) from String").
                executeSingle();

            print("Hex sum of all hex values is '" + res1.get().get(0) + "'");
        }

        print("Custom SQL function example finished.");
    }

    /**
     * Prints given string.
     *
     * @param str String.
     */
    private static void print(String str) {
        System.out.println(">>> " + str);
    }

    /**
     * Converts hexadecimal value to integer. This method can be used in SQL queries.
     *
     * @param hex Hexadecimal value.
     * @return Integer representation of given hex value.
     */
    @GridCacheQuerySqlFunction(alias = "from_hex", deterministic = true)
    public static int fromHex(String hex) {
        return Integer.parseInt(hex, 16);
    }

    /**
     * Converts integer value to hexadecimal string.
     * <p>
     * This is the custom function that will be used in SQL queries.
     *
     * @param i Integer.
     * @return Hexadecimal representation of given integer value.
     */
    @GridCacheQuerySqlFunction(alias = "to_hex", deterministic = true)
    public static String toHex(int i) {
        return Integer.toHexString(i);
    }
}
