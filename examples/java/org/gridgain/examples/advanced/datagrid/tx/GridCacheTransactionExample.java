// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.tx;

import org.gridgain.examples.advanced.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.product.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * This example demonstrates some examples of how to use cache transactions.
 * You can execute this example with or without remote nodes.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridCacheTransactionExample {
    // Typedefs:
    // ---------
    // G -> GridFactory
    // CIX1 -> GridInClosureX
    // P2 -> GridPredicate2

    /** Cache. */
    private static GridCache<UUID, Object> cache;

    /**
     * Puts data to cache and then queries them.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            // Uncomment any configured cache instance to observe
            // different cache behavior for different cache modes.
            cache = g.cache("partitioned_tx");
//            cache = g.cache("replicated");
//            cache = g.cache("local");

            print("Cache transaction example started.");

            Organization org = new Organization("GridGain");
            Person p = new Person(org, "Jon", "Doe", 1000, "I have a 'Master Degree'");

            GridCacheTx tx = cache.txStart(OPTIMISTIC, REPEATABLE_READ);

            try {
                assert cache.get(org.getId()) == null;
                assert cache.get(p.getId()) == null;

                cache.put(org.getId(), org);
                cache.put(p.getId(), p);

                // Since transaction is started with REPEATABLE_READ isolation,
                // 'get()' operation within transaction will return the same values
                // as were 'put' within the same transaction.
                assert cache.get(org.getId()) == org;
                assert cache.get(p.getId()) == p;

                // Commit transaction.
                tx.commit();

                // Get values from cache outside transaction and make sure that they are stored.
                assert org.equals(cache.get(org.getId()));
                assert p.equals(cache.get(p.getId()));
            }
            finally {
                // This call will rollback transaction
                // if it has not committed yet.
                tx.end();
            }

            print("Cache transaction example finished");
        }
    }

    /**
     * Prints out given object to standard out.
     *
     * @param o Object to print.
     */
    private static void print(Object o) {
        System.out.println(">>> " + o);
    }
}
