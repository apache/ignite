/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.store;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;

import java.util.*;

/**
 * Demonstrates usage of cache with underlying persistent store configured.
 * <p>
 * Remote nodes should always be started using {@link CacheNodeWithStoreStartup}.
 * Also you can change type of underlying store modifying configuration in the
 * {@link CacheNodeWithStoreStartup#configure()} method.
 */
public class CacheStoreExample {
    /** Global person ID to use across entire example. */
    private static final Long id = Math.abs(UUID.randomUUID().getLeastSignificantBits());

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        IgniteConfiguration cfg = CacheNodeWithStoreStartup.configure();

        // To start grid with desired configuration uncomment the appropriate line.
        try (Ignite g = Ignition.start(cfg)) {
            System.out.println();
            System.out.println(">>> Cache store example started.");

            GridCache<Long, Person> cache = g.cache(null);

            // Clean up caches on all nodes before run.
            cache.globalClearAll(0);

            try (GridCacheTx tx = cache.txStart()) {
                Person val = cache.get(id);

                System.out.println("Read value: " + val);

                val = cache.put(id, person(id, "Isaac", "Newton"));

                System.out.println("Overwrote old value: " + val);

                val = cache.get(id);

                System.out.println("Read value: " + val);

                tx.commit();
            }

            System.out.println("Read value after commit: " + cache.get(id));
        }
    }

    /**
     * Creates person.
     *
     * @param id ID.
     * @param firstName First name.
     * @param lastName Last name.
     * @return Newly created person.
     */
    private static Person person(long id, String firstName, String lastName) {
        return new Person(id, firstName, lastName);
    }
}
