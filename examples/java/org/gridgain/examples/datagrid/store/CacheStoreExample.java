// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.store;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

import java.util.*;

/**
 * Demonstrates usage of cache with underlying persistent store configured.
 * <p>
 * You should not be using stand-alone nodes (started with {@code 'ggstart.sh})
 * because GridGain nodes do not know about the cache stores
 * we define in this example. However, users can always add JAR-files with their classes to
 * {@code GRIDGAIN_HOME/libs/ext} folder to make them available to GridGain.
 * If this was done here (i.e. we had JAR-file containing custom cache store built
 * and put to {@code GRIDGAIN_HOME/libs/ext} folder), we could easily startup
 * remote nodes with {@code 'ggstart.sh examples/config/example-cache-storeloader.xml'}
 * command.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheStoreExample {
    /** Global person ID to use across entire example. */
    private static final UUID id = UUID.randomUUID();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        GridConfiguration cfg = CacheNodeWithStoreStartup.configure();

        // To start grid with desired configuration uncomment the appropriate line.
        try (Grid g = GridGain.start(cfg)) {
            System.out.println();
            System.out.println(">>> Started store example.");

            GridCache<UUID, Person> cache = GridGain.grid().cache(null);

            try (GridCacheTx tx = cache.txStart()) {
                Person val = cache.get(id);

                System.out.println("Read value: " + val);

                val = cache.put(id, person(1, "Isaac", "Newton"));

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
