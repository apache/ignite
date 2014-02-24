// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.basic.datagrid;

import org.gridgain.examples.advanced.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.lang.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * This example demonstrates some of the cache rich API capabilities.
 * You can execute this example with or without remote nodes.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridCacheApiExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";
    //private static final String CACHE_NAME = "replicated";
    //private static final String CACHE_NAME = "local";

    /**
     * Put data to cache and then query it.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            print("Cache  API example started.");

            GridCache<UUID, Object> cache = g.cache(CACHE_NAME);

            // Demonstrates concurrent-map-like operations on cache.
            concurrentMapApi();

            // Demonstrates visitor-like operations on cache.
            visitors(cache);

            // Demonstrates iterations over cached collections.
            collections(cache);

            print("Cache advanced API example finished.");
        }
    }

    /**
     * Demonstrates cache operations similar to {@link ConcurrentMap} API. Note that
     * cache API is a lot richer than the JDK {@link ConcurrentMap}.
     *
     * @throws GridException If failed.
     */
    private static void concurrentMapApi() throws GridException {
        GridCache<Integer, String> cache = GridGain.grid().cache(CACHE_NAME);

        // Put and return previous value.
        cache.put(1, "1");

        // Put and do not return previous value. All methods ending with 'x' behave this way.
        // Performs better when previous value is not needed.
        cache.putx(2, "2");

        // Put asynchronously.
        // Every cache operation has async counterpart.
        GridFuture<String> fut = cache.putAsync(3, "3");

        fut.listenAsync(new GridInClosure<GridFuture<String>>() {
            @Override public void apply(GridFuture<String> fut) {
                try {
                    System.out.println("Put operation completed [previous-value=" + fut.get() + ']');
                }
                catch (GridException e) {
                    throw new GridClosureException(e);
                }
            }
        });

        // Put-if-absent.
        boolean b1 = cache.putxIfAbsent(4, "4");
        boolean b2 = cache.putxIfAbsent(4, "44");
        assert b1 && !b2;


        // Put-with-predicate, will succeed if predicate evaluates to true.
        cache.putx(5, "5");
        cache.putx(5, "55", new GridPredicate<GridCacheEntry<Integer, String>>() {
            @Override public boolean apply(GridCacheEntry<Integer, String> e) {
                return "5".equals(e.peek()); // Update only if previous value is "5".
            }
        });

        // Transform - assign new value based on previous value.
        cache.putx(6, "6");
        cache.transform(6, new GridClosure<String, String>() {
            @Override public String apply(String v) {
                return v + "6"; // Set new value based on previous value.
            }
        });

        // Replace.
        cache.putx(7, "7");
        b1 = cache.replace(7, "7", "77");
        b2 = cache.replace(7, "7", "777");
        assert b1 & !b2;
    }

    /**
     * Demonstrates visitor operations on cache, in particular {@code forEach(..)},
     * {@code forAll(..)} and {@code reduce(..)} methods.
     *
     * @param cache Cache to use.
     */
    private static void visitors(GridCacheProjection<UUID, Object> cache) {
        System.out.println(">>>");
        System.out.println(">>> Visitors Example.");
        System.out.println(">>>");

        // We only care about Person objects, therefore,
        // let's get projection to filter only Person instances.
        GridCacheProjection<UUID, Person> people = cache.projection(UUID.class, Person.class);

        // Visit by IDs.
        people.forEach(new GridInClosure<GridCacheEntry<UUID,Person>>() {
            @Override public void apply(GridCacheEntry<UUID, Person> e) {
                print("Visited forEach person: " + e);
            }
        });

        print("Finished cache visitor operations.");
    }

    /**
     * Demonstrates collection operations on cache.
     *
     * @param cache Cache to use.
     */
    private static void collections(GridCacheProjection<UUID, Object> cache) {
        System.out.println(">>>");
        System.out.println(">>> Collections Example.");
        System.out.println(">>>");

        // We only care about Person objects, therefore,
        // let's get projection to filter only Person instances.
        GridCacheProjection<UUID, Person> people = cache.projection(UUID.class, Person.class);

        // Iterate only over keys of people with name "Jon".
        // Iteration includes only local keys.
        for (UUID id : people.projection(new GridPredicate<GridCacheEntry<UUID, Person>>() {
            @Override public boolean apply(GridCacheEntry<UUID, Person> e) {
                Person p = e.peek();

                return p != null && "Jon".equals(p.getFirstName());
            }
        }).keySet()) {
            // Print out keys.
            print("Cached ID for person named 'Jon' from keySet: " + id);
        }

        // Delete all people with name "Jane".
        Collection<Person> janes = people.projection(new GridPredicate<GridCacheEntry<UUID, Person>>() {
            @Override public boolean apply(GridCacheEntry<UUID, Person> e) {
                Person p = e.peek();

                return p != null && "Jane".equals(p.getFirstName());
            }
        }).values();

        if (!janes.isEmpty()) {
            assert janes.size() == 1 : "Incorrect 'Janes' size: " + janes.size();

            int cnt = 0;

            for (Iterator<Person> it = janes.iterator(); it.hasNext(); ) {
                Person p = it.next();

                // Make sure that we are deleting "Jane".
                assert "Jane".equals(p.getFirstName()) : "First name is not 'Jane': " + p.getFirstName();

                // Remove all Janes.
                it.remove();

                cnt++;

                print("Removed Jane from cache: " + p);
            }

            assert cnt == 1;

            // Make sure that no Jane is present in cache.
            for (Person p : people.values()) {
                // Person cannot be "Jane".
                assert !"Jane".equals(p.getFirstName());

                print("Person still present in cache: " + p);
            }
        }
        else
            print("Jane's entry does not belong to local node.");

        print("Finished collection operations on cache.");
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
