// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.advanced;

import org.gridgain.examples.advanced.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.events.GridEventType.*;
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
public class GridCacheAdvancedExample {
    /**
     * Put data to cache and then query it.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            print("Cache advanced API example started.");

            // Register remote event handlers.
            startConsumeEvents();

            // Uncomment any configured cache instance to observe
            // different cache behavior for different cache modes.
            GridCache<UUID, Object> cache = g.cache("partitioned");
            // GridCache<UUID, Object> cache = g.cache("replicated");
            // GridCache<UUID, Object> cache = g.cache("local");

            // Demonstrates concurrent-map-like operations on cache.
            concurrentMap(cache);

            // Demonstrates visitor-like operations on cache.
            visitors(cache);

            // Demonstrates iterations over cached collections.
            collections(cache);

            print("Cache advanced API example finished.");
        }
    }

    /**
     * This method will register listener for cache events on all nodes,
     * so we can actually see what happens underneath locally and remotely.
     *
     * @throws GridException If failed.
     */
    private static void startConsumeEvents() throws  GridException {
        GridGain.grid().events().consumeRemote(
            null,
            new GridPredicate<GridCacheEvent>() {
                @Override public boolean apply(GridCacheEvent e) {
                    // Make sure not to use any other classes that should be p2p-loaded.
                    System.out.println(e.shortDisplay());

                    return true;
                }
            },
            EVT_CACHE_OBJECT_PUT,
            EVT_CACHE_OBJECT_READ,
            EVT_CACHE_OBJECT_REMOVED).get();
    }

    /**
     * This method will unregister listener for cache events on all nodes. We must do this
     * because in SHARED deployment mode classes will be undeployed when all master nodes
     * leave grid, and listener notification may cause undefined behaviour.
     *
     * @param consumeId Consume ID returned from
     *      {@link GridEvents#consumeRemote(GridBiPredicate, GridPredicate, int...)} method.
     * @throws GridException If failed.
     */
    private static void stopConsumeEvents(UUID consumeId) throws  GridException {
        GridGain.grid().events().stopConsume(consumeId).get();
    }

    /**
     * Demonstrates cache operations similar to {@link ConcurrentMap} API. Note that
     * cache API is a lot richer than the JDK {@link ConcurrentMap} as it supports
     * functional programming and asynchronous mode.
     *
     * @param cache Cache to use.
     * @throws GridException If failed.
     */
    private static void concurrentMap(GridCacheProjection<UUID,Object> cache) throws GridException {
        System.out.println(">>>");
        System.out.println(">>> ConcurrentMap Example.");
        System.out.println(">>>");

        // Organizations.
        Organization org1 = new Organization("GridGain");
        Organization org2 = new Organization("Other");

        // People.
        final Person p1 = new Person(org1, "Jon", "Doe", 1000, "I have a 'Master Degree'");
        Person p2 = new Person(org2, "Jane", "Doe", 2000, "I have a 'Master Degree'");
        Person p3 = new Person(org1, "Jon", "Smith", 3000, "I have a 'Bachelor Degree'");
        Person p4 = new Person(org1, "Tom", "White", 4000, "I have a 'Bachelor Degree'");

        /*
         * Convenience projections for type-safe cache views.
         */
        GridCacheProjection<UUID, Organization> orgCache = cache.projection(UUID.class, Organization.class);
        GridCacheProjection<UUID, Person> peopleCache = cache.projection(UUID.class, Person.class);

        /*
         * Basic put.
         */
        orgCache.put(org1.getId(), org1);
        orgCache.put(org2.getId(), org2);

        /*
         * PutIfAbsent.
         */
        Person prev = peopleCache.putIfAbsent(p1.getId(), p1);

        assert prev == null;

        boolean ok = peopleCache.putxIfAbsent(p1.getId(), p1);

        assert !ok; // Second putIfAbsent for p1 must not go through.

        /*
         * Asynchronous putIfAbsent
         */
        GridFuture<Boolean> fut2 = peopleCache.putxIfAbsentAsync(p2.getId(), p2);
        GridFuture<Boolean> fut3 = peopleCache.putxIfAbsentAsync(p3.getId(), p3);

        // Both asynchronous putIfAbsent should be successful.
        assert fut2.get();
        assert fut3.get();

        /*
         * Replace operations.
         */

        // Replace p1 with p2 only if p4 is present in cache.
        Person p = peopleCache.replace(p1.getId(), p4);

        assert p!= null && p.equals(p1);

        // Put p1 back.
        ok = peopleCache.replace(p1.getId(), p4, p1);

        assert ok;

        /**
         * Remove operation that matches both, key and value.
         * This method is not available on projection, so we
         * call cache directly.
         */
        ok = peopleCache.cache().remove(p3.getId(), p3);

        assert ok;

        // Make sure that remove succeeded.
        assert peopleCache.peek(p3.getId()) == null;

        /*
         * Put operation with a filter.
         */
        ok = peopleCache.putx(p3.getId(), p3, new GridPredicate<GridCacheEntry<UUID, Person>>() {
            @Override public boolean apply(GridCacheEntry<UUID, Person> e) {
                return e.peek() == null; // Only put if currently no value (this should succeed).
            }
        });

        assert ok;

        print("Finished concurrentMap operations example on cache.");
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
