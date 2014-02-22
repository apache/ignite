// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.store;

import org.gridgain.examples.advanced.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.product.*;

import java.util.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * This example demonstrates JDBC and Hibernate-based implementations
 * of persistent store functionality in cache.
 * <p>
 * You can execute this example with or without remote nodes. If you start remote nodes make
 * sure you use the same config file as in example and all necessary classes are available
 * on classpath. You may use {@link GridCacheNodeStartup} to start remote nodes.
 * <p>
 * If you run this example from IDE, use support classes {@link GridCacheJdbcNodeStartup}
 * and {@link GridCacheHibernateNodeStartup} to start remote nodes - you'll have all classes
 * needed for the example compiled and put to node classpath.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridCacheStoreExample {
    /** Global person ID to use across entire example. */
    private static final UUID PERSON_ID = UUID.randomUUID();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        // To start grid with desired configuration uncomment the appropriate line.
        try (Grid g = GridGain.start("examples/config/example-cache-store-jdbc.xml")) {
//        try (Grid g = GridGain.start("examples/config/example-cache-store-hibernate.xml")) {
            commitExample();

            rollbackExample();
        }
    }

    /**
     * Store commit example.
     *
     * @throws GridException If commit example fails.
     */
    private static void commitExample() throws GridException {
        System.out.println(">>>");
        System.out.println(">>> Starting store commit example.");
        System.out.println(">>>");

        GridCache<UUID, Person> cache = GridGain.grid().cache(null);

        GridCacheTx tx = cache.txStart();

        try {
            Person val = cache.get(PERSON_ID);

            System.out.println("Read value: " + val);

            val = cache.put(PERSON_ID, person(PERSON_ID, "Isaac", "Newton", "Famous scientist"));

            System.out.println("Overwrote old value: " + val);

            val = cache.get(PERSON_ID);

            System.out.println("Read value: " + val);

            tx.commit();
        }
        finally {
            tx.end();
        }

        System.out.println("Read value after commit: " + cache.get(PERSON_ID));
    }

    /**
     * Store rollback example.
     *
     * @throws GridException If example fails.
     */
    private static void rollbackExample() throws GridException {
        System.out.println(">>>");
        System.out.println(">>> Starting store rollback example.");
        System.out.println(">>>");

        GridCache<UUID, Person> cache = GridGain.grid().cache(null);

        GridCacheTx tx = cache.txStart();

        try {
            Person val = cache.get(PERSON_ID);

            System.out.println("Read value: " + val);

            // Put doomed value to cache.
            val = cache.put(PERSON_ID, person(PERSON_ID, "James", "Maxwell", "Famous scientist"));

            System.out.println("Overwrote old value: " + val);

            val = cache.get(PERSON_ID);

            System.out.println("Read value: " + val);
        }
        finally {
            tx.end();
        }

        System.out.println("Key value after rollback: " + cache.get(PERSON_ID));
    }

    /**
     * Creates person.
     *
     * @param id ID.
     * @param firstName First name.
     * @param lastName Last name.
     * @param resume Resume.
     * @return Newly created person.
     */
    private static Person person(UUID id, String firstName, String lastName, String resume) {
        Person person = new Person(id);

        person.setFirstName(firstName);
        person.setLastName(lastName);
        person.setResume(resume);

        return person;
    }
}
