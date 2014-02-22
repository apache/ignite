// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.hibernate;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.hibernate.*;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.*;
import org.hibernate.criterion.*;
import org.hibernate.service.*;
import org.hibernate.stat.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * This example demonstrates the use of GridGain In-Memory Data Grid as a Hibernate
 * Second-Level cache provider.
 * <p>
 * The Hibernate Second-Level cache (or "L2 cache" shortly) lets you significantly
 * reduce the number of requests to the underlying SQL database. Because database
 * access is known to be an expansive operation, using L2 cache may improve
 * performance dramatically.
 * <p>
 * This example defines 2 entity classes: {@link User} and {@link Post}, with
 * 1 <-> N relation, and marks them with appropriate annotations for Hibernate
 * object-relational mapping to SQL tables of an underlying H2 in-memory database.
 * The example launches GridGain node in the same JVM and registers it in
 * Hibernate configuration as an L2 cache implementation. It then stores and
 * queries instances of the entity classes to and from the database, having
 * Hibernate SQL output, L2 cache statistics output, and GridGain cache metrics
 * output enabled.
 * <p>
 * When running example, it's easy to notice that when an object is first
 * put into a database, the L2 cache is not used and it's contents is empty.
 * However, when an object is first read from the database, it is immediately
 * stored in L2 cache (which is GridGain In-Memory Data Grid in fact), which can
 * be seen in stats output. Further requests of the same object only read the data
 * from L2 cache and do not hit the database.
 * <p>
 * In this example, the Hibernate query cache is also enabled. Query cache lets you
 * avoid hitting the database in case of repetitive queries with the same parameter
 * values. You may notice that when the example runs the same query repeatedly in
 * loop, only the first query hits the database and the successive requests take the
 * data from L2 cache.
 * <p>
 * Note: this example uses {@link AccessType#READ_ONLY} L2 cache access type, but you
 * can experiment with other access types by modifying the Hibernate configuration file
 * {@code GRIDGAIN_HOME/examples/config/hibernate.xml}, used by the example.
 * <p>
 * Note: you may also start additional standalone GridGain nodes for this example, in
 * which case the L2 cache data will be split between the nodes
 * ({@link GridCacheMode#PARTITIONED} cache mode). To launch a standalone node, open
 * the console, change the current directory to {@code GRIDGAIN_HOME/bin}, and run the
 * command below.
 * <br>
 * Linux:
 * <pre><code>
 * $ ./ggstart.sh examples/config/example-cache-hibernate.xml
 * </code></pre>
 * Windows:
 * <pre><code>
 * > ggstart.bat examples/config/example-cache-hibernate.xml
 * </code></pre>
 *
 * @author @java.author
 * @version @java.version
 */
public class GridHibernateL2CacheExample {
    /** JDBC URL for backing database (an H2 in-memory database is used). */
    private static final String JDBC_URL = "jdbc:h2:mem:example;DB_CLOSE_DELAY=-1";

    /** Entity names for stats output. */
    private static final List<String> ENTITY_NAMES =
        Arrays.asList(User.class.getName(), Post.class.getName(), User.class.getName() + ".posts");

    /**
     * Main method, that starts this example.
     *
     * @param args Command line arguments (not used).
     * @throws GridException If GridGain node startup failed.
     */
    public static void main(String[] args) throws GridException {
        // Start the GridGain node, run the example, and stop the node when finished.
        try (Grid grid = GridGain.start("examples/config/example-cache-hibernate.xml")) {
            // We use a single session factory, but create a dedicated session
            // for each transaction or query. This way we ensure that L1 cache
            // is not used (L1 cache has per-session scope only).
            SessionFactory sesFactory = createHibernateSessionFactory();

            System.out.println(">>>\n>>> Creating objects.\n>>>");

            final long userId;

            Session ses = sesFactory.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                User user = new User("jedi", "Luke", "Skywalker");

                user.getPosts().add(new Post(user, "Let the Force be with you."));

                ses.save(user);

                user = new User("tdurden", "Tyler", "Durden");

                user.getPosts().add(new Post(user, "The things you own end up owning you."));

                ses.save(user);

                user = new User("jsmith", "John", "Smith");

                user.getPosts().add(new Post(user, "Never send a human to do a machine's job."));

                ses.save(user);

                tx.commit();

                // Create a user object, store it in DB, and save the database-generated
                // object ID. You may try adding more objects in a similar way.
                userId = user.getId();
            }
            finally {
                ses.close();
            }

            // Output L2 cache and GridGain cache stats. You may notice that
            // at this point the object is not yet stored in L2 cache, because
            // the read was not yet performed.
            printStats(sesFactory);
            printStats(grid);

            System.out.println(">>>\n>>> Querying object by ID.\n>>>");

            // Query user by ID several times. First time we get an L2 cache
            // miss, and the data is queried from DB, but it is then stored
            // in cache and successive queries hit the cache and return
            // immediately, no SQL query is made.
            for (int i = 0; i < 3; i++) {
                ses = sesFactory.openSession();

                try {

                    Transaction tx = ses.beginTransaction();

                    User user = (User)ses.get(User.class, userId);

                    System.out.println("User: " + user);

                    for (Post post : user.getPosts())
                        System.out.println("\tPost: " + post);

                    tx.commit();
                }
                finally {
                    ses.close();
                }
            }

            // Output the stats. We should see 1 miss and 2 hits for
            // User and Collection object (stored separately in L2 cache).
            // The Post is loaded with the collection, so it won't imply
            // a miss.
            printStats(sesFactory);
            printStats(grid);

            System.out.println(">>>\n>>> Querying all User objects.\n>>>");

            // From here on, we run several criteria queries repeatedly to
            // demonstrate how query cache works. Again, in each case, the
            // first query will go to the database, but the successive
            // queries will only fetch data from L2 cache, leaving the
            // database intact.
            for (int i = 0; i < 3; i++) {
                List<User> users = runQuery(sesFactory, User.class, null);

                System.out.println("Users: " + users);
            }

            // Output the stats.
            printStats(sesFactory);
            printStats(grid);

            System.out.println(">>>\n>>> Querying User objects by lastName.\n>>>");

            // Query users by last name several times.
            for (int i = 0; i < 3; i++) {
                List<User> users = runQuery(sesFactory, User.class, new GridClosure<Criteria, Criteria>() {
                    @Override public Criteria apply(Criteria c) {
                        return c.add(Restrictions.eq("lastName", "Smith"));
                    }
                });

                System.out.println("Users by last name: " + users);
            }

            // Output the stats.
            printStats(sesFactory);
            printStats(grid);

            System.out.println(">>>\n>>> Querying all Post objects.\n>>>");

            // Query all posts several times.
            for (int i = 0; i < 3; i++) {
                List<Post> posts = runQuery(sesFactory, Post.class, null);

                System.out.println("Posts: " + posts);
            }

            // Output the stats.
            printStats(sesFactory);
            printStats(grid);

            System.out.println(">>>\n>>> Querying Post objects by author ID.\n>>>");

            // Query posts by author ID several times.
            for (int i = 0; i < 3; i++) {
                List<Post> posts = runQuery(sesFactory, Post.class, new GridClosure<Criteria, Criteria>() {
                    @Override public Criteria apply(Criteria c) {
                        return c.createCriteria("author").add(Restrictions.eq("id", userId));
                    }
                });

                System.out.println("Posts by author ID: " + posts);
            }

            // Output the stats.
            printStats(sesFactory);
            printStats(grid);
        }
    }

    /**
     * Creates a new Hibernate {@link SessionFactory} using a programmatic
     * configuration.
     *
     * @return New Hibernate {@link SessionFactory}.
     */
    private static SessionFactory createHibernateSessionFactory() {
        ServiceRegistryBuilder builder = new ServiceRegistryBuilder();

        builder.applySetting("hibernate.connection.url", JDBC_URL);
        builder.applySetting("hibernate.show_sql", true);

        return new Configuration().configure(U.resolveGridGainUrl("examples/config/hibernate-L2-cache.xml"))
            .buildSessionFactory(builder.buildServiceRegistry());
    }

    /**
     * Runs a Hibernate {@link Criteria} multi-result query for the
     * specified entity class.
     *
     * @param sesFactory Hibernate {@link SessionFactory}.
     * @param cls Target entity class.
     * @param critClo Optional closure for specifying criteria. If {@code null} - all objects of
     *      the specified entity class will be fetched.
     * @return List of queried objects.
     */
    @SuppressWarnings("unchecked")
    private static <T> List<T> runQuery(SessionFactory sesFactory, Class<T> cls,
        @Nullable GridClosure<Criteria, Criteria> critClo) {
        Session ses = sesFactory.openSession();

        try {
            Criteria crit = ses.createCriteria(cls);

            // Modify criteria with closure if specified.
            if (critClo != null)
                crit = critClo.apply(crit);

            // Enable caching for this query. This way, if a query result for
            // a given criteria is already present in cache, it will be immediately
            // returned, and no database query will be run (see Hibernate SQL output in
            // console).
            crit.setCacheable(true);

            return crit.list();
        }
        finally {
            ses.close();
        }
    }

    /**
     * Prints Hibernate L2 cache statistics to standard output.
     *
     * @param sesFactory Hibernate {@link SessionFactory}, for which to print
     *                   statistics.
     */
    private static void printStats(SessionFactory sesFactory) {
        System.out.println("=== Hibernate L2 cache statistics ===");

        for (String entityName : ENTITY_NAMES) {
            System.out.println("\tEntity: " + entityName);

            SecondLevelCacheStatistics stats =
                sesFactory.getStatistics().getSecondLevelCacheStatistics(entityName);

            System.out.println("\t\tL2 cache entries: " + stats.getEntries());
            System.out.println("\t\tHits: " + stats.getHitCount());
            System.out.println("\t\tMisses: " + stats.getMissCount());
        }

        System.out.println("=====================================");
    }

    /**
     * Prints GridGain cache metrics.
     *
     * @param grid {@link Grid} instance, for which to print statistics.
     */
    private static void printStats(Grid grid) {
        System.out.println("=== GridGain cache metrics ===");

        for (String entityName : ENTITY_NAMES) {
            System.out.println("\tEntity: " + entityName);

            GridCache<?, ?> cache = grid.cache(entityName);

            assert cache != null;

            GridCacheMetrics metrics = cache.metrics();

            System.out.println("\t\tReads: " + metrics.reads());
            System.out.println("\t\tWrites: " + metrics.writes());
            System.out.println("\t\tHits: " + metrics.hits());
            System.out.println("\t\tMisses: " + metrics.misses());
            System.out.println("\t\tCommits: " + metrics.txCommits());
        }

        System.out.println("==============================");
    }
}
