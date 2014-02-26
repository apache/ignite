// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.query;

import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Grid cache queries example. This example demonstrates SQL, TEXT, and FULL SCAN
 * queries over cache.
 * <p>
 * Example also demonstrates usage of fields queries that return only required
 * fields instead of whole key-value pairs. When fields queries are distributed
 * across several nodes, they may not work as expected. Keep in mind following
 * limitations (not applied if data is queried from one node only):
 * <ul>
 *     <li>
 *         {@code Group by} and {@code sort by} statements are applied separately
 *         on each node, so result set will likely be incorrectly grouped or sorted
 *         after results from multiple remote nodes are grouped together.
 *     </li>
 *     <li>
 *         Aggregation functions like {@code sum}, {@code max}, {@code avg}, etc.
 *         are also applied on each node. Therefore you will get several results
 *         containing aggregated values, one for each node.
 *     </li>
 *     <li>
 *         Joins will work correctly only if joined objects are stored in
 *         collocated mode. Refer to
 *         {@link org.gridgain.grid.cache.affinity.GridCacheAffinityKey}
 *         javadoc for more details.
 *     </li>
 *     <li>
 *         Note that if you created query on to local or replicated cache, all data will
 *         be queried only on one node, not depending on what caches participate in
 *         the query (some data from partitioned cache can be lost). And visa versa,
 *         if you created it on partitioned cache, data from replicated caches
 *         will be duplicated.
 *     </li>
 * </ul>
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridCacheQueryExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";
    // private static final String CACHE_NAME = "replicated";

    /**
     * Put data to cache and then queries them.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = args.length == 0 ? GridGain.start("examples/config/example-cache.xml") : GridGain.start(args[0])) {
            print("Query example started.");

            // Populate cache.
            initialize(g);

            // Using distributed queries for partitioned cache and local queries for replicated cache.
            // Since in replicated caches data is available on all nodes, including local one,
            // it is enough to just query the local node.
            GridProjection p = cache(g).configuration().getCacheMode() == PARTITIONED ? g :
                g.forLocal();

            // Example for SQL-based querying employees based on salary ranges.
            querySalaries(g, p);

            // Example for SQL-based querying employees for a given organization (includes SQL join).
            queryEmployees(g, p);

            // Example for TEXT-based querying for a given string in peoples resumes.
            queryDegree(g, p);

            // Example for SQL-based querying with custom remote and local reducers
            // to calculate average salary among all employees within a company.
            queryAverageSalary(g, p);

            // Example for SQL-based querying with custom remote transformer to make sure
            // that only required data without any overhead is returned to caller.
            queryEmployeeNames(g, p);

            // Example for SQL-based fields queries that return only required
            // fields instead of whole key-value pairs.
            queryFields(g, p);

            print("Query example finished.");
        }
    }

    /**
     * Gets instance of cache to use.
     *
     * @param g Grid.
     * @return Cache to use.
     */
    private static <K, V> GridCache<K, V> cache(Grid g) {
        return g.cache(CACHE_NAME);
    }

    /**
     * Example for SQL queries based on salary ranges.
     *
     * @param g Grid.
     * @param p Grid projection to run query on.
     * @throws GridException In case of error.
     */
    private static void querySalaries(Grid g, GridProjection p) throws GridException {
        GridCacheProjection<GridCacheAffinityKey<UUID>, AffinityPerson> cache = cache(g);

        // Create query which selects salaries based on range.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, AffinityPerson>> qry =
            cache.queries().createSqlQuery(AffinityPerson.class, "salary > ? and salary <= ?").projection(p);

        // Execute queries for salary ranges.
        print("People with salaries between 0 and 1000: ", qry.execute(0, 1000).get());

        print("People with salaries between 1000 and 2000: ", qry.execute(1000, 2000).get());

        print("People with salaries greater than 2000: ", qry.execute(2000, Integer.MAX_VALUE).get());
    }

    /**
     * Example for SQL queries based on all employees working for a specific organization.
     *
     * @param g Grid.
     * @param p Grid projection to run query on.
     * @throws GridException In case of error.
     */
    private static void queryEmployees(Grid g, GridProjection p) throws GridException {
        GridCacheProjection<GridCacheAffinityKey<UUID>, AffinityPerson> cache = cache(g);

        // Create query which joins on 2 types to select people for a specific organization.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, AffinityPerson>> qry =
            cache.queries().createSqlQuery(AffinityPerson.class, "from AffinityPerson, Organization " + "where " +
                "AffinityPerson.orgId = Organization.id and lower(Organization.name) = lower(?)")
            .projection(p);

        // Execute queries for find employees for different organizations.
        print("Following people are 'GridGain' employees: ", qry.execute("GridGain").get());
        print("Following people are 'Other' employees: ", qry.execute("Other").get());
    }

    /**
     * Example for TEXT queries using LUCENE-based indexing of people's resumes.
     *
     * @param g Grid.
     * @param p Grid projection to run query on.
     * @throws GridException In case of error.
     */
    private static void queryDegree(Grid g, GridProjection p) throws GridException {
        GridCacheProjection<GridCacheAffinityKey<UUID>, AffinityPerson> cache = cache(g);

        //  Query for all people with "Master Degree" in their resumes.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, AffinityPerson>> masters =
            cache.queries().createFullTextQuery(AffinityPerson.class, "Master").projection(p);

        // Query for all people with "Bachelor Degree"in their resumes.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, AffinityPerson>> bachelors =
            cache.queries().createFullTextQuery(AffinityPerson.class, "Bachelor").projection(p);

        print("Following people have 'Master Degree' in their resumes: ", masters.execute().get());

        print("Following people have 'Bachelor Degree' in their resumes: ", bachelors.execute().get());
    }

    /**
     * Example for SQL queries with custom remote and local reducers to calculate
     * average salary for a specific organization.
     *
     * @param g Grid.
     * @param p Grid projection to run query on.
     * @throws GridException In case of error.
     */
    private static void queryAverageSalary(Grid g, GridProjection p) throws GridException {
        GridCacheProjection<GridCacheAffinityKey<UUID>, AffinityPerson> cache = cache(g);

        // Calculate average of salary of all persons in GridGain.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, AffinityPerson>> qry =
            cache.queries().createSqlQuery(AffinityPerson.class, "from AffinityPerson, Organization " + "where " +
                "AffinityPerson.orgId = Organization.id and lower(Organization.name) = lower(?)");

        // Calculate average salary for a specific organization.
        GridCacheQueryFuture<GridBiTuple<Double, Integer>> fut =
            qry.projection(p).execute(new RemoteReducer(), "GridGain");

        LocalReducer locRdc = new LocalReducer();

        GridBiTuple<Double, Integer> t;

        while ((t = fut.next()) != null) {
            if (!locRdc.collect(t))
                break;
        }

        print("Average salary for 'GridGain' employees: " + locRdc.reduce());
    }

    /**
     * Remote reducer.
     */
    private static class RemoteReducer extends GridReducer<Map.Entry<GridCacheAffinityKey<UUID>, AffinityPerson>,
        GridBiTuple<Double, Integer>> {
        /** */
        private double sum;

        /** */
        private int cnt;

        /** {@inheritDoc} */
        @Override public boolean collect(Map.Entry<GridCacheAffinityKey<UUID>, AffinityPerson > e) {
            sum += e.getValue().getSalary();

            cnt++;

            // Continue collecting.
            return true;
        }

        /** {@inheritDoc} */
        @Override public GridBiTuple<Double, Integer> reduce() {
            return new GridBiTuple<>(sum, cnt);
        }
    }

    /**
     * Local reducer.
     */
    private static class LocalReducer extends GridReducer<GridBiTuple<Double, Integer>, Double> {
        /** */
        private double sum;

        /** */
        private int cnt;

        /** {@inheritDoc} */
        @Override public boolean collect(GridBiTuple<Double, Integer > t) {
            sum += t.get1();
            cnt += t.get2();

            // Continue collecting.
            return true;
        }

        /** {@inheritDoc} */
        @Override public Double reduce() {
            double avg = cnt == 0 ? 0 : sum / cnt;

            // Reset reducer state to correctly execute query several times.
            sum = 0;
            cnt = 0;

            return avg;
        }
    }

    /**
     * Example for SQL queries with custom transformer to allow passing
     * only the required set of fields back to caller.
     *
     * @param g Grid.
     * @param p Grid projection to run query on.
     * @throws GridException In case of error.
     */
    private static void queryEmployeeNames(Grid g, GridProjection p) throws GridException {
        GridCacheProjection<GridCacheAffinityKey<UUID>, AffinityPerson> cache = cache(g);

        // Create query to get names of all employees working for some company.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, AffinityPerson>> qry =
            cache.queries().createSqlQuery(AffinityPerson.class,
                "from AffinityPerson, Organization " +
                    "where AffinityPerson.orgId = Organization.id and lower(Organization.name) = lower(?)");

        // Transformer to convert Person objects to String.
        // Since caller only needs employee names, we only
        // send names back.
        Collection<String> names = qry.projection(p).execute(
            new GridClosure<Map.Entry<GridCacheAffinityKey<UUID>, AffinityPerson>, String>() {
                @Override public String apply(Map.Entry<GridCacheAffinityKey<UUID>, AffinityPerson> e) {
                    return e.getValue().getLastName();
                }
            }, "GridGain").get();

        print("Names of all 'GridGain' employees: " + names);
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     *
     * @param g Grid.
     * @param p Grid projection to run query on.
     * @throws GridException In case of error.
     */
    private static void queryFields(Grid g, GridProjection p) throws GridException {
        GridCache<?, ?> cache = g.cache(CACHE_NAME);

        // Create query to get names of all employees.
        GridCacheQuery<List<?>> qry = cache.queries().createSqlFieldsQuery("select concat(firstName, ' ', " +
            "lastName) from AffinityPerson").projection(p);

        // Execute query to get collection of rows. In this particular
        // case each row will have one element with full name of an employees.
        Collection<List<?>> res = qry.execute().get();

        // Print names.
        print("Names of all employees:", res);
    }

    /**
     * Populate cache with test data.
     *
     * @param g Grid.
     * @throws GridException In case of error.
     * @throws InterruptedException In case of error.
     */
    private static void initialize(Grid g) throws GridException, InterruptedException {
        GridCacheProjection<GridCacheAffinityKey<UUID>, AffinityPerson> cache = cache(g);

        // Organization projection.
        GridCacheProjection<UUID, Organization> orgCache = cache.projection(UUID.class, Organization.class);

        // Person projection.
        GridCacheProjection<GridCacheAffinityKey<UUID>, AffinityPerson> personCache =
            cache.projection(GridCacheAffinityKey.class, AffinityPerson.class);

        // Organizations.
        Organization org1 = new Organization("GridGain");
        Organization org2 = new Organization("Other");

        // People.
        AffinityPerson p1 = new AffinityPerson(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        AffinityPerson p2 = new AffinityPerson(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
        AffinityPerson p3 = new AffinityPerson(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
        AffinityPerson p4 = new AffinityPerson(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

        orgCache.put(org1.getId(), org1);
        orgCache.put(org2.getId(), org2);

        // Note that in this example we use custom affinity key for Person objects
        // to ensure that all persons are collocated with their organizations.
        personCache.put(p1.key(), p1);
        personCache.put(p2.key(), p2);
        personCache.put(p3.key(), p3);
        personCache.put(p4.key(), p4);

        // Wait 1 second to be sure that all nodes processed put requests.
        Thread.sleep(1000);
    }

    /**
     * Prints collection of objects to standard out.
     *
     * @param msg Message to print before all objects are printed.
     * @param res Query results.
     */
    private static void print(String msg, Iterable<?> res) {
        if (msg != null)
            System.out.println(">>> " + msg);

        print(res);
    }

    /**
     * Prints collection items.
     *
     * @param col Collection.
     */
    private static void print(Iterable<?> col) {
        for (Object next : col) {
            if (next instanceof Iterable)
                print(((Iterable)next).iterator());
            else
                System.out.println(">>>     " + next);
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
