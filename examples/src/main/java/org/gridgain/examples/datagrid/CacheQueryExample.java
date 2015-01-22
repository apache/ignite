/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.examples.datagrid;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.query.*;

import java.io.*;
import java.util.*;

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
 *         collocated mode. Refer to {@link GridCacheAffinityKey} javadoc for more details.
 *     </li>
 *     <li>
 *         Note that if you created query on to replicated cache, all data will
 *         be queried only on one node, not depending on what caches participate in
 *         the query (some data from partitioned cache can be lost). And visa versa,
 *         if you created it on partitioned cache, data from replicated caches
 *         will be duplicated.
 *     </li>
 * </ul>
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheQueryExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite g = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache query example started.");

            // Clean up caches on all nodes before run.
            g.cache(CACHE_NAME).globalClearAll(0);

            // Populate cache.
            initialize();

            // Example for SQL-based querying employees based on salary ranges.
            sqlQuery();

            // Example for SQL-based querying employees for a given organization (includes SQL join).
            sqlQueryWithJoin();

            // Example for TEXT-based querying for a given string in peoples resumes.
            textQuery();

            // Example for SQL-based querying with custom remote and local reducers
            // to calculate average salary among all employees within a company.
            sqlQueryWithReducers();

            // Example for SQL-based querying with custom remote transformer to make sure
            // that only required data without any overhead is returned to caller.
            sqlQueryWithTransformer();

            // Example for SQL-based fields queries that return only required
            // fields instead of whole key-value pairs.
            sqlFieldsQuery();

            // Example for SQL-based fields queries that uses joins.
            sqlFieldsQueryWithJoin();

            print("Cache query example finished.");
        }
    }

    /**
     * Example for SQL queries based on salary ranges.
     *
     * @throws IgniteCheckedException In case of error.
     */
    private static void sqlQuery() throws IgniteCheckedException {
        GridCache<GridCacheAffinityKey<UUID>, Person> cache = Ignition.ignite().cache(CACHE_NAME);

        // Create query which selects salaries based on range.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, Person>> qry =
            cache.queries().createSqlQuery(Person.class, "salary > ? and salary <= ?");

        // Execute queries for salary ranges.
        print("People with salaries between 0 and 1000: ", qry.execute(0, 1000).get());

        print("People with salaries between 1000 and 2000: ", qry.execute(1000, 2000).get());

        print("People with salaries greater than 2000: ", qry.execute(2000, Integer.MAX_VALUE).get());
    }

    /**
     * Example for SQL queries based on all employees working for a specific organization.
     *
     * @throws IgniteCheckedException In case of error.
     */
    private static void sqlQueryWithJoin() throws IgniteCheckedException {
        GridCache<GridCacheAffinityKey<UUID>, Person> cache = Ignition.ignite().cache(CACHE_NAME);

        // Create query which joins on 2 types to select people for a specific organization.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, Person>> qry =
            cache.queries().createSqlQuery(Person.class, "from Person, Organization " +
                "where Person.orgId = Organization.id " +
                "and lower(Organization.name) = lower(?)");

        // Execute queries for find employees for different organizations.
        print("Following people are 'GridGain' employees: ", qry.execute("GridGain").get());
        print("Following people are 'Other' employees: ", qry.execute("Other").get());
    }

    /**
     * Example for TEXT queries using LUCENE-based indexing of people's resumes.
     *
     * @throws IgniteCheckedException In case of error.
     */
    private static void textQuery() throws IgniteCheckedException {
        GridCache<GridCacheAffinityKey<UUID>, Person> cache = Ignition.ignite().cache(CACHE_NAME);

        //  Query for all people with "Master Degree" in their resumes.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, Person>> masters =
            cache.queries().createFullTextQuery(Person.class, "Master");

        // Query for all people with "Bachelor Degree"in their resumes.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, Person>> bachelors =
            cache.queries().createFullTextQuery(Person.class, "Bachelor");

        print("Following people have 'Master Degree' in their resumes: ", masters.execute().get());
        print("Following people have 'Bachelor Degree' in their resumes: ", bachelors.execute().get());
    }

    /**
     * Example for SQL queries with custom remote and local reducers to calculate
     * average salary for a specific organization.
     *
     * @throws IgniteCheckedException In case of error.
     */
    private static void sqlQueryWithReducers() throws IgniteCheckedException {
        GridCacheProjection<GridCacheAffinityKey<UUID>, Person> cache = Ignition.ignite().cache(CACHE_NAME);

        // Calculate average of salary of all persons in GridGain.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, Person>> qry = cache.queries().createSqlQuery(
            Person.class,
            "from Person, Organization where Person.orgId = Organization.id and " +
                "lower(Organization.name) = lower(?)");

        Collection<IgniteBiTuple<Double, Integer>> res = qry.execute(
            new IgniteReducer<Map.Entry<GridCacheAffinityKey<UUID>, Person>, IgniteBiTuple<Double, Integer>>() {
                private double sum;

                private int cnt;

                @Override public boolean collect(Map.Entry<GridCacheAffinityKey<UUID>, Person> e) {
                    sum += e.getValue().salary;

                    cnt++;

                    // Continue collecting.
                    return true;
                }

                @Override public IgniteBiTuple<Double, Integer> reduce() {
                    return new IgniteBiTuple<>(sum, cnt);
                }
            }, "GridGain").get();

        double sum = 0.0d;
        int cnt = 0;

        for (IgniteBiTuple<Double, Integer> t : res) {
            sum += t.get1();
            cnt += t.get2();
        }

        double avg = sum / cnt;

        // Calculate average salary for a specific organization.
        print("Average salary for 'GridGain' employees: " + avg);
    }

    /**
     * Example for SQL queries with custom transformer to allow passing
     * only the required set of fields back to caller.
     *
     * @throws IgniteCheckedException In case of error.
     */
    private static void sqlQueryWithTransformer() throws IgniteCheckedException {
        GridCache<GridCacheAffinityKey<UUID>, Person> cache = Ignition.ignite().cache(CACHE_NAME);

        // Create query to get names of all employees working for some company.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, Person>> qry =
            cache.queries().createSqlQuery(Person.class,
                "from Person, Organization " +
                    "where Person.orgId = Organization.id and lower(Organization.name) = lower(?)");

        // Transformer to convert Person objects to String.
        // Since caller only needs employee names, we only
        // send names back.
        IgniteClosure<Map.Entry<GridCacheAffinityKey<UUID>, Person>, String> trans =
            new IgniteClosure<Map.Entry<GridCacheAffinityKey<UUID>, Person>, String>() {
                @Override public String apply(Map.Entry<GridCacheAffinityKey<UUID>, Person> e) {
                    return e.getValue().lastName;
                }
            };

        // Query all nodes for names of all GridGain employees.
        print("Names of all 'GridGain' employees: " + qry.execute(trans, "GridGain").get());
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     *
     * @throws IgniteCheckedException In case of error.
     */
    private static void sqlFieldsQuery() throws IgniteCheckedException {
        GridCache<?, ?> cache = Ignition.ignite().cache(CACHE_NAME);

        // Create query to get names of all employees.
        GridCacheQuery<List<?>> qry1 = cache.queries().createSqlFieldsQuery(
            "select concat(firstName, ' ', lastName) from Person");

        // Execute query to get collection of rows. In this particular
        // case each row will have one element with full name of an employees.
        Collection<List<?>> res = qry1.execute().get();

        // Print names.
        print("Names of all employees:", res);
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     *
     * @throws IgniteCheckedException In case of error.
     */
    private static void sqlFieldsQueryWithJoin() throws IgniteCheckedException {
        GridCache<?, ?> cache = Ignition.ignite().cache(CACHE_NAME);

        // Create query to get names of all employees.
        GridCacheQuery<List<?>> qry1 = cache.queries().createSqlFieldsQuery(
            "select concat(firstName, ' ', lastName), Organization.name from Person, Organization where " +
                "Person.orgId = Organization.id");

        // Execute query to get collection of rows. In this particular
        // case each row will have one element with full name of an employees.
        Collection<List<?>> res = qry1.execute().get();

        // Print persons' names and organizations' names.
        print("Names of all employees and organizations they belong to:", res);
    }

    /**
     * Populate cache with test data.
     *
     * @throws IgniteCheckedException In case of error.
     * @throws InterruptedException In case of error.
     */
    private static void initialize() throws IgniteCheckedException, InterruptedException {
        GridCache<?, ?> cache = Ignition.ignite().cache(CACHE_NAME);

        // Organization projection.
        GridCacheProjection<UUID, Organization> orgCache = cache.projection(UUID.class, Organization.class);

        // Person projection.
        GridCacheProjection<GridCacheAffinityKey<UUID>, Person> personCache =
            cache.projection(GridCacheAffinityKey.class, Person.class);

        // Organizations.
        Organization org1 = new Organization("GridGain");
        Organization org2 = new Organization("Other");

        // People.
        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
        Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
        Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

        orgCache.put(org1.id, org1);
        orgCache.put(org2.id, org2);

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
     * @param col Query results.
     */
    private static void print(String msg, Iterable<?> col) {
        if (msg != null)
            System.out.println(">>> " + msg);

        print(col);
    }

    /**
     * Prints collection items.
     *
     * @param col Collection.
     */
    private static void print(Iterable<?> col) {
        for (Object next : col) {
            if (next instanceof Iterable)
                print((Iterable<?>)next);
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

    /**
     * Person class.
     */
    private static class Person implements Serializable {
        /** Person ID (indexed). */
        @GridCacheQuerySqlField(index = true)
        private UUID id;

        /** Organization ID (indexed). */
        @GridCacheQuerySqlField(index = true)
        private UUID orgId;

        /** First name (not-indexed). */
        @GridCacheQuerySqlField
        private String firstName;

        /** Last name (not indexed). */
        @GridCacheQuerySqlField
        private String lastName;

        /** Resume text (create LUCENE-based TEXT index for this field). */
        @GridCacheQueryTextField
        private String resume;

        /** Salary (indexed). */
        @GridCacheQuerySqlField
        private double salary;

        /** Custom cache key to guarantee that person is always collocated with its organization. */
        private transient GridCacheAffinityKey<UUID> key;

        /**
         * Constructs person record.
         *
         * @param org Organization.
         * @param firstName First name.
         * @param lastName Last name.
         * @param salary Salary.
         * @param resume Resume text.
         */
        Person(Organization org, String firstName, String lastName, double salary, String resume) {
            // Generate unique ID for this person.
            id = UUID.randomUUID();

            orgId = org.id;

            this.firstName = firstName;
            this.lastName = lastName;
            this.resume = resume;
            this.salary = salary;
        }

        /**
         * Gets cache affinity key. Since in some examples person needs to be collocated with organization, we create
         * custom affinity key to guarantee this collocation.
         *
         * @return Custom affinity key to guarantee that person is always collocated with organization.
         */
        public GridCacheAffinityKey<UUID> key() {
            if (key == null)
                key = new GridCacheAffinityKey<>(id, orgId);

            return key;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person [firstName=" + firstName +
                ", lastName=" + lastName +
                ", id=" + id +
                ", orgId=" + orgId +
                ", resume=" + resume +
                ", salary=" + salary + ']';
        }
    }

    /**
     * Organization class.
     */
    private static class Organization implements Serializable {
        /** Organization ID (indexed). */
        @GridCacheQuerySqlField(index = true)
        private UUID id;

        /** Organization name (indexed). */
        @GridCacheQuerySqlField(index = true)
        private String name;

        /**
         * Create organization.
         *
         * @param name Organization name.
         */
        Organization(String name) {
            id = UUID.randomUUID();

            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization [id=" + id + ", name=" + name + ']';
        }
    }
}
