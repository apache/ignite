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

package org.apache.ignite.examples.sql;

import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * SQL queries example with the usage of Java SQL API.
 * <p>
 * Example also demonstrates usage of fields queries that return only required
 * fields instead of whole key-value pairs. When fields queries are distributed
 * across several nodes, they may not work as expected. Keep in mind following
 * limitations (not applied if data is queried from one node only):
 * <ul>
 *     <li>
 *         Non-distributed joins will work correctly only if joined objects are stored in
 *         collocated mode. Refer to {@link AffinityKey} javadoc for more details.
 *         <p>
 *         To use distributed joins it is necessary to set query 'distributedJoin' flag using
 *         {@link SqlFieldsQuery#setDistributedJoins(boolean)} or {@link SqlQuery#setDistributedJoins(boolean)}.
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
 * Remote nodes should be started using {@link ExampleNodeStartup} which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class SqlQueriesExample {
    /** Organizations cache name. */
    private static final String ORG_CACHE = SqlQueriesExample.class.getSimpleName() + "Organizations";

    /** Persons collocated with Organizations cache name. */
    private static final String COLLOCATED_PERSON_CACHE = SqlQueriesExample.class.getSimpleName() + "CollocatedPersons";

    /** Persons cache name. */
    private static final String PERSON_CACHE = SqlQueriesExample.class.getSimpleName() + "Persons";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> SQL queries example started.");

            CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE);

            orgCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            orgCacheCfg.setIndexedTypes(Long.class, Organization.class);

            CacheConfiguration<AffinityKey<Long>, Person> colPersonCacheCfg =
                new CacheConfiguration<>(COLLOCATED_PERSON_CACHE);

            colPersonCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            colPersonCacheCfg.setIndexedTypes(AffinityKey.class, Person.class);

            CacheConfiguration<Long, Person> personCacheCfg = new CacheConfiguration<>(PERSON_CACHE);

            personCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            personCacheCfg.setIndexedTypes(Long.class, Person.class);

            try {
                // Create caches.
                ignite.getOrCreateCache(orgCacheCfg);
                ignite.getOrCreateCache(colPersonCacheCfg);
                ignite.getOrCreateCache(personCacheCfg);

                // Populate caches.
                initialize();

                // Example for SQL-based querying employees based on salary ranges.
                sqlQuery();

                // Example for SQL-based querying employees for a given organization
                // (includes SQL join for collocated objects).
                sqlQueryWithJoin();

                // Example for SQL-based querying employees for a given organization
                // (includes distributed SQL join).
                sqlQueryWithDistributedJoin();

                // Example for SQL-based querying to calculate average salary
                // among all employees within a company.
                sqlQueryWithAggregation();

                // Example for SQL-based fields queries that return only required
                // fields instead of whole key-value pairs.
                sqlFieldsQuery();

                // Example for SQL-based fields queries that uses joins.
                sqlFieldsQueryWithJoin();
            }
            finally {
                // Distributed cache could be removed from cluster only by Ignite.destroyCache() call.
                ignite.destroyCache(COLLOCATED_PERSON_CACHE);
                ignite.destroyCache(PERSON_CACHE);
                ignite.destroyCache(ORG_CACHE);
            }

            print("SQL queries example finished.");
        }
    }

    /**
     * Example for SQL queries based on salary ranges.
     */
    private static void sqlQuery() {
        IgniteCache<Long, Person> cache = Ignition.ignite().cache(PERSON_CACHE);

        // SQL clause which selects salaries based on range.
        String sql = "salary > ? and salary <= ?";

        // Execute queries for salary ranges.
        print("People with salaries between 0 and 1000 (queried with SQL query): ",
            cache.query(new SqlQuery<AffinityKey<Long>, Person>(Person.class, sql).
                setArgs(0, 1000)).getAll());

        print("People with salaries between 1000 and 2000 (queried with SQL query): ",
            cache.query(new SqlQuery<AffinityKey<Long>, Person>(Person.class, sql).
                setArgs(1000, 2000)).getAll());
    }

    /**
     * Example for SQL queries based on all employees working for a specific organization.
     */
    private static void sqlQueryWithJoin() {
        IgniteCache<AffinityKey<Long>, Person> cache = Ignition.ignite().cache(COLLOCATED_PERSON_CACHE);

        // SQL clause query which joins on 2 types to select people for a specific organization.
        String joinSql =
            "from Person, \"" + ORG_CACHE + "\".Organization as org " +
            "where Person.orgId = org.id " +
            "and lower(org.name) = lower(?)";

        // Execute queries for find employees for different organizations.
        print("Following people are 'ApacheIgnite' employees: ",
            cache.query(new SqlQuery<AffinityKey<Long>, Person>(Person.class, joinSql).
                setArgs("ApacheIgnite")).getAll());

        print("Following people are 'Other' employees: ",
            cache.query(new SqlQuery<AffinityKey<Long>, Person>(Person.class, joinSql).
                setArgs("Other")).getAll());
    }

    /**
     * Example for SQL queries based on all employees working
     * for a specific organization (query uses distributed join).
     */
    private static void sqlQueryWithDistributedJoin() {
        IgniteCache<Long, Person> cache = Ignition.ignite().cache(PERSON_CACHE);

        // SQL clause query which joins on 2 types to select people for a specific organization.
        String joinSql =
            "from Person, \"" + ORG_CACHE + "\".Organization as org " +
            "where Person.orgId = org.id " +
            "and lower(org.name) = lower(?)";

        SqlQuery qry = new SqlQuery<Long, Person>(Person.class, joinSql).
            setArgs("ApacheIgnite");

        // Enable distributed joins for query.
        qry.setDistributedJoins(true);

        // Execute queries for find employees for different organizations.
        print("Following people are 'ApacheIgnite' employees (distributed join): ", cache.query(qry).getAll());

        qry.setArgs("Other");

        print("Following people are 'Other' employees (distributed join): ", cache.query(qry).getAll());
    }

    /**
     * Example for SQL queries to calculate average salary for a specific organization.
     */
    private static void sqlQueryWithAggregation() {
        IgniteCache<AffinityKey<Long>, Person> cache = Ignition.ignite().cache(COLLOCATED_PERSON_CACHE);

        // Calculate average of salary of all persons in ApacheIgnite.
        // Note that we also join on Organization cache as well.
        String sql =
            "select avg(salary) " +
            "from Person, \"" + ORG_CACHE + "\".Organization as org " +
            "where Person.orgId = org.id " +
            "and lower(org.name) = lower(?)";

        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(sql).setArgs("ApacheIgnite"));

        // Calculate average salary for a specific organization.
        print("Average salary for 'ApacheIgnite' employees: ", cursor.getAll());
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     */
    private static void sqlFieldsQuery() {
        IgniteCache<Long, Person> cache = Ignition.ignite().cache(PERSON_CACHE);

        // Execute query to get names of all employees.
        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(
            "select concat(firstName, ' ', lastName) from Person"));

        // In this particular case each row will have one element with full name of an employees.
        List<List<?>> res = cursor.getAll();

        // Print names.
        print("Names of all employees:", res);
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     */
    private static void sqlFieldsQueryWithJoin() {
        IgniteCache<AffinityKey<Long>, Person> cache = Ignition.ignite().cache(COLLOCATED_PERSON_CACHE);

        // Execute query to get names of all employees.
        String sql =
            "select concat(firstName, ' ', lastName), org.name " +
            "from Person, \"" + ORG_CACHE + "\".Organization as org " +
            "where Person.orgId = org.id";

        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(sql));

        // In this particular case each row will have one element with full name of an employees.
        List<List<?>> res = cursor.getAll();

        // Print persons' names and organizations' names.
        print("Names of all employees and organizations they belong to: ", res);
    }

    /**
     * Populate cache with test data.
     */
    private static void initialize() {
        IgniteCache<Long, Organization> orgCache = Ignition.ignite().cache(ORG_CACHE);

        // Clear cache before running the example.
        orgCache.clear();

        // Organizations.
        Organization org1 = new Organization("ApacheIgnite");
        Organization org2 = new Organization("Other");

        orgCache.put(org1.id(), org1);
        orgCache.put(org2.id(), org2);

        IgniteCache<AffinityKey<Long>, Person> colPersonCache = Ignition.ignite().cache(COLLOCATED_PERSON_CACHE);
        IgniteCache<Long, Person> personCache = Ignition.ignite().cache(PERSON_CACHE);

        // Clear caches before running the example.
        colPersonCache.clear();
        personCache.clear();

        // People.
        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
        Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
        Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

        // Note that in this example we use custom affinity key for Person objects
        // to ensure that all persons are collocated with their organizations.
        colPersonCache.put(p1.key(), p1);
        colPersonCache.put(p2.key(), p2);
        colPersonCache.put(p3.key(), p3);
        colPersonCache.put(p4.key(), p4);

        // These Person objects are not collocated with their organizations.
        personCache.put(p1.id, p1);
        personCache.put(p2.id, p2);
        personCache.put(p3.id, p3);
        personCache.put(p4.id, p4);
    }

    /**
     * Prints message and query results.
     *
     * @param msg Message to print before all objects are printed.
     * @param col Query results.
     */
    private static void print(String msg, Iterable<?> col) {
        print(msg);
        print(col);
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }

    /**
     * Prints query results.
     *
     * @param col Query results.
     */
    private static void print(Iterable<?> col) {
        for (Object next : col)
            System.out.println(">>>     " + next);
    }
}
