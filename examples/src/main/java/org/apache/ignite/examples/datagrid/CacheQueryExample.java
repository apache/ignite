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

package org.apache.ignite.examples.datagrid;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.examples.*;

import javax.cache.*;
import java.io.*;
import java.util.*;

/**
 * Cache queries example. This example demonstrates SQL, TEXT, and FULL SCAN
 * queries over cache.
 * <p>
 * Example also demonstrates usage of fields queries that return only required
 * fields instead of whole key-value pairs. When fields queries are distributed
 * across several nodes, they may not work as expected. Keep in mind following
 * limitations (not applied if data is queried from one node only):
 * <ul>
 *     <li>
 *         Joins will work correctly only if joined objects are stored in
 *         collocated mode. Refer to {@link CacheAffinityKey} javadoc for more details.
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
public class CacheQueryExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheQueryExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache query example started.");

            CacheConfiguration<?, ?> cfg = new CacheConfiguration<>();

            cfg.setCacheMode(CacheMode.PARTITIONED);
            cfg.setName(CACHE_NAME);
            cfg.setIndexedTypes(
                UUID.class, Organization.class,
                CacheAffinityKey.class, Person.class
            );

            try (IgniteCache<?, ?> cache = ignite.createCache(cfg)) {
                // Populate cache.
                initialize();

                // Example for SQL-based querying employees based on salary ranges.
                sqlQuery();

                // Example for SQL-based querying employees for a given organization (includes SQL join).
                sqlQueryWithJoin();

                // Example for TEXT-based querying for a given string in peoples resumes.
                textQuery();

                // Example for SQL-based querying to calculate average salary among all employees within a company.
                sqlQueryWithAggregation();

                // Example for SQL-based fields queries that return only required
                // fields instead of whole key-value pairs.
                sqlFieldsQuery();

                // Example for SQL-based fields queries that return only required
                // fields instead of whole key-value pairs.
                sqlFieldsQuery();

                // Example for SQL-based fields queries that uses joins.
                sqlFieldsQueryWithJoin();
            }

            print("Cache query example finished.");
        }
    }

    /**
     * Example for SQL queries based on salary ranges.
     */
    private static void sqlQuery() {
        IgniteCache<CacheAffinityKey<UUID>, Person> cache = Ignition.ignite().cache(CACHE_NAME);

        // SQL clause which selects salaries based on range.
        String sql = "salary > ? and salary <= ?";

        // Execute queries for salary ranges.
        print("People with salaries between 0 and 1000: ",
            cache.query(new SqlQuery<CacheAffinityKey<UUID>, Person>(Person.class, sql).
                setArgs(0, 1000)).getAll());

        print("People with salaries between 1000 and 2000: ",
            cache.query(new SqlQuery<CacheAffinityKey<UUID>, Person>(Person.class, sql).
                setArgs(1000, 2000)).getAll());

        print("People with salaries greater than 2000: ",
            cache.query(new SqlQuery<CacheAffinityKey<UUID>, Person>(Person.class, sql).
                setArgs(2000, Integer.MAX_VALUE)).getAll());
    }

    /**
     * Example for SQL queries based on all employees working for a specific organization.
     */
    private static void sqlQueryWithJoin() {
        IgniteCache<CacheAffinityKey<UUID>, Person> cache = Ignition.ignite().cache(CACHE_NAME);

        // SQL clause query which joins on 2 types to select people for a specific organization.
        String joinSql =
            "from Person, Organization "
            + "where Person.orgId = Organization.id "
            + "and lower(Organization.name) = lower(?)";

        // Execute queries for find employees for different organizations.
        print("Following people are 'GridGain' employees: ",
            cache.query(new SqlQuery<CacheAffinityKey<UUID>, Person>(Person.class, joinSql).
                setArgs("GridGain")).getAll());
        print("Following people are 'Other' employees: ",
            cache.query(new SqlQuery<CacheAffinityKey<UUID>, Person>(Person.class, joinSql).
                setArgs("Other")).getAll());
    }

    /**
     * Example for TEXT queries using LUCENE-based indexing of people's resumes.
     */
    private static void textQuery() {
        IgniteCache<CacheAffinityKey<UUID>, Person> cache = Ignition.ignite().cache(CACHE_NAME);

        //  Query for all people with "Master Degree" in their resumes.
        QueryCursor<Cache.Entry<CacheAffinityKey<UUID>, Person>> masters =
            cache.query(new TextQuery<CacheAffinityKey<UUID>, Person>(Person.class, "Master"));

        // Query for all people with "Bachelor Degree" in their resumes.
        QueryCursor<Cache.Entry<CacheAffinityKey<UUID>, Person>> bachelors =
            cache.query(new TextQuery<CacheAffinityKey<UUID>, Person>(Person.class, "Bachelor"));

        print("Following people have 'Master Degree' in their resumes: ", masters.getAll());
        print("Following people have 'Bachelor Degree' in their resumes: ", bachelors.getAll());
    }

    /**
     * Example for SQL queries to calculate average salary for a specific organization.
     */
    private static void sqlQueryWithAggregation() {
        IgniteCache<CacheAffinityKey<UUID>, Person> cache = Ignition.ignite().cache(CACHE_NAME);

        // Calculate average of salary of all persons in GridGain.
        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select avg(salary) from Person, " +
            "Organization where Person.orgId = Organization.id and " + "lower(Organization.name) = lower(?)"
        ).setArgs("GridGain"));

        // Calculate average salary for a specific organization.
        print("Average salary for 'GridGain' employees: " + cursor.getAll());
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     */
    private static void sqlFieldsQuery() {
        IgniteCache<?, ?> cache = Ignition.ignite().cache(CACHE_NAME);

        // Create query to get names of all employees.
        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select concat(firstName, ' ', " +
            "lastName) from Person"));

        // Execute query to get collection of rows. In this particular
        // case each row will have one element with full name of an employees.
        List<List<?>> res = cursor.getAll();

        // Print names.
        print("Names of all employees:", res);
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     */
    private static void sqlFieldsQueryWithJoin() {
        IgniteCache<?, ?> cache = Ignition.ignite().cache(CACHE_NAME);

        // Execute query to get names of all employees.
        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select concat(firstName, ' ', lastName), " +
            "" + "Organization.name from Person, Organization where " + "Person.orgId = Organization.id"));

        // In this particular case each row will have one element with full name of an employees.
        List<List<?>> res = cursor.getAll();

        // Print persons' names and organizations' names.
        print("Names of all employees and organizations they belong to:", res);
    }

    /**
     * Populate cache with test data.
     */
    private static void initialize() {
        IgniteCache<Object, Object> cache = Ignition.ignite().cache(CACHE_NAME);

        // Organizations.
        Organization org1 = new Organization("GridGain");
        Organization org2 = new Organization("Other");

        // People.
        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
        Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
        Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

        cache.put(org1.id, org1);
        cache.put(org2.id, org2);

        // Note that in this example we use custom affinity key for Person objects
        // to ensure that all persons are collocated with their organizations.
        cache.put(p1.key(), p1);
        cache.put(p2.key(), p2);
        cache.put(p3.key(), p3);
        cache.put(p4.key(), p4);
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
        @QuerySqlField(index = true)
        private UUID id;

        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private UUID orgId;

        /** First name (not-indexed). */
        @QuerySqlField
        private String firstName;

        /** Last name (not indexed). */
        @QuerySqlField
        private String lastName;

        /** Resume text (create LUCENE-based TEXT index for this field). */
        @QueryTextField
        private String resume;

        /** Salary (indexed). */
        @QuerySqlField(index = true)
        private double salary;

        /** Custom cache key to guarantee that person is always collocated with its organization. */
        private transient CacheAffinityKey<UUID> key;

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
        public CacheAffinityKey<UUID> key() {
            if (key == null)
                key = new CacheAffinityKey<>(id, orgId);

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
        @QuerySqlField(index = true)
        private UUID id;

        /** Organization name (indexed). */
        @QuerySqlField(index = true)
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
