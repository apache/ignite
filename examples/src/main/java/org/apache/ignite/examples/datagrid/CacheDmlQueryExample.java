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

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.Person;

/**
 * Example to showcase DML capabilities of Ignite's SQL engine.
 */
public class CacheDmlQueryExample {
    /** Organizations cache name. */
    private static final String ORG_CACHE = CacheQueryExample.class.getSimpleName() + "Organizations";

    /** Persons cache name. */
    private static final String PERSON_CACHE = CacheQueryExample.class.getSimpleName() + "Persons";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    @SuppressWarnings({"unused", "ThrowFromFinallyBlock"})
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache query example started.");

            CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE);

            orgCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            orgCacheCfg.setIndexedTypes(Long.class, Organization.class);

            CacheConfiguration<AffinityKey<Long>, Person> personCacheCfg = new CacheConfiguration<>(PERSON_CACHE);

            personCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            personCacheCfg.setIndexedTypes(AffinityKey.class, Person.class);

            // Auto-close cache at the end of the example.
            try (
                IgniteCache<Long, Organization> orgCache = ignite.getOrCreateCache(orgCacheCfg);
                IgniteCache<AffinityKey<Long>, Person> personCache = ignite.getOrCreateCache(personCacheCfg)
            ) {
                // Populate cache.
                initialize();

                sqlFieldsQueryWithJoin();

                update();

                print("Names and salaries after salary raise for Masters:");

                sqlFieldsQueryWithJoin();

                print("Names and salaries after shift and hire:");

                merge();

                sqlFieldsQueryWithJoin();

                print("Names and salaries after removing non-Igniters:");

                delete();

                sqlFieldsQueryWithJoin();
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(PERSON_CACHE);
                ignite.destroyCache(ORG_CACHE);
            }

            print("Cache query example finished.");
        }
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

        IgniteCache<Long, Person> personCache = Ignition.ignite().cache(PERSON_CACHE);

        // Clear cache before running the example.
        personCache.clear();

        // People.
        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");

        Person[] people = new Person[] { p1, p2 };

        // 2 keys and 2 Persons - total 8 arguments.
        Object[] insertQryArgs = new Object[4];

        for (int i = 0; i < 2; i++) {
            insertQryArgs[i * 2] = people[i].key();
            insertQryArgs[i * 2 + 1] = people[i];
        }

        personCache.query(new SqlFieldsQuery("insert into Person (_key, _val) values (?, ?), (?, ?)")
            .setArgs(insertQryArgs));

        // And now let's insert people from 2nd organization via field values

        // 2 Persons, 7 arguments for each - total 14 arguments.
        insertQryArgs = new Object[14];

        long id3 = Person.ID_GEN.incrementAndGet();

        long id4 = Person.ID_GEN.incrementAndGet();

        AffinityKey<Long> key3 = new AffinityKey<>(id3, org2.id());
        AffinityKey<Long> key4 = new AffinityKey<>(id4, org2.id());

        insertQryArgs[0] = key3;
        insertQryArgs[1] = id3;
        insertQryArgs[2] = org2.id();
        insertQryArgs[3] = "John";
        insertQryArgs[4] = "Smith";
        insertQryArgs[5] = 1000;
        insertQryArgs[6] = "John Smith has Bachelor Degree.";

        insertQryArgs[7] = key4;
        insertQryArgs[8] = id4;
        insertQryArgs[9] = org2.id();
        insertQryArgs[10] = "Jane";
        insertQryArgs[11] = "Smith";
        insertQryArgs[12] = 2000;
        insertQryArgs[13] = "Jane Smith has Master Degree.";

        personCache.query(new SqlFieldsQuery("insert into Person (_key, id, orgId, firstName, lastName, salary, resume) " +
            "values (?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?)").setArgs(insertQryArgs));
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     */
    private static void sqlFieldsQueryWithJoin() {
        IgniteCache<AffinityKey<Long>, Person> cache = Ignition.ignite().cache(PERSON_CACHE);

        // Execute query to get names of all employees.
        String sql =
            "select Person.id, concat(firstName, ' ', lastName), salary, org.name " +
                "from Person, \"" + ORG_CACHE + "\".Organization as org " +
                "where Person.orgId = org.id";

        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(sql));

        // In this particular case each row will have one element with full name of an employees.
        List<List<?>> res = cursor.getAll();

        // Print persons' names and organizations' names.
        print("Ids and names of all employees, their salaries, and organizations they belong to: ", res);
    }

    /**
     * Example of conditional UPDATE query - raise salary by 10% to everyone that has Master's degree
     * and works on Ignite.
     */
    private static void update() {
        IgniteCache<AffinityKey<Long>, Person> cache = Ignition.ignite().cache(PERSON_CACHE);

        cache.query(new SqlFieldsQuery("update Person set salary = salary * 1.1 where resume like '%Master%' and " +
            "id in (select p.id from Person p, \"" + ORG_CACHE + "\".Organization as o where o.name like '%Ignite%' " +
            "and p.orgId = o.id)"));
    }

    /**
     * Example of MERGE query - insert one new employee and replace an old one.
     */
    private static void merge() {
        IgniteCache<Long, Organization> orgCache = Ignition.ignite().cache(ORG_CACHE);

        IgniteCache<AffinityKey<Long>, Person> cache = Ignition.ignite().cache(PERSON_CACHE);

        // Let's find some guy we'll base the new one upon...
        List<List<?>> res = cache.query(new SqlFieldsQuery("select _val from Person where concat(firstName, ' ', " +
            "lastName) = ?").setArgs("John Smith")).getAll();

        Person p = (Person) res.get(0).get(0);

        // Now let's change some fields and put him back together with completely new one.
        p.firstName = "Sam";
        p.lastName = "Lewitt";
        p.salary = 3000;
        p.resume = "Sam Levitt has PhD degree.";

        Person newPerson = new Person(orgCache.get(p.orgId), "Mike", "Rose", 800, "Mike Rose is a college graduate.");

        cache.query(new SqlFieldsQuery("merge into Person(_key, _val) values (?, ?), (?, ?)").setArgs(p.key(),
            p, newPerson.key(), newPerson));
    }

    /**
     * Example of conditional DELETE query - delete everyone working at 'Other' company.
     */
    private static void delete() {
        IgniteCache<AffinityKey<Long>, Person> cache = Ignition.ignite().cache(PERSON_CACHE);

        cache.query(new SqlFieldsQuery("delete from Person where id in (select p.id from Person p, \"" +
            ORG_CACHE + "\".Organization as o where o.name = 'Other' and p.orgId = o.id)")).getAll();
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
