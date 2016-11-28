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
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.Person;

/**
 * Example to showcase DML capabilities of Ignite's SQL engine.
 */
public class CacheQueryDmlExample {
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
            print(">>> Cache query DML example started.");

            CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE);
            orgCacheCfg.setIndexedTypes(Long.class, Organization.class);

            CacheConfiguration<AffinityKey<Long>, Person> personCacheCfg = new CacheConfiguration<>(PERSON_CACHE);
            personCacheCfg.setIndexedTypes(AffinityKey.class, Person.class);

            // Auto-close cache at the end of the example.
            try (
                IgniteCache<Long, Organization> orgCache = ignite.getOrCreateCache(orgCacheCfg);
                IgniteCache<AffinityKey<Long>, Person> personCache = ignite.getOrCreateCache(personCacheCfg)
            ) {
                // Populate cache.
                initialize();
                queryData("Initial state:");

                update();
                queryData("Raised salary for Apache employees:");

                merge();
                queryData("Names and salaries after shift and hire:");

                delete();
                queryData("Removing non-Apache persons:");
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

        // Organizations.
        Organization org1 = new Organization("Apache");
        Organization org2 = new Organization("Other");

        orgCache.put(org1.id(), org1);
        orgCache.put(org2.id(), org2);

        // Insert persons by key/value.
        IgniteCache<Long, Person> personCache = Ignition.ignite().cache(PERSON_CACHE);

        Person p1 = new Person(org1, "John", "Doe", 2000, "Master");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Bachelor");

        SqlFieldsQuery qry = new SqlFieldsQuery("insert into Person (_key, _val) values (?, ?)");

        personCache.query(qry.setArgs(p1.key(), p1));
        personCache.query(qry.setArgs(p2.key(), p2));

        // Insert persons via field values.
        long id3 = Person.ID_GEN.incrementAndGet();
        long id4 = Person.ID_GEN.incrementAndGet();

        AffinityKey<Long> key3 = new AffinityKey<>(id3, org2.id());
        AffinityKey<Long> key4 = new AffinityKey<>(id4, org2.id());

        qry = new SqlFieldsQuery(
            "insert into Person (_key, id, orgId, firstName, lastName, salary, resume) values (?, ?, ?, ?, ?, ?, ?)");

        personCache.query(qry.setArgs(key3, id3, org2.id(), "John", "Smith", 1000, "Bachelor"));
        personCache.query(qry.setArgs(key4, id4, org2.id(), "Jane", "Smith", 2000, "Master"));
    }

    /**
     * Example of conditional UPDATE query - raise salary by 10% to everyone that has Master's degree
     * and works on Ignite.
     */
    private static void update() {
        IgniteCache<AffinityKey<Long>, Person> cache = Ignition.ignite().cache(PERSON_CACHE);

        cache.query(new SqlFieldsQuery("update Person set salary = salary * 1.1 where resume like '%Master%' and " +
            "id in (select p.id from Person p, \"" + ORG_CACHE + "\".Organization as o where o.name = ? " +
            "and p.orgId = o.id)").setArgs("Apache"));
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
     * Query current data.
     *
     * @param msg Message.
     */
    private static void queryData(String msg) {
        IgniteCache<AffinityKey<Long>, Person> cache = Ignition.ignite().cache(PERSON_CACHE);

        // Execute query to get names of all employees.
        String sql =
            "select p.id, concat(p.firstName, ' ', p.lastName), o.name, p.resume, p.salary " +
            "from Person as p, \"" + ORG_CACHE + "\".Organization as o " +
            "where p.orgId = o.id";

        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(sql));

        // Print person and organization names.
        List<List<?>> res = cursor.getAll();

        print(msg, res);
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
