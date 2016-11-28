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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.Person;

import java.util.List;

/**
 * Example to showcase DML capabilities of Ignite's SQL engine.
 */
public class CacheQueryDmlExample {
    /** Organizations cache name. */
    private static final String ORG_CACHE = "Organizations";

    /** Persons cache name. */
    private static final String PERSON_CACHE = "Persons";

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
                insert(orgCache, personCache);
                queryData(personCache, "Insert data:");

                update(personCache);
                queryData(personCache, "Update salary for Master degrees:");

                delete(personCache);
                queryData(personCache, "Delete non-Apache employees:");
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(PERSON_CACHE);
                ignite.destroyCache(ORG_CACHE);
            }

            print("Cache query DML example finished.");
        }
    }

    /**
     * Populate cache with test data.
     *
     * @param orgCache Organization cache,
     * @param personCache Person cache.
     */
    private static void insert(IgniteCache<Long, Organization> orgCache,
        IgniteCache<AffinityKey<Long>, Person> personCache) {
        // Organizations.
        Organization org1 = new Organization("Apache");
        Organization org2 = new Organization("Other");

        orgCache.put(org1.id(), org1);
        orgCache.put(org2.id(), org2);

        // Insert persons via field values.
        AffinityKey<Long> key1 = new AffinityKey<>(1L, org2.id());
        AffinityKey<Long> key2 = new AffinityKey<>(2L, org2.id());
        AffinityKey<Long> key3 = new AffinityKey<>(3L, org2.id());
        AffinityKey<Long> key4 = new AffinityKey<>(4L, org2.id());

        SqlFieldsQuery qry = new SqlFieldsQuery(
            "insert into Person (_key, id, orgId, firstName, lastName, salary, resume) values (?, ?, ?, ?, ?, ?, ?)");

        personCache.query(qry.setArgs(key1, 1L, org1.id(), "John", "Doe", 4000, "Master"));
        personCache.query(qry.setArgs(key2, 2L, org1.id(), "Jane", "Roe", 2000, "Bachelor"));
        personCache.query(qry.setArgs(key3, 3L, org2.id(), "Mary", "Major", 5000, "Master"));
        personCache.query(qry.setArgs(key4, 4L, org2.id(), "Richard", "Miles", 3000, "Bachelor"));
    }

    /**
     * Example of conditional UPDATE query: raise salary by 10% to everyone who has Master degree.
     *
     * @param personCache Person cache.
     */
    private static void update(IgniteCache<AffinityKey<Long>, Person> personCache) {
        String sql =
            "update Person set salary = salary * 1.1 " +
            "where resume = ?";

        personCache.query(new SqlFieldsQuery(sql).setArgs("Master"));
    }

    /**
     * Example of conditional DELETE query: delete non-Apache employees.
     *
     * @param personCache Person cache.
     */
    private static void delete(IgniteCache<AffinityKey<Long>, Person> personCache) {
        String sql =
            "delete from Person " +
            "where id in (" +
                "select p.id " +
                "from Person p, \"Organizations\".Organization as o " +
                "where o.name != ? and p.orgId = o.id" +
            ")";

        personCache.query(new SqlFieldsQuery(sql).setArgs("Apache")).getAll();
    }

    /**
     * Query current data.
     *
     * @param personCache Person cache.
     * @param msg Message.
     */
    private static void queryData(IgniteCache<AffinityKey<Long>, Person> personCache, String msg) {
        String sql =
            "select p.id, concat(p.firstName, ' ', p.lastName), o.name, p.resume, p.salary " +
            "from Person as p, \"Organizations\".Organization as o " +
            "where p.orgId = o.id";

        List<List<?>> res = personCache.query(new SqlFieldsQuery(sql)).getAll();

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
