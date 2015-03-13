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

package org.apache.ignite.internal.processors.query.h2.sql;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;

/**
 *
 */
public class IgniteVsH2QueryTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String PARTITIONED_CACHE = "partitioned";

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static Ignite ignite;

    /** Partitioned cache. */
    private static IgniteCache pCache;

    /** Replicated cache. */
    private static IgniteCache rCache;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        c.setMarshaller(new OptimizedMarshaller(true));

        c.setCacheConfiguration(createCache("partitioned", CacheMode.PARTITIONED), 
            createCache("replicated", CacheMode.REPLICATED)
        );

        return c;
    }

    /**
     * Creates new cache configuration.
     *
     * @param name Cache name.
     * @param mode Cache mode.
     * @return Cache configuration.
     */
    private static CacheConfiguration createCache(String name, CacheMode mode) {
        CacheConfiguration<?,?> cc = defaultCacheConfiguration();

        cc.setName(name);
        cc.setCacheMode(mode);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setEvictNearSynchronized(false);
        cc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cc.setDistributionMode(PARTITIONED_ONLY);

        if (mode == CacheMode.PARTITIONED)
            cc.setIndexedTypes(
                Integer.class, Organization.class,
                CacheAffinityKey.class, Person.class,
                CacheAffinityKey.class, Purchase.class
            );
        else if (mode == CacheMode.REPLICATED)
            cc.setIndexedTypes(
                Integer.class, Product.class
            );
        else
            throw new IllegalStateException("mode: " + mode);

        return cc;
    }


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrids(4);

        pCache = ignite.jcache("partitioned");
        
        rCache = ignite.jcache("replicated");

        awaitPartitionMapExchange();

        initialize();

    }
    
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
        
        stopAllGrids();
    }

    /**
     * Populate cache with test data.
     */
    @SuppressWarnings("unchecked")
    private void initialize() {
        int idGen = 0;
        
        // Organizations.
        List<Organization> orgs = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            int id = idGen++;
            
            Organization org = new Organization(id, "Org" + id);
            
            orgs.add(org);
            
            pCache.put(org.id, org);
        }

        // Persons.
        List<Person> persons = new ArrayList<>();
        
        for (int i = 0; i < 5; i++) {
            int id = idGen++;

            Person person = new Person(id, orgs.get(i % orgs.size()), "name" + id, "lastname" + id, id * 100.0);

            persons.add(person);

            pCache.put(person.key(), person);
        }

        // Products.
        List<Product> products = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            int id = idGen++;
            
            Product product = new Product(id, "Product" + id, id*1000);
            
            products.add(product);
            
            rCache.put(product.id, product);
        }

        // Purchases.
        for (int i = 0; i < products.size() * 2; i++) {
            int id = idGen++;

            Purchase purchase = new Purchase(id, products.get(i % products.size()), persons.get(i % persons.size()));

            pCache.put(purchase.key(), purchase);
        }
    }

    @SuppressWarnings("unchecked")
    private void test0(IgniteCache cache, String sql, Object... args){
        List<List<?>> res1 = cache.queryFields(new SqlFieldsQuery(sql).setArgs(args)).getAll();

        log.info(">>> Results (count=" + res1.size() + "):");
        
        for (List<?> objects : res1)
            log.info(objects.toString());
    }
    
    private void test0(String sql, Object... args) {
        test0(pCache, sql, args);        
    }

    /**
     * TODO
     */
    public void testSqlQueryWithAggregation() {
        test0("select avg(salary) from Person, Organization where Person.orgId = Organization.id and "
            + "lower(Organization.name) = lower(?)", "GridGain");
    }

    /**
     * TODO
     */
    public void testSqlFieldsQuery() {
        test0("select concat(firstName, ' ', lastName) from Person");
    }

    /**
     * TODO
     */
    public void testSqlFieldsQueryWithJoin() {
        test0("select concat(firstName, ' ', lastName), "
            + "Organization.name from Person, Organization where "
            + "Person.orgId = Organization.id");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCache() throws Exception {
        log.info("-- Organizations --");
        test0("select id, name from Organization");

        log.info("-- Persons --");
        test0("select id, firstName, lastName, orgId from Person");
        
        log.info("-- Purchases --");
        test0("select id, personId, productId from Purchase");
        
        log.info("-- Products --");
        test0(rCache, "select * from \"replicated\".Product");

        log.info("-- Person.id=3 --");
        test0("select *" +
            "  from Person" +
            "  where Person.id = ?", 3);

        log.info("-- Person.id=3 with Purchase --"); //TODO Investigate
        test0("select *" +
            "  from Person, Purchase" +
            "  where Person.id = ?", 3);

        log.info("-- Person.id = Purchase.personId --"); //TODO Investigate (should be 20 results instead of 8)
        test0("select *" +
            "  from Person, Purchase" +
            "  where Person.id = Purchase.personId");

        log.info("-- Cross query --"); //TODO Investigate
        test0("select concat(firstName, ' ', lastName), Product.name " +
            "  from Person, Purchase, \"replicated\".Product " +
            "  where Person.id = Purchase.personId and Purchase.productId = Product.id" +
            "  group by Product.id");
        
        log.info("-- Cross query with group by --"); //TODO Investigate
        test0("select concat(firstName, ' ', lastName), count (Product.id) " +
            "  from Person, Purchase, \"replicated\".Product " +
            "  where Person.id = Purchase.personId and Purchase.productId = Product.id" +
            "  group by Product.id");
    }
    
    /**
     * Person class.
     */
    private static class Person implements Serializable {
        /** Person ID (indexed). */
        @QuerySqlField(index = true)
        private int id;

        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private int orgId;

        /** First name (not-indexed). */
        @QuerySqlField
        private String firstName;

        /** Last name (not indexed). */
        @QuerySqlField
        private String lastName;

        /** Salary (indexed). */
        @QuerySqlField(index = true)
        private double salary;

        /** Custom cache key to guarantee that person is always collocated with its organization. */
        private transient CacheAffinityKey<Integer> key;

        /**
         * Constructs person record.
         *
         * @param org Organization.
         * @param firstName First name.
         * @param lastName Last name.
         * @param salary Salary.
         */
        Person(int id, Organization org, String firstName, String lastName, double salary) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.salary = salary;
            orgId = org.id;
        }

        /**
         * Gets cache affinity key. Since in some examples person needs to be collocated with organization, we create
         * custom affinity key to guarantee this collocation.
         *
         * @return Custom affinity key to guarantee that person is always collocated with organization.
         */
        public CacheAffinityKey<Integer> key() {
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
                ", salary=" + salary + ']';
        }
    }

    /**
     * Organization class.
     */
    private static class Organization implements Serializable {
        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private int id;

        /** Organization name (indexed). */
        @QuerySqlField(index = true)
        private String name;

        /**
         * Create Organization.
         *
         * @param id Organization ID.
         * @param name Organization name.
         */
        Organization(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization [id=" + id + ", name=" + name + ']';
        }
    }

    /**
     * Product class. 
     */
    private static class Product implements Serializable {
        /** Primary key. */
        @QuerySqlField(index = true)
        private int id;

        /** Product name. */
        @QuerySqlField
        private String name;
        
        /** Product price */
        @QuerySqlField
        private int price;

        /**
         * Create Product.
         *  
         * @param id Product ID.
         * @param name Product name.
         * @param price Product price.
         */
        Product(int id, String name, int price) {
            this.id = id;
            this.name = name;
            this.price = price;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Product [id=" + id + ", name=" + name + ", price=" + price + ']';
        }
    }

    /**
     * Purchase class.
     */
    private static class Purchase implements Serializable {
        /** Primary key. */
        @QuerySqlField(index = true)
        private int id;

        /** Product ID. */
        @QuerySqlField
        private int productId;

        /** Person ID. */
        @QuerySqlField
        private int personId;

        /** Custom cache key to guarantee that purchase is always collocated with its person. */
        private transient CacheAffinityKey<Integer> key;

        //TODO
        Purchase(int id, Product product, Person person) {
            this.id = id;
            productId = product.id;
            personId = person.id;
        }

        /**
         * Gets cache affinity key. Since in some examples purchase needs to be collocated with person, we create
         * custom affinity key to guarantee this collocation.
         *
         * @return Custom affinity key to guarantee that purchase is always collocated with person.
         */
        public CacheAffinityKey<Integer> key() {
            if (key == null)
                key = new CacheAffinityKey<>(id, personId);

            return key;
        }


        /** {@inheritDoc} */
        @Override public String toString() {
            return "Purchase [id=" + id + ", productId=" + productId + ", personId=" + personId + ']';
        }
    }
}
