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

package org.apache.ignite.tests;

import com.datastax.driver.core.SimpleStatement;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.tests.utils.CacheStoreHelper;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import org.apache.ignite.tests.pojos.Product;
import org.apache.ignite.tests.pojos.ProductOrder;
import org.apache.ignite.tests.pojos.Person;
import org.apache.ignite.tests.pojos.SimplePerson;
import org.apache.ignite.tests.pojos.PersonId;
import org.apache.ignite.tests.pojos.SimplePersonId;

/**
 * Unit tests for Ignite caches which utilizing {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}
 * to store cache data into Cassandra tables
 */
public class IgnitePersistentStoreTest {
    /** */
    private static final Logger LOGGER = Logger.getLogger(IgnitePersistentStoreTest.class.getName());

    /** */
    @BeforeClass
    public static void setUpClass() {
        if (CassandraHelper.useEmbeddedCassandra()) {
            try {
                CassandraHelper.startEmbeddedCassandra(LOGGER);
            }
            catch (Throwable e) {
                throw new RuntimeException("Failed to start embedded Cassandra instance", e);
            }
        }

        LOGGER.info("Testing admin connection to Cassandra");
        CassandraHelper.testAdminConnection();

        LOGGER.info("Testing regular connection to Cassandra");
        CassandraHelper.testRegularConnection();

        LOGGER.info("Dropping all artifacts from previous tests execution session");
        CassandraHelper.dropTestKeyspaces();

        LOGGER.info("Start tests execution");
    }

    /** */
    @AfterClass
    public static void tearDownClass() {
        try {
            CassandraHelper.dropTestKeyspaces();
        }
        finally {
            CassandraHelper.releaseCassandraResources();

            if (CassandraHelper.useEmbeddedCassandra()) {
                try {
                    CassandraHelper.stopEmbeddedCassandra();
                }
                catch (Throwable e) {
                    LOGGER.error("Failed to stop embedded Cassandra instance", e);
                }
            }
        }
    }

    /** */
    @Test
    public void primitiveStrategyTest() {
        Ignition.stopAll(true);

        Map<Long, Long> longMap = TestsHelper.generateLongsMap();
        Map<String, String> strMap = TestsHelper.generateStringsMap();
        Map<Long, String> longStrMap = TestsHelper.generateLongStringMap();

        LOGGER.info("Running PRIMITIVE strategy write tests");

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/primitive/ignite-config.xml")) {
            IgniteCache<Long, Long> longCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Long>("cache1"));
            IgniteCache<String, String> strCache = ignite.getOrCreateCache(new CacheConfiguration<String, String>("cache2"));
            IgniteCache<Long, String> longStrCache = ignite.getOrCreateCache(new CacheConfiguration<Long, String>("cache3"));

            LOGGER.info("Running single operation write tests");
            longCache.put(1L, 1L);
            strCache.put("1", "1");
            longStrCache.put(1L, "1");
            LOGGER.info("Single operation write tests passed");

            LOGGER.info("Running bulk operation write tests");
            longCache.putAll(longMap);
            strCache.putAll(strMap);
            longStrCache.putAll(longStrMap);
            LOGGER.info("Bulk operation write tests passed");
        }

        LOGGER.info("PRIMITIVE strategy write tests passed");

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/primitive/ignite-config.xml")) {
            LOGGER.info("Running PRIMITIVE strategy read tests");

            IgniteCache<Long, Long> longCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Long>("cache1"));
            IgniteCache<String, String> strCache = ignite.getOrCreateCache(new CacheConfiguration<String, String>("cache2"));
            IgniteCache<Long, String> longStrCache = ignite.getOrCreateCache(new CacheConfiguration<Long, String>("cache3"));

            LOGGER.info("Running single operation read tests");

            Long longVal = longCache.get(1L);
            if (!longVal.equals(longMap.get(1L)))
                throw new RuntimeException("Long value was incorrectly deserialized from Cassandra");

            String strVal = strCache.get("1");
            if (!strVal.equals(strMap.get("1")))
                throw new RuntimeException("String value was incorrectly deserialized from Cassandra");

            String longStrVal = longStrCache.get(1L);
            if (!longStrVal.equals(longStrMap.get(1L)))
                throw new RuntimeException("LongString value was incorrectly deserialized from Cassandra");

            LOGGER.info("Single operation read tests passed");

            LOGGER.info("Running bulk operation read tests");

            Map<Long, Long> longMap1 = longCache.getAll(longMap.keySet());
            if (!TestsHelper.checkMapsEqual(longMap, longMap1))
                throw new RuntimeException("Long values batch was incorrectly deserialized from Cassandra");

            Map<String, String> strMap1 = strCache.getAll(strMap.keySet());
            if (!TestsHelper.checkMapsEqual(strMap, strMap1))
                throw new RuntimeException("String values batch was incorrectly deserialized from Cassandra");

            Map<Long, String> longStrMap1 = longStrCache.getAll(longStrMap.keySet());
            if (!TestsHelper.checkMapsEqual(longStrMap, longStrMap1))
                throw new RuntimeException("LongString values batch was incorrectly deserialized from Cassandra");

            LOGGER.info("Bulk operation read tests passed");

            LOGGER.info("PRIMITIVE strategy read tests passed");

            LOGGER.info("Running PRIMITIVE strategy delete tests");

            longCache.remove(1L);
            longCache.removeAll(longMap.keySet());

            strCache.remove("1");
            strCache.removeAll(strMap.keySet());

            longStrCache.remove(1L);
            longStrCache.removeAll(longStrMap.keySet());

            LOGGER.info("PRIMITIVE strategy delete tests passed");
        }
    }

    /** */
    @Test
    public void blobStrategyTest() {
        Ignition.stopAll(true);

        Map<Long, Long> longMap = TestsHelper.generateLongsMap();
        Map<Long, Person> personMap = TestsHelper.generateLongsPersonsMap();

        LOGGER.info("Running BLOB strategy write tests");

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/blob/ignite-config.xml")) {
            IgniteCache<Long, Long> longCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Long>("cache1"));
            IgniteCache<Long, Person> personCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Person>("cache2"));

            LOGGER.info("Running single operation write tests");
            longCache.put(1L, 1L);
            personCache.put(1L, TestsHelper.generateRandomPerson(1L));
            LOGGER.info("Single operation write tests passed");

            LOGGER.info("Running bulk operation write tests");
            longCache.putAll(longMap);
            personCache.putAll(personMap);
            LOGGER.info("Bulk operation write tests passed");
        }

        LOGGER.info("BLOB strategy write tests passed");

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/blob/ignite-config.xml")) {
            LOGGER.info("Running BLOB strategy read tests");

            IgniteCache<Long, Long> longCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Long>("cache1"));
            IgniteCache<Long, Person> personCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Person>("cache2"));

            LOGGER.info("Running single operation read tests");

            Long longVal = longCache.get(1L);
            if (!longVal.equals(longMap.get(1L)))
                throw new RuntimeException("Long value was incorrectly deserialized from Cassandra");

            Person person = personCache.get(1L);
            if (!person.equals(personMap.get(1L)))
                throw new RuntimeException("Person value was incorrectly deserialized from Cassandra");

            LOGGER.info("Single operation read tests passed");

            LOGGER.info("Running bulk operation read tests");

            Map<Long, Long> longMap1 = longCache.getAll(longMap.keySet());
            if (!TestsHelper.checkMapsEqual(longMap, longMap1))
                throw new RuntimeException("Long values batch was incorrectly deserialized from Cassandra");

            Map<Long, Person> personMap1 = personCache.getAll(personMap.keySet());
            if (!TestsHelper.checkPersonMapsEqual(personMap, personMap1, false))
                throw new RuntimeException("Person values batch was incorrectly deserialized from Cassandra");

            LOGGER.info("Bulk operation read tests passed");

            LOGGER.info("BLOB strategy read tests passed");

            LOGGER.info("Running BLOB strategy delete tests");

            longCache.remove(1L);
            longCache.removeAll(longMap.keySet());

            personCache.remove(1L);
            personCache.removeAll(personMap.keySet());

            LOGGER.info("BLOB strategy delete tests passed");
        }
    }

    /** */
    @Test
    public void blobBinaryLoadCacheTest() {
        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/loadall_blob/ignite-config.xml")) {
            IgniteCache<Long, PojoPerson> personCache = ignite.getOrCreateCache("cache2");

            assert ignite.configuration().getMarshaller() instanceof BinaryMarshaller;

            personCache.put(1L, new PojoPerson(1, "name"));

            assert personCache.withKeepBinary().get(1L) instanceof BinaryObject;
        }

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/loadall_blob/ignite-config.xml")) {
            IgniteCache<Long, PojoPerson> personCache = ignite.getOrCreateCache("cache2");

            personCache.loadCache(null, null);

            PojoPerson person = personCache.get(1L);

            LOGGER.info("loadCache tests passed");
        }
    }

    /** */
    @Test
    public void pojoStrategyTest() {
        Ignition.stopAll(true);

        LOGGER.info("Running POJO strategy write tests");

        Map<Long, Person> personMap1 = TestsHelper.generateLongsPersonsMap();
        Map<PersonId, Person> personMap2 = TestsHelper.generatePersonIdsPersonsMap();
        Map<PersonId, Person> personMap7 = TestsHelper.generatePersonIdsPersonsForHandlerMap();
        Map<Long, Product> productsMap = TestsHelper.generateProductsMap();
        Map<Long, ProductOrder> ordersMap = TestsHelper.generateOrdersMap();

        Product product = TestsHelper.generateRandomProduct(-1L);
        ProductOrder order = TestsHelper.generateRandomOrder(-1L);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/pojo/ignite-config.xml")) {
            IgniteCache<Long, Person> personCache1 = ignite.getOrCreateCache(new CacheConfiguration<Long, Person>("cache1"));
            IgniteCache<PersonId, Person> personCache2 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache2"));
            IgniteCache<PersonId, Person> personCache3 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache3"));
            IgniteCache<PersonId, Person> personCache4 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache4"));
            IgniteCache<PersonId, Person> personCache7 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache7"));
            IgniteCache<Long, Product> productCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Product>("product"));
            IgniteCache<Long, ProductOrder> orderCache = ignite.getOrCreateCache(new CacheConfiguration<Long, ProductOrder>("order"));

            LOGGER.info("Running single operation write tests");

            personCache1.put(1L, TestsHelper.generateRandomPerson(1L));

            PersonId id = TestsHelper.generateRandomPersonId();
            personCache2.put(id, TestsHelper.generateRandomPerson(id.getPersonNumber()));

            id = TestsHelper.generateRandomPersonId();
            personCache3.put(id, TestsHelper.generateRandomPerson(id.getPersonNumber()));
            personCache4.put(id, TestsHelper.generateRandomPerson(id.getPersonNumber()));

            id = TestsHelper.generateRandomPersonIdForHandler();
            personCache7.put(id, TestsHelper.generateRandomPersonForHandler(id.getPersonNumber()));

            productCache.put(product.getId(), product);
            orderCache.put(order.getId(), order);

            LOGGER.info("Single operation write tests passed");

            LOGGER.info("Running bulk operation write tests");
            personCache1.putAll(personMap1);
            personCache2.putAll(personMap2);
            personCache3.putAll(personMap2);
            personCache4.putAll(personMap2);
            personCache7.putAll(personMap7);
            productCache.putAll(productsMap);
            orderCache.putAll(ordersMap);
            LOGGER.info("Bulk operation write tests passed");
        }

        LOGGER.info("POJO strategy write tests passed");

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/pojo/ignite-config.xml")) {
            LOGGER.info("Running POJO strategy read tests");

            IgniteCache<Long, Person> personCache1 = ignite.getOrCreateCache(new CacheConfiguration<Long, Person>("cache1"));
            IgniteCache<PersonId, Person> personCache2 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache2"));
            IgniteCache<PersonId, Person> personCache3 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache3"));
            IgniteCache<PersonId, Person> personCache4 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache4"));
            IgniteCache<PersonId, Person> personCache7 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache7"));
            IgniteCache<Long, Product> productCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Product>("product"));
            IgniteCache<Long, ProductOrder> orderCache = ignite.getOrCreateCache(new CacheConfiguration<Long, ProductOrder>("order"));

            LOGGER.info("Running single operation read tests");
            Person person = personCache1.get(1L);
            if (!person.equalsPrimitiveFields(personMap1.get(1L)))
                throw new RuntimeException("Person value was incorrectly deserialized from Cassandra");

            PersonId id = personMap2.keySet().iterator().next();

            person = personCache2.get(id);
            if (!person.equalsPrimitiveFields(personMap2.get(id)))
                throw new RuntimeException("Person value was incorrectly deserialized from Cassandra");

            person = personCache3.get(id);
            if (!person.equals(personMap2.get(id)))
                throw new RuntimeException("Person value was incorrectly deserialized from Cassandra");

            person = personCache4.get(id);
            if (!person.equals(personMap2.get(id)))
                throw new RuntimeException("Person value was incorrectly deserialized from Cassandra");

            PersonId idHandler = personMap7.keySet().iterator().next();
            person = personCache7.get(idHandler);
            if (!person.equals(personMap7.get(idHandler)))
                throw new RuntimeException("Person value was incorrectly deserialized from Cassandra");

            Product product1 = productCache.get(product.getId());
            if (!product.equals(product1))
                throw new RuntimeException("Product value was incorrectly deserialized from Cassandra");

            ProductOrder order1 = orderCache.get(order.getId());
            if (!order.equals(order1))
                throw new RuntimeException("Order value was incorrectly deserialized from Cassandra");

            LOGGER.info("Single operation read tests passed");

            LOGGER.info("Running bulk operation read tests");

            Map<Long, Person> persons1 = personCache1.getAll(personMap1.keySet());
            if (!TestsHelper.checkPersonMapsEqual(persons1, personMap1, true))
                throw new RuntimeException("Persons values batch was incorrectly deserialized from Cassandra");

            Map<PersonId, Person> persons2 = personCache2.getAll(personMap2.keySet());
            if (!TestsHelper.checkPersonMapsEqual(persons2, personMap2, true))
                throw new RuntimeException("Person values batch was incorrectly deserialized from Cassandra");

            Map<PersonId, Person> persons3 = personCache3.getAll(personMap2.keySet());
            if (!TestsHelper.checkPersonMapsEqual(persons3, personMap2, false))
                throw new RuntimeException("Person values batch was incorrectly deserialized from Cassandra");

            Map<PersonId, Person> persons4 = personCache4.getAll(personMap2.keySet());
            if (!TestsHelper.checkPersonMapsEqual(persons4, personMap2, false))
                throw new RuntimeException("Person values batch was incorrectly deserialized from Cassandra");

            Map<PersonId, Person> persons7 = personCache7.getAll(personMap7.keySet());
            if (!TestsHelper.checkPersonMapsEqual(persons7, personMap7, false))
                throw new RuntimeException("Person values batch was incorrectly deserialized from Cassandra");

            Map<Long, Product> productsMap1 = productCache.getAll(productsMap.keySet());
            if (!TestsHelper.checkProductMapsEqual(productsMap, productsMap1))
                throw new RuntimeException("Product values batch was incorrectly deserialized from Cassandra");

            Map<Long, ProductOrder> ordersMap1 = orderCache.getAll(ordersMap.keySet());
            if (!TestsHelper.checkOrderMapsEqual(ordersMap, ordersMap1))
                throw new RuntimeException("Order values batch was incorrectly deserialized from Cassandra");

            LOGGER.info("Bulk operation read tests passed");

            LOGGER.info("POJO strategy read tests passed");

            LOGGER.info("Running POJO strategy delete tests");

            personCache1.remove(1L);
            personCache1.removeAll(personMap1.keySet());

            personCache2.remove(id);
            personCache2.removeAll(personMap2.keySet());

            personCache3.remove(id);
            personCache3.removeAll(personMap2.keySet());

            personCache4.remove(id);
            personCache4.removeAll(personMap2.keySet());

            personCache7.remove(idHandler);
            personCache7.removeAll(personMap7.keySet());

            productCache.remove(product.getId());
            productCache.removeAll(productsMap.keySet());

            orderCache.remove(order.getId());
            orderCache.removeAll(ordersMap.keySet());

            LOGGER.info("POJO strategy delete tests passed");
        }
    }

    /** */
    @Test
    public void pojoStrategySimpleObjectsTest() {
        Ignition.stopAll(true);

        LOGGER.info("Running POJO strategy write tests for simple objects");

        Map<SimplePersonId, SimplePerson> personMap5 = TestsHelper.generateSimplePersonIdsPersonsMap();
        Map<SimplePersonId, SimplePerson> personMap6 = TestsHelper.generateSimplePersonIdsPersonsMap();

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/pojo/ignite-config.xml")) {
            IgniteCache<SimplePersonId, SimplePerson> personCache5 = ignite.getOrCreateCache(new CacheConfiguration<SimplePersonId, SimplePerson>("cache5"));
            IgniteCache<SimplePersonId, SimplePerson> personCache6 = ignite.getOrCreateCache(new CacheConfiguration<SimplePersonId, SimplePerson>("cache6"));

            LOGGER.info("Running single operation write tests");

            SimplePersonId id = TestsHelper.generateRandomSimplePersonId();
            personCache5.put(id, TestsHelper.generateRandomSimplePerson(id.personNum));
            personCache6.put(id, TestsHelper.generateRandomSimplePerson(id.personNum));

            LOGGER.info("Single operation write tests passed");

            LOGGER.info("Running bulk operation write tests");
            personCache5.putAll(personMap5);
            personCache6.putAll(personMap6);
            LOGGER.info("Bulk operation write tests passed");
        }

        LOGGER.info("POJO strategy write tests for simple objects passed");

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/pojo/ignite-config.xml")) {
            LOGGER.info("Running POJO strategy read tests for simple objects");

            IgniteCache<SimplePersonId, SimplePerson> personCache5 = ignite.getOrCreateCache(new CacheConfiguration<SimplePersonId, SimplePerson>("cache5"));
            IgniteCache<SimplePersonId, SimplePerson> personCache6 = ignite.getOrCreateCache(new CacheConfiguration<SimplePersonId, SimplePerson>("cache6"));

            LOGGER.info("Running single operation read tests");

            SimplePersonId id = personMap5.keySet().iterator().next();

            SimplePerson person = personCache5.get(id);
            if (!person.equalsPrimitiveFields(personMap5.get(id)))
                throw new RuntimeException("SimplePerson value was incorrectly deserialized from Cassandra");

            id = personMap6.keySet().iterator().next();

            person = personCache6.get(id);
            if (!person.equals(personMap6.get(id)))
                throw new RuntimeException("SimplePerson value was incorrectly deserialized from Cassandra");

            LOGGER.info("Single operation read tests passed");

            LOGGER.info("Running bulk operation read tests");

            Map<SimplePersonId, SimplePerson> persons5 = personCache5.getAll(personMap5.keySet());
            if (!TestsHelper.checkSimplePersonMapsEqual(persons5, personMap5, true))
                throw new RuntimeException("SimplePerson values batch was incorrectly deserialized from Cassandra");

            Map<SimplePersonId, SimplePerson> persons6 = personCache6.getAll(personMap6.keySet());
            if (!TestsHelper.checkSimplePersonMapsEqual(persons6, personMap6, false))
                throw new RuntimeException("SimplePerson values batch was incorrectly deserialized from Cassandra");

            LOGGER.info("Bulk operation read tests passed");

            LOGGER.info("POJO strategy read tests for simple objects passed");

            LOGGER.info("Running POJO strategy delete tests for simple objects");

            personCache5.remove(id);
            personCache5.removeAll(personMap5.keySet());

            personCache6.remove(id);
            personCache6.removeAll(personMap6.keySet());

            LOGGER.info("POJO strategy delete tests for simple objects passed");
        }
    }

    /** */
    @Test
    public void pojoStrategyTransactionTest() {
        CassandraHelper.dropTestKeyspaces();

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/pojo/ignite-config.xml")) {
            pojoStrategyTransactionTest(ignite, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
            pojoStrategyTransactionTest(ignite, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
            pojoStrategyTransactionTest(ignite, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);
            pojoStrategyTransactionTest(ignite, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
            pojoStrategyTransactionTest(ignite, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
            pojoStrategyTransactionTest(ignite, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);
        }
    }

    /** */
    @Test
    public void loadCacheTest() {
        Ignition.stopAll(true);

        LOGGER.info("Running loadCache test");

        LOGGER.info("Filling Cassandra table with test data");

        CacheStore store = CacheStoreHelper.createCacheStore("personTypes",
            new ClassPathResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-3.xml"),
            CassandraHelper.getAdminDataSrc());

        Collection<CacheEntryImpl<PersonId, Person>> entries = TestsHelper.generatePersonIdsPersonsEntries();

        //noinspection unchecked
        store.writeAll(entries);

        LOGGER.info("Cassandra table filled with test data");

        LOGGER.info("Running loadCache test");

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/pojo/ignite-config.xml")) {
            CacheConfiguration<PersonId, Person> ccfg = new CacheConfiguration<>("cache3");

            IgniteCache<PersonId, Person> personCache3 = ignite.getOrCreateCache(ccfg);

            int size = personCache3.size(CachePeekMode.ALL);

            LOGGER.info("Initial cache size " + size);

            LOGGER.info("Loading cache data from Cassandra table");

            String qry = "select * from test1.pojo_test3 limit 3";

            personCache3.loadCache(null, qry);

            size = personCache3.size(CachePeekMode.ALL);
            Assert.assertEquals("Cache data was incorrectly loaded from Cassandra table by '" + qry + "'", 3, size);

            personCache3.clear();

            personCache3.loadCache(null, new SimpleStatement(qry));

            size = personCache3.size(CachePeekMode.ALL);
            Assert.assertEquals("Cache data was incorrectly loaded from Cassandra table by statement", 3, size);

            personCache3.clear();

            personCache3.loadCache(null);

            size = personCache3.size(CachePeekMode.ALL);
            Assert.assertEquals("Cache data was incorrectly loaded from Cassandra. " +
                    "Expected number of records is " + TestsHelper.getBulkOperationSize() +
                    ", but loaded number of records is " + size,
                TestsHelper.getBulkOperationSize(), size);

            LOGGER.info("Cache data loaded from Cassandra table");
        }

        LOGGER.info("loadCache test passed");
    }

    /** */
    @SuppressWarnings("unchecked")
    private void pojoStrategyTransactionTest(Ignite ignite, TransactionConcurrency concurrency,
                                             TransactionIsolation isolation) {
        LOGGER.info("-----------------------------------------------------------------------------------");
        LOGGER.info("Running POJO transaction tests using " + concurrency +
                " concurrency and " + isolation + " isolation level");
        LOGGER.info("-----------------------------------------------------------------------------------");

        CacheStore productStore = CacheStoreHelper.createCacheStore("product",
            new ClassPathResource("org/apache/ignite/tests/persistence/pojo/product.xml"),
            CassandraHelper.getAdminDataSrc());

        CacheStore orderStore = CacheStoreHelper.createCacheStore("order",
            new ClassPathResource("org/apache/ignite/tests/persistence/pojo/order.xml"),
            CassandraHelper.getAdminDataSrc());

        Map<Long, Product> productsMap = TestsHelper.generateProductsMap(5);
        Map<Long, Product> productsMap1;
        Map<Long, ProductOrder> ordersMap = TestsHelper.generateOrdersMap(5);
        Map<Long, ProductOrder> ordersMap1;
        Product product = TestsHelper.generateRandomProduct(-1L);
        ProductOrder order = TestsHelper.generateRandomOrder(-1L, -1L, new Date());

        IgniteTransactions txs = ignite.transactions();

        IgniteCache<Long, Product> productCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Product>("product"));
        IgniteCache<Long, ProductOrder> orderCache = ignite.getOrCreateCache(new CacheConfiguration<Long, ProductOrder>("order"));

        LOGGER.info("Running POJO strategy write tests");

        LOGGER.info("Running single operation write tests");

        Transaction tx = txs.txStart(concurrency, isolation);

        try {
            productCache.put(product.getId(), product);
            orderCache.put(order.getId(), order);

            if (productStore.load(product.getId()) != null || orderStore.load(order.getId()) != null) {
                throw new RuntimeException("Single write operation test failed. Transaction wasn't committed yet, but " +
                        "objects were already persisted into Cassandra");
            }

            Map<Long, Product> products = (Map<Long, Product>)productStore.loadAll(productsMap.keySet());
            Map<Long, ProductOrder> orders = (Map<Long, ProductOrder>)orderStore.loadAll(ordersMap.keySet());

            if ((products != null && !products.isEmpty()) || (orders != null && !orders.isEmpty())) {
                throw new RuntimeException("Single write operation test failed. Transaction wasn't committed yet, but " +
                        "objects were already persisted into Cassandra");
            }

            tx.commit();
        }
        finally {
            U.closeQuiet(tx);
        }

        Product product1 = (Product)productStore.load(product.getId());
        ProductOrder order1 = (ProductOrder)orderStore.load(order.getId());

        if (product1 == null || order1 == null) {
            throw new RuntimeException("Single write operation test failed. Transaction was committed, but " +
                    "no objects were persisted into Cassandra");
        }

        if (!product.equals(product1) || !order.equals(order1)) {
            throw new RuntimeException("Single write operation test failed. Transaction was committed, but " +
                    "objects were incorrectly persisted/loaded to/from Cassandra");
        }

        LOGGER.info("Single operation write tests passed");

        LOGGER.info("Running bulk operation write tests");

        tx = txs.txStart(concurrency, isolation);

        try {
            productCache.putAll(productsMap);
            orderCache.putAll(ordersMap);

            productsMap1 = (Map<Long, Product>)productStore.loadAll(productsMap.keySet());
            ordersMap1 = (Map<Long, ProductOrder>)orderStore.loadAll(ordersMap.keySet());

            if ((productsMap1 != null && !productsMap1.isEmpty()) || (ordersMap1 != null && !ordersMap1.isEmpty())) {
                throw new RuntimeException("Bulk write operation test failed. Transaction wasn't committed yet, but " +
                        "objects were already persisted into Cassandra");
            }

            tx.commit();
        }
        finally {
            U.closeQuiet(tx);
        }

        productsMap1 = (Map<Long, Product>)productStore.loadAll(productsMap.keySet());
        ordersMap1 = (Map<Long, ProductOrder>)orderStore.loadAll(ordersMap.keySet());

        if (productsMap1 == null || productsMap1.isEmpty() || ordersMap1 == null || ordersMap1.isEmpty()) {
            throw new RuntimeException("Bulk write operation test failed. Transaction was committed, but " +
                    "no objects were persisted into Cassandra");
        }

        if (productsMap1.size() < productsMap.size() || ordersMap1.size() < ordersMap.size()) {
            throw new RuntimeException("Bulk write operation test failed. There were committed less objects " +
                    "into Cassandra than expected");
        }

        if (productsMap1.size() > productsMap.size() || ordersMap1.size() > ordersMap.size()) {
            throw new RuntimeException("Bulk write operation test failed. There were committed more objects " +
                    "into Cassandra than expected");
        }

        for (Map.Entry<Long, Product> entry : productsMap.entrySet()) {
            product = productsMap1.get(entry.getKey());

            if (!entry.getValue().equals(product)) {
                throw new RuntimeException("Bulk write operation test failed. Transaction was committed, but " +
                        "some objects were incorrectly persisted/loaded to/from Cassandra");
            }
        }

        for (Map.Entry<Long, ProductOrder> entry : ordersMap.entrySet()) {
            order = ordersMap1.get(entry.getKey());

            if (!entry.getValue().equals(order)) {
                throw new RuntimeException("Bulk write operation test failed. Transaction was committed, but " +
                        "some objects were incorrectly persisted/loaded to/from Cassandra");
            }
        }

        LOGGER.info("Bulk operation write tests passed");

        LOGGER.info("POJO strategy write tests passed");

        LOGGER.info("Running POJO strategy delete tests");

        LOGGER.info("Running single delete tests");

        tx = txs.txStart(concurrency, isolation);

        try {
            productCache.remove(-1L);
            orderCache.remove(-1L);

            if (productStore.load(-1L) == null || orderStore.load(-1L) == null) {
                throw new RuntimeException("Single delete operation test failed. Transaction wasn't committed yet, but " +
                        "objects were already deleted from Cassandra");
            }

            tx.commit();
        }
        finally {
            U.closeQuiet(tx);
        }

        if (productStore.load(-1L) != null || orderStore.load(-1L) != null) {
            throw new RuntimeException("Single delete operation test failed. Transaction was committed, but " +
                    "objects were not deleted from Cassandra");
        }

        LOGGER.info("Single delete tests passed");

        LOGGER.info("Running bulk delete tests");

        tx = txs.txStart(concurrency, isolation);

        try {
            productCache.removeAll(productsMap.keySet());
            orderCache.removeAll(ordersMap.keySet());

            productsMap1 = (Map<Long, Product>)productStore.loadAll(productsMap.keySet());
            ordersMap1 = (Map<Long, ProductOrder>)orderStore.loadAll(ordersMap.keySet());

            if (productsMap1.size() != productsMap.size() || ordersMap1.size() != ordersMap.size()) {
                throw new RuntimeException("Bulk delete operation test failed. Transaction wasn't committed yet, but " +
                        "objects were already deleted from Cassandra");
            }

            tx.commit();
        }
        finally {
            U.closeQuiet(tx);
        }

        productsMap1 = (Map<Long, Product>)productStore.loadAll(productsMap.keySet());
        ordersMap1 = (Map<Long, ProductOrder>)orderStore.loadAll(ordersMap.keySet());

        if ((productsMap1 != null && !productsMap1.isEmpty()) || (ordersMap1 != null && !ordersMap1.isEmpty())) {
            throw new RuntimeException("Bulk delete operation test failed. Transaction was committed, but " +
                    "objects were not deleted from Cassandra");
        }

        LOGGER.info("Bulk delete tests passed");

        LOGGER.info("POJO strategy delete tests passed");

        LOGGER.info("-----------------------------------------------------------------------------------");
        LOGGER.info("Passed POJO transaction tests for " + concurrency +
                " concurrency and " + isolation + " isolation level");
        LOGGER.info("-----------------------------------------------------------------------------------");
    }

    /** */
    public static class PojoPerson {
        /** */
        private int id;

        /** */
        private String name;

        /** */
        public PojoPerson() {
            // No-op.
        }

        /** */
        public PojoPerson(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** */
        public int getId() {
            return id;
        }

        /** */
        public String getName() {
            return name;
        }
    }
}
